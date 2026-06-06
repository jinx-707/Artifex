"""
api/db.py – Shared PostgreSQL connection pool and helper functions.

All route modules import from here so the pool is a true singleton.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any

import asyncpg
import structlog

from api.websockets.events import broadcast_workflow_event

logger = structlog.get_logger()
_process_logger = logging.getLogger("artifex.db")

DATABASE_URL: str = os.getenv(
    "DATABASE_URL", "postgresql://artifex:artifex123@postgres:5432/placements"
)

# ── Connection pool (module-level singleton) ──────────────────────────────────
_db_pool: asyncpg.Pool | None = None


def get_pool() -> asyncpg.Pool | None:
    """Return the current pool (may be None before init)."""
    return _db_pool


async def init_db_pool() -> None:
    """
    Create the asyncpg connection pool.
    Schema is managed exclusively by Alembic – no DDL here.
    """
    global _db_pool
    _db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    logger.info("api.db_pool_ready", url=DATABASE_URL.split("@")[-1])


def run_alembic_upgrade() -> None:
    """
    Run ``alembic upgrade head`` synchronously at startup.

    Uses subprocess so we don't need to import Alembic's internals into the
    async event loop.  Logs errors instead of crashing so the application can
    still start and serve health-check / diagnostic endpoints even when the
    schema is in a degraded state.
    """
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    alembic_ini = os.path.join(repo_root, "alembic.ini")

    if not os.path.exists(alembic_ini):
        _process_logger.warning(
            "alembic.ini not found at %s – skipping schema migration", alembic_ini
        )
        return

    _process_logger.info("Running alembic upgrade head …")
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", alembic_ini, "upgrade", "head"],
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    if result.returncode != 0:
        _process_logger.error(
            "alembic upgrade head FAILED:\nstdout: %s\nstderr: %s",
            result.stdout,
            result.stderr,
        )
        _process_logger.warning(
            "Continuing despite migration failure – some tables may be out of date. "
            "Run `alembic upgrade head` manually after diagnosing the issue."
        )
        return
    _process_logger.info("alembic upgrade head OK:\n%s", result.stdout or "(no output)")


# ── Placement helpers ─────────────────────────────────────────────────────────

async def store_placement(placement: dict) -> None:
    """Upsert a placement record into PostgreSQL."""
    if _db_pool is None:
        logger.warning("api.store_placement.no_pool")
        return
    async with _db_pool.acquire() as conn:
        family = placement.get("family")
        if not isinstance(family, dict):
            family = None
        family_id = family.get("family_id") if family else None
        await conn.execute(
            """
            INSERT INTO placements
                (workflow_id, child_id, family_id, family_json,
                 risk_score, risk_explanation, match_explanation, last_notes, status, updated_at)
            VALUES ($1, $2, COALESCE($3, 'unassigned'), COALESCE($4::jsonb, '{}'::jsonb),
                    $5, $6, $7, $8, $9, NOW())
            ON CONFLICT (workflow_id) DO UPDATE SET
                family_id         = CASE WHEN $3 IS NOT NULL THEN $3 ELSE placements.family_id END,
                risk_score        = EXCLUDED.risk_score,
                risk_explanation  = EXCLUDED.risk_explanation,
                match_explanation = COALESCE(EXCLUDED.match_explanation, placements.match_explanation),
                last_notes        = EXCLUDED.last_notes,
                family_json       = CASE WHEN $4 IS NOT NULL THEN $4::jsonb ELSE placements.family_json END,
                status            = EXCLUDED.status,
                updated_at        = NOW()
            """,
            placement.get("workflow_id", f"wf-{placement.get('child_id', 'unknown')}"),
            placement.get("child_id", "unknown"),
            family_id,
            json.dumps(family) if family else None,
            float(placement.get("risk_score", 0.0)),
            placement.get("risk_explanation"),
            placement.get("match_explanation"),
            placement.get("last_notes"),
            placement.get("status") or "active",
        )


async def store_workflow_event(
    workflow_id: str, stage: str, status: str, data: dict | None = None
) -> None:
    """Persist a workflow event and update workflow_status."""
    if _db_pool is None:
        logger.warning("api.store_workflow_event.no_pool")
        return
    safe_data = data or {}
    async with _db_pool.acquire() as conn:
        current_wf_status = await conn.fetchval(
            "SELECT status FROM workflow_status WHERE workflow_id = $1",
            workflow_id,
        )
        if current_wf_status in ("approved", "rejected", "closed"):
            if status == "in_progress":
                status = "completed"

        await conn.execute(
            "INSERT INTO workflow_events (workflow_id, stage, status, data) "
            "VALUES ($1, $2, $3, $4::jsonb)",
            workflow_id, stage, status, json.dumps(safe_data),
        )
        progress = int(safe_data.get("progress", 0))
        timestamp = safe_data.get("timestamp") or safe_data.get("updated_at") or datetime.now(timezone.utc).isoformat()
        await conn.execute(
            "INSERT INTO workflow_status "
            "  (workflow_id, status, current_stage, progress, metadata, updated_at) "
            "VALUES ($1,$2,$3,$4,$5::jsonb,NOW()) "
            "ON CONFLICT (workflow_id) DO UPDATE SET "
            "  status=CASE WHEN workflow_status.status IN ('approved','rejected','closed') "
            "             THEN workflow_status.status ELSE EXCLUDED.status END, "
            "  current_stage=CASE WHEN workflow_status.status IN ('approved','rejected','closed') "
            "                    THEN workflow_status.current_stage ELSE EXCLUDED.current_stage END, "
            "  progress=CASE WHEN workflow_status.status IN ('approved','rejected','closed') "
            "               THEN workflow_status.progress ELSE EXCLUDED.progress END, "
            "  metadata=COALESCE(EXCLUDED.metadata, workflow_status.metadata), "
            "  updated_at=NOW()",
            workflow_id, status, stage, progress, json.dumps(safe_data),
        )

        reasoning_steps = safe_data.get("reasoning", [])
        if reasoning_steps and isinstance(reasoning_steps, list):
            for i, step in enumerate(reasoning_steps):
                agent_name = safe_data.get("agent", stage)
                try:
                    await conn.execute(
                        "INSERT INTO reasoning_traces "
                        "  (workflow_id, stage, agent_name, step_index, content, timestamp) "
                        "VALUES ($1, $2, $3, $4, $5, NOW()) "
                        "ON CONFLICT DO NOTHING",
                        workflow_id, stage, agent_name, i, str(step),
                    )
                except Exception:
                    pass

        # Fetch timeline using same connection to avoid pool exhaustion
        timeline_rows = await conn.fetch(
            "SELECT stage, status, data, timestamp FROM workflow_events "
            "WHERE workflow_id = $1 ORDER BY timestamp ASC LIMIT 200",
            workflow_id,
        )
        timeline = [dict(r) for r in timeline_rows]

    await broadcast_workflow_event(
        workflow_id,
        {
            "type": "workflow_event",
            "workflow_id": workflow_id,
            "stage": stage,
            "status": status,
            "progress": progress,
            "timestamp": timestamp,
            "current_stage": stage,
            "payload": {
                **safe_data,
                "agent": safe_data.get("agent", stage),
                "action": safe_data.get("action", ""),
                "output": safe_data.get("output", ""),
                "confidence": safe_data.get("confidence"),
                "confidence_score": safe_data.get("confidence_score"),
                "latency": safe_data.get("latency", 0),
                "reasoning": safe_data.get("reasoning", []),
                "input": safe_data.get("input", ""),
                "inputData": safe_data.get("inputData", ""),
                "outputData": safe_data.get("outputData", ""),
                "decisionExplanation": safe_data.get("decisionExplanation", ""),
                "logs": safe_data.get("logs", []),
                "message": safe_data.get("message", safe_data.get("details", "")),
                "details": safe_data.get("details", ""),
            },
            "timeline": timeline,
        },
    )


async def get_workflow_timeline(workflow_id: str, limit: int = 100) -> list[dict]:
    if _db_pool is None:
        return []
    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT stage, status, data, timestamp FROM workflow_events "
            "WHERE workflow_id = $1 ORDER BY timestamp ASC LIMIT $2",
            workflow_id, limit,
        )
    return [dict(r) for r in rows]


async def get_workflow_status_db(workflow_id: str) -> dict | None:
    if _db_pool is None:
        return None
    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT workflow_id, status, current_stage, progress, metadata, updated_at "
            "FROM workflow_status WHERE workflow_id = $1",
            workflow_id,
        )
    return dict(row) if row else None


async def store_ml_inference_log(
    workflow_id: str,
    child_id: str,
    payload: dict,
    result: dict,
    model_version: str | None = None,
) -> None:
    if _db_pool is None:
        return
    async with _db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO ml_inference_logs "
            "  (workflow_id, child_id, payload, result, model_version) "
            "VALUES ($1,$2,$3::jsonb,$4::jsonb,$5)",
            workflow_id, child_id,
            json.dumps(payload), json.dumps(result), model_version,
        )


async def store_prediction(
    workflow_id: str,
    child_id: str,
    recommended: dict,
    score: float | None = None,
    confidence: float | None = None,
    model_version: str | None = None,
    risk_score: float | None = None,
    feature_importance: list | None = None,
    top_matches: list | None = None,
) -> None:
    if _db_pool is None:
        return
    async with _db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO placement_predictions "
            "  (workflow_id, child_id, recommended, score, confidence, "
            "   risk_score, feature_importance, top_matches, model_version) "
            "VALUES ($1,$2,$3::jsonb,$4,$5,$6,$7::jsonb,$8::jsonb,$9)",
            workflow_id, child_id,
            json.dumps(recommended), score, confidence, risk_score,
            json.dumps(feature_importance) if feature_importance else None,
            json.dumps(top_matches) if top_matches else None,
            model_version,
        )


async def get_latest_prediction(workflow_id: str) -> dict | None:
    if _db_pool is None:
        return None
    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT recommended, score, confidence, risk_score, "
            "       feature_importance, top_matches, model_version, created_at "
            "FROM placement_predictions "
            "WHERE workflow_id = $1 ORDER BY created_at DESC LIMIT 1",
            workflow_id,
        )
    if not row:
        return None
    result = dict(row)
    for col in ("recommended", "feature_importance", "top_matches"):
        val = result.get(col)
        if isinstance(val, str):
            try:
                result[col] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                pass
    return result


async def get_all_placements() -> list[dict]:
    """Fetch the 50 most recently updated placements from PostgreSQL."""
    if _db_pool is None:
        return []
    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                p.*,
                ws.status AS ws_status,
                ws.current_stage,
                ws.progress,
                pp.recommended,
                pp.score AS match_score,
                pp.confidence AS confidence_score,
                pp.risk_score AS predicted_risk_score,
                pp.feature_importance,
                pp.top_matches
            FROM placements p
            LEFT JOIN workflow_status ws ON p.workflow_id = ws.workflow_id
            LEFT JOIN LATERAL (
                SELECT recommended, score, confidence, risk_score,
                       feature_importance, top_matches
                FROM placement_predictions
                WHERE workflow_id = p.workflow_id
                ORDER BY created_at DESC
                LIMIT 1
            ) pp ON TRUE
            ORDER BY p.updated_at DESC
            LIMIT 50
            """
        )
    result = []
    for row in rows:
        record = dict(row)
        for col in ("family_json", "recommended", "feature_importance", "top_matches"):
            val = record.get(col)
            if isinstance(val, str):
                try:
                    record[col] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    pass

        family = record.get("family_json") or record.get("family") or {}
        if isinstance(family, str):
            try:
                family = json.loads(family)
            except (json.JSONDecodeError, TypeError):
                family = {}
        if isinstance(family, dict):
            record["family"] = family
            record["family_id"] = record.get("family_id") or family.get("family_id")
            record["foster_family_name"] = (
                record.get("foster_family_name") or family.get("name")
            )
            record["location"] = record.get("location") or family.get("location")
            record["capacity"] = record.get("capacity") or family.get("capacity")

        recommended = record.get("recommended")
        if recommended is not None:
            if isinstance(recommended, dict):
                record["recommended_family"] = (
                    recommended.get("family") or recommended.get("name")
                )
                if record.get("capacity") is None:
                    record["capacity"] = (
                        recommended.get("capacity")
                        or recommended.get("available_capacity")
                    )
                if record.get("location") is None:
                    record["location"] = recommended.get("location")
                if not record.get("foster_family_name"):
                    record["foster_family_name"] = record["recommended_family"]
                if not record.get("family_id"):
                    record["family_id"] = recommended.get("family_id")
            else:
                record["recommended_family"] = recommended
                if not record.get("foster_family_name"):
                    record["foster_family_name"] = recommended

        # Prefer workflow_status.status over placements.status when present
        if record.get("ws_status"):
            record["status"] = record["ws_status"]

        if record.get("confidence_score") is None and record.get("confidence") is not None:
            record["confidence_score"] = record.get("confidence")

        if record.get("predicted_risk_score") is not None:
            record["risk_score"] = record.get("predicted_risk_score")

        result.append(record)

    logger.info("placements.response", count=len(result))
    return result


# ── Event deduplication (PostgreSQL-backed) ───────────────────────────────────

async def is_duplicate_event(event_id: str) -> bool:
    """
    Check and mark an event as processed using the processed_events table.

    Uses INSERT … ON CONFLICT DO NOTHING and checks affected rows.
    Falls back to Redis (if available) then in-memory set.
    """
    if _db_pool is not None:
        try:
            async with _db_pool.acquire() as conn:
                result = await conn.execute(
                    "INSERT INTO processed_events (event_id) VALUES ($1) "
                    "ON CONFLICT (event_id) DO NOTHING",
                    event_id,
                )
                # asyncpg returns "INSERT 0 <n>" – n=0 means conflict (duplicate)
                inserted = int(result.split()[-1])
                return inserted == 0
        except Exception as exc:  # noqa: BLE001
            logger.warning("api.db_dedup_error", error=str(exc))

    # Fallback: in-memory set (single-replica only)
    if event_id in _processed_events_fallback:
        return True
    _processed_events_fallback.add(event_id)
    if len(_processed_events_fallback) > 10_000:
        _processed_events_fallback.clear()
    return False


_processed_events_fallback: set[str] = set()


async def cleanup_old_processed_events() -> None:
    """Delete processed_events rows older than 7 days. Run daily."""
    if _db_pool is None:
        return
    try:
        async with _db_pool.acquire() as conn:
            deleted = await conn.execute(
                "DELETE FROM processed_events "
                "WHERE processed_at < NOW() - INTERVAL '7 days'"
            )
        count = int(deleted.split()[-1])
        logger.info("api.processed_events_cleanup", deleted=count)
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.processed_events_cleanup_error", error=str(exc))


# ── Pending approvals (DB-backed, replaces in-memory list) ───────────────────

async def add_pending_approval(
    workflow_id: str,
    child_id: str,
    risk_score: float = 0.0,
) -> None:
    """Insert a pending approval record (idempotent via ON CONFLICT DO NOTHING)."""
    if _db_pool is None:
        return
    try:
        async with _db_pool.acquire() as conn:
            result = await conn.execute(
                """
                INSERT INTO pending_approvals (workflow_id, child_id, risk_score, status)
                VALUES ($1, $2, $3, 'pending')
                ON CONFLICT (workflow_id) DO NOTHING
                """,
                workflow_id, child_id, risk_score,
            )
            if result and "INSERT 0 1" in result:
                logger.info("approval_created", workflow_id=workflow_id, child_id=child_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.add_pending_approval.error", error=str(exc))


async def get_pending_approvals_db() -> list[dict]:
    """Return all pending approval rows."""
    if _db_pool is None:
        return []
    try:
        async with _db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT workflow_id, child_id, risk_score, status, created_at "
                "FROM pending_approvals WHERE status = 'pending' "
                "ORDER BY created_at DESC"
            )
        return [dict(r) for r in rows]
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.get_pending_approvals_db.error", error=str(exc))
        return []


async def backfill_missing_placements() -> int:
    """Create placements rows for workflows stuck without one."""
    if _db_pool is None:
        return 0
    count = 0
    try:
        async with _db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (we.workflow_id) we.workflow_id
                FROM workflow_events we
                LEFT JOIN placements p ON p.workflow_id = we.workflow_id
                WHERE p.workflow_id IS NULL
                ORDER BY we.workflow_id, we.timestamp DESC
                """
            )
            for row in rows:
                wfid = row["workflow_id"]
                await conn.execute(
                    """
                    INSERT INTO placements (workflow_id, child_id, status, risk_score, family_id, family_json)
                    SELECT $1,
                           COALESCE(
                               (SELECT data->>'child_id' FROM workflow_events
                                WHERE workflow_id = $1 AND data->>'child_id' IS NOT NULL
                                ORDER BY timestamp DESC LIMIT 1),
                               'unknown'
                           ),
                           'pending', 0.0, 'unassigned', '{}'::jsonb
                    ON CONFLICT (workflow_id) DO NOTHING
                    """,
                    wfid,
                )
                exists = await conn.fetchval(
                    "SELECT 1 FROM active_placements WHERE workflow_id = $1", wfid
                )
                if not exists:
                    cid = await conn.fetchval(
                        """SELECT data->>'child_id' FROM workflow_events
                           WHERE workflow_id = $1 AND data->>'child_id' IS NOT NULL
                           ORDER BY timestamp DESC LIMIT 1""",
                        wfid,
                    )
                    await conn.execute(
                        "INSERT INTO active_placements "
                        "  (workflow_id, child_id, family_id, status) "
                        "VALUES ($1, $2, 'unassigned', 'pending_review')",
                        wfid, cid or 'unknown',
                    )
                count += 1
            if count:
                logger.info("placement_backfill", backfilled=count)
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.backfill_missing_placements.error", error=str(exc))
    return count


async def backfill_missing_approvals() -> int:
    """Find workflows stuck at supervisor_approval without a pending_approvals record."""
    if _db_pool is None:
        return 0
    count = 0
    try:
        async with _db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (we.workflow_id) we.workflow_id
                FROM workflow_events we
                LEFT JOIN pending_approvals pa ON pa.workflow_id = we.workflow_id
                WHERE we.stage IN ('supervisor_approval', 'approval_pending')
                  AND we.status = 'in_progress'
                  AND pa.workflow_id IS NULL
                ORDER BY we.workflow_id, we.timestamp DESC
                """
            )
            for row in rows:
                wfid = row["workflow_id"]
                await conn.execute(
                    """
                    INSERT INTO pending_approvals (workflow_id, child_id, risk_score, status)
                    SELECT $1,
                           COALESCE(p.child_id, ap.child_id, we.child_id, 'unknown'),
                           COALESCE(p.risk_score, 0.0),
                           'pending'
                    FROM (SELECT $1 AS wfid) dummy
                    LEFT JOIN placements p ON p.workflow_id = dummy.wfid
                    LEFT JOIN active_placements ap ON ap.workflow_id = dummy.wfid
                    LEFT JOIN (
                        SELECT workflow_id,
                               data->>'child_id' AS child_id
                        FROM workflow_events
                        WHERE workflow_id = dummy.wfid
                        ORDER BY timestamp DESC
                        LIMIT 1
                    ) we ON TRUE
                    ON CONFLICT (workflow_id) DO NOTHING
                    """,
                    wfid,
                )
                count += 1
            if count:
                logger.info("approval_backfill", backfilled=count)
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.backfill_missing_approvals.error", error=str(exc))
    return count


async def resolve_pending_approval(workflow_id: str, new_status: str) -> None:
    """Mark a pending approval as approved/rejected."""
    if _db_pool is None:
        return
    try:
        async with _db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE pending_approvals SET status = $2 WHERE workflow_id = $1",
                workflow_id, new_status,
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.resolve_pending_approval.error", error=str(exc))


# ── Audit log helper ──────────────────────────────────────────────────────────

import hashlib as _hashlib

_audit_hash_lock = __import__("asyncio").Lock()


def _compute_audit_hash(
    prev_hash: str,
    action: str,
    target_id: str,
    timestamp: str,
    user_id: str,
    role: str,
) -> str:
    """SHA-256 hash of the concatenated audit fields for tamper detection."""
    raw = f"{prev_hash}|{action}|{target_id}|{timestamp}|{user_id}|{role}"
    return _hashlib.sha256(raw.encode()).hexdigest()


async def log_action(
    user_id: str,
    role: str,
    action: str,
    target_type: str,
    target_id: str,
    details: dict[str, Any],
    request: Any | None = None,
) -> None:
    """Write an immutable, hash-chained audit record to PostgreSQL. Non-blocking."""
    ip = request.client.host if request and request.client else None
    ua = request.headers.get("user-agent") if request else None
    try:
        if _db_pool is not None:
            async with _audit_hash_lock:
                async with _db_pool.acquire() as conn:
                    # Fetch the hash of the most recent audit entry for chaining
                    prev_row = await conn.fetchrow(
                        "SELECT hash FROM audit_logs ORDER BY id DESC LIMIT 1"
                    )
                    prev_hash: str = prev_row["hash"] if prev_row and prev_row["hash"] else "0" * 64

                    import datetime as _dt  # noqa: PLC0415
                    ts = _dt.datetime.now(_dt.timezone.utc).isoformat()
                    entry_hash = _compute_audit_hash(
                        prev_hash, action, target_id, ts, user_id, role
                    )

                    await conn.execute(
                        "INSERT INTO audit_logs "
                        "  (user_id, role, action, target_type, target_id, "
                        "   details, ip_address, user_agent, prev_hash, hash) "
                        "VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10)",
                        user_id, role, action, target_type, target_id,
                        json.dumps(details), ip, ua, prev_hash, entry_hash,
                    )
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.audit_log.error", action=action, error=str(exc))


# ── Agent execution helpers ─────────────────────────────────────────────────────


async def store_agent_execution(
    workflow_id: str,
    stage: str,
    agent_name: str,
    *,
    action: str | None = None,
    output: str | None = None,
    confidence: float | None = None,
    latency_seconds: float | None = None,
    status: str = "completed",
    details: dict | None = None,
) -> None:
    """Persist an agent execution record for monitoring/observability."""
    if _db_pool is None:
        return
    try:
        async with _db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO agent_executions "
                "  (workflow_id, stage, agent_name, action, output, confidence, "
                "   latency_seconds, status, details, completed_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, NOW())",
                workflow_id, stage, agent_name, action, output, confidence,
                latency_seconds, status, json.dumps(details) if details else None,
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.store_agent_execution.error", error=str(exc))


async def get_reasoning_traces(
    workflow_id: str,
    stage: str | None = None,
) -> list[dict]:
    """Retrieve reasoning traces for a workflow, optionally filtered by stage."""
    if _db_pool is None:
        return []
    try:
        async with _db_pool.acquire() as conn:
            if stage:
                rows = await conn.fetch(
                    "SELECT id, workflow_id, stage, agent_name, step_index, content, timestamp "
                    "FROM reasoning_traces "
                    "WHERE workflow_id = $1 AND stage = $2 "
                    "ORDER BY step_index ASC",
                    workflow_id, stage,
                )
            else:
                rows = await conn.fetch(
                    "SELECT id, workflow_id, stage, agent_name, step_index, content, timestamp "
                    "FROM reasoning_traces "
                    "WHERE workflow_id = $1 "
                    "ORDER BY timestamp ASC",
                    workflow_id,
                )
        return [dict(r) for r in rows]
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.get_reasoning_traces.error", error=str(exc))
        return []


async def get_agent_executions(
    workflow_id: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Retrieve agent execution records, optionally filtered by workflow."""
    if _db_pool is None:
        return []
    try:
        async with _db_pool.acquire() as conn:
            if workflow_id:
                rows = await conn.fetch(
                    "SELECT id, workflow_id, stage, agent_name, action, output, "
                    "       confidence, latency_seconds, status, details, "
                    "       started_at, completed_at "
                    "FROM agent_executions WHERE workflow_id = $1 "
                    "ORDER BY started_at DESC LIMIT $2",
                    workflow_id, limit,
                )
            else:
                rows = await conn.fetch(
                    "SELECT id, workflow_id, stage, agent_name, action, output, "
                    "       confidence, latency_seconds, status, details, "
                    "       started_at, completed_at "
                    "FROM agent_executions "
                    "ORDER BY started_at DESC LIMIT $1",
                    limit,
                )
        return [dict(r) for r in rows]
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.get_agent_executions.error", error=str(exc))
        return []


# ── User authentication helpers (PostgreSQL-backed) ────────────────────────────


async def get_user_by_email(email: str) -> dict | None:
    """Look up a user by email in the users table."""
    if _db_pool is None:
        return None
    try:
        async with _db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, email, password_hash, role, display_name, is_active, "
                "       created_at, last_login_at "
                "FROM users WHERE email = $1 AND is_active = TRUE",
                email,
            )
        return dict(row) if row else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.get_user_by_email.error", error=str(exc))
        return None


async def update_last_login(email: str) -> None:
    """Update the last_login_at timestamp for a user."""
    if _db_pool is None:
        return
    try:
        async with _db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET last_login_at = NOW() WHERE email = $1",
                email,
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.update_last_login.error", error=str(exc))
