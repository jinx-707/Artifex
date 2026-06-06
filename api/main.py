"""
Artifex REST API – router aggregator and startup handler.

Schema is managed by Alembic (alembic upgrade head runs at startup).
All route logic lives in api/routes/ and api/websockets/.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path as _EnvPath
from typing import Any

from dotenv import load_dotenv

# Resolve .env from the repo root, falling back to cwd-relative for Docker.
_repo_root = _EnvPath(__file__).resolve().parent.parent
load_dotenv(dotenv_path=_repo_root / ".env", override=False)
load_dotenv(override=False)

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://artifex:artifex123@postgres:5432/placements"
)

import structlog
from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field
from temporalio.client import WorkflowExecutionStatus, WorkflowQueryFailedError, WorkflowQueryRejectedError
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError
from temporalio.service import TLSConfig

from nats_client.client import NATSManager
from .dependencies import get_settings, get_temporal_client
from .auth import get_current_user, require_role, verify_ws_token, authenticate_user, create_access_token
from .db import (
    init_db_pool, run_alembic_upgrade,
    store_placement, store_workflow_event, store_prediction,
    get_workflow_timeline, get_workflow_status_db, get_latest_prediction,
    get_all_placements, cleanup_old_processed_events,
)

logger = structlog.get_logger()

_TEMPORAL_STATUS_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_TEMPORAL_STATUS_TTL_SECONDS = 8.0
_TEMPORAL_STATUS_TIMEOUT_SECONDS = 1.5


async def _read_temporal_status(workflow_id: str) -> dict[str, Any] | None:
    """Get a lightweight Temporal status snapshot with timeout and TTL caching."""
    now = time.time()
    cached = _TEMPORAL_STATUS_CACHE.get(workflow_id)
    if cached and (now - cached[0]) < _TEMPORAL_STATUS_TTL_SECONDS:
        return cached[1]

    try:
        client = await get_temporal_client()
        handle = client.get_workflow_handle(workflow_id)
        async with asyncio.timeout(_TEMPORAL_STATUS_TIMEOUT_SECONDS):
            wf_status = await handle.query("get_status")
        if isinstance(wf_status, dict):
            result = {
                "status": wf_status.get("status") or "unknown",
                "current_stage": wf_status.get("current_stage"),
                "progress": wf_status.get("progress") or 0,
                "risk_score": wf_status.get("risk_score"),
                "match_score": wf_status.get("match_score"),
                "confidence_score": wf_status.get("confidence_score"),
                "active": wf_status.get("active", True),
            }
            _TEMPORAL_STATUS_CACHE[workflow_id] = (now, result)
            return result
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.temporal_status_skipped", workflow_id=workflow_id, error=str(exc))
    return None

# ── Prometheus metrics ────────────────────────────────────────────────────────
REQUEST_COUNT = Counter(
    "artifex_api_requests_total", "Total API requests", ["endpoint", "status"]
)
REQUEST_LATENCY = Histogram(
    "artifex_api_latency_seconds", "API request latency", ["endpoint"]
)

# ── In-process state (shared with route modules) ──────────────────────────────
_api_latest_placements: list[dict[str, Any]] = []
_placements_lock = asyncio.Lock()
_agent_heartbeats: dict[str, float] = {}
_heartbeat_lock = asyncio.Lock()

# Known foster-care agents the orchestration page expects
KNOWN_AGENTS = [
    "intake", "planner", "risk", "matching",
    "fairness", "approval", "monitoring",
]

# ── Pydantic models ───────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    goal: str = Field(..., min_length=1, max_length=4096)
    max_retries: int = Field(default=3, ge=0, le=10)


class RunResponse(BaseModel):
    workflow_id: str
    trace_id: str
    status: str = "started"
    message: str = "Workflow submitted successfully"


class StatusResponse(BaseModel):
    workflow_id: str
    status: str
    result: Any = None


class EmergentRunRequest(BaseModel):
    goal: str = Field(..., min_length=1, max_length=4096)


class LoginRequest(BaseModel):
    email: str = Field(..., min_length=1, max_length=254)
    password: str = Field(..., min_length=1, max_length=128)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    user_id: str


# ── NATS subscribers ──────────────────────────────────────────────────────────

async def _heartbeat_subscriber(manager: NATSManager) -> None:
    async def _handle(msg: dict) -> None:
        agent_name = msg.get("agent") or msg.get("name", "unknown")
        async with _heartbeat_lock:
            was_missing = agent_name not in _agent_heartbeats
            _agent_heartbeats[agent_name] = time.time()
        if was_missing:
            logger.info("agent_registered", agent=agent_name, source="nats")
        logger.debug("heartbeat_received", agent=agent_name, source="nats")

    await manager.subscribe("agent.*.heartbeat", _handle)
    logger.info("api.nats_subscriber_ready", subject="agent.*.heartbeat")
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass


async def _workflow_event_subscriber(manager: NATSManager) -> None:
    """NATS fallback for workflow events published by Temporal activities."""
    from api.db import store_workflow_event as _store  # noqa: PLC0415

    async def _handle(msg: dict) -> None:
        workflow_id = msg.get("workflow_id", "")
        stage = msg.get("stage", "unknown")
        status = msg.get("status", "unknown")
        data = msg.get("data") or {}
        if not workflow_id:
            logger.warning("ws.workflow_event_subscriber.missing_workflow_id")
            return
        logger.info(
            "api.workflow_event_nats_received",
            workflow_id=workflow_id,
            stage=stage,
            status=status,
        )
        try:
            await _store(workflow_id, stage=stage, status=status, data=data)
        except Exception:  # noqa: BLE001
            logger.exception(
                "api.workflow_event_nats_store_error",
                workflow_id=workflow_id,
                stage=stage,
            )

    await manager.subscribe("foster.workflow_events", _handle)
    logger.info("api.nats_subscriber_ready", subject="foster.workflow_events")
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass


async def _placement_subscriber(manager: NATSManager) -> None:
    async def _handle(msg: dict) -> None:
        child_id = msg.get("child_id", "unknown")
        await store_placement(msg)
        wf_id = msg.get("workflow_id") or msg.get("workflowId") or f"foster-{child_id}"
        try:
            await store_workflow_event(wf_id, "placement_matched", "completed", msg)
        except Exception:  # noqa: BLE001
            logger.exception("api._placement_subscriber.store_event_error", workflow_id=wf_id)
        try:
            recommended = {"family": msg.get("family"), "explanation": msg.get("match_explanation")}
            await store_prediction(
                wf_id, child_id, recommended,
                score=msg.get("match_score") or msg.get("score") or msg.get("risk_score"),
                confidence=msg.get("confidence"),
                risk_score=msg.get("risk_score"),
                feature_importance=msg.get("feature_importance"),
                top_matches=msg.get("top_matches"),
                model_version=msg.get("model_version"),
            )
        except Exception:  # noqa: BLE001
            logger.exception("api._placement_subscriber.store_prediction_error", workflow_id=wf_id)
        global _api_latest_placements
        async with _placements_lock:
            for i, p in enumerate(_api_latest_placements):
                if p.get("child_id") == child_id:
                    _api_latest_placements[i] = msg
                    break
            else:
                _api_latest_placements.append(msg)
            _api_latest_placements = _api_latest_placements[-50:]
        logger.info("api.placement_received_nats", child_id=child_id)

    await manager.subscribe("foster.placements", _handle)
    logger.info("api.nats_subscriber_ready", subject="foster.placements")
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass


async def _daily_cleanup_loop() -> None:
    """Run processed_events cleanup once per day."""
    while True:
        await asyncio.sleep(86400)  # 24 hours
        try:
            await cleanup_old_processed_events()
        except Exception:  # noqa: BLE001
            logger.exception("api.daily_cleanup.error")


async def _seed_agent_registrations() -> None:
    """
    Seed simulated agent registrations so the /agent/status endpoint and
    orchestration page always have agents to display, even when the real
    NATS-based agents are not running in the local dev environment.
    """
    for name in KNOWN_AGENTS:
        async with _heartbeat_lock:
            _agent_heartbeats[name] = time.time()
        logger.info("agent_registered", agent=name, source="seed")
    logger.info("api.seeded_agent_registrations", agents=KNOWN_AGENTS)
    # Refresh heartbeats every 30s to prevent agents from appearing stale
    while True:
        await asyncio.sleep(30)
        for name in KNOWN_AGENTS:
            async with _heartbeat_lock:
                _agent_heartbeats[name] = time.time()
        logger.debug("heartbeat_received", agents=KNOWN_AGENTS, source="seed")


async def _resilient_task(coro_factory, name: str, delay: float = 5.0) -> None:
    """
    Fix ⑤: Run *coro_factory()* in a loop so that if the coroutine raises an
    unhandled exception the task is automatically restarted after *delay*
    seconds instead of dying silently.
    """
    while True:
        try:
            await coro_factory()
        except asyncio.CancelledError:
            raise  # propagate cancellation so the lifespan can shut down cleanly
        except Exception:  # noqa: BLE001
            logger.exception(f"api.{name}.crashed_restarting", restart_in_seconds=delay)
            await asyncio.sleep(delay)


# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not GROQ_API_KEY:
        raise RuntimeError(
            "GROQ_API_KEY is not set. Add it to your .env file or export it in your shell."
        )

    # Run Alembic migrations before opening the pool (safe for multi-replica deploys
    # because Alembic uses an advisory lock internally).
    run_alembic_upgrade()

    # Initialise PostgreSQL connection pool (schema already up-to-date)
    await init_db_pool()

    # Seed demo families if the table is empty
    await _seed_demo_families()

    # Backfill placements & approval records for stuck workflows
    try:
        from api.db import backfill_missing_placements as _bp, backfill_missing_approvals as _ba
        pf = await _bp()
        if pf:
            logger.info("api.startup.backfilled_placements", count=pf)
        af = await _ba()
        if af:
            logger.info("api.startup.backfilled_approvals", count=af)
    except Exception as _e:  # noqa: BLE001
        logger.warning("api.startup.backfill_error", error=str(_e))

    # Share in-process state with route modules
    from .routes.placements import set_placement_store
    from .routes.dashboard import set_heartbeat_store
    set_placement_store(_api_latest_placements, _placements_lock)
    set_heartbeat_store(_agent_heartbeats, _heartbeat_lock)

    # Try to connect to NATS – non-fatal if unavailable
    manager: NATSManager | None = None
    placement_task = heartbeat_task = None
    try:
        manager = NATSManager(NATS_URL)
        await manager.connect()
        logger.info("api.startup.nats_connected", nats_url=NATS_URL)
        placement_task = asyncio.create_task(
            _resilient_task(lambda: _placement_subscriber(manager), "placement_subscriber")
        )
        heartbeat_task = asyncio.create_task(
            _resilient_task(lambda: _heartbeat_subscriber(manager), "heartbeat_subscriber")
        )
        workflow_event_task = asyncio.create_task(
            _resilient_task(lambda: _workflow_event_subscriber(manager), "workflow_event_subscriber")
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.startup.nats_unavailable", error=str(exc),
                       note="Running without NATS – WebSocket polling fallback active")

    # Always seed agent registrations so the orchestration page has data,
    # even when real NATS agents aren't running.
    seed_task = asyncio.create_task(_seed_agent_registrations())

    cleanup_task = asyncio.create_task(_daily_cleanup_loop())

    try:
        from scripts.load_afcars_data import background_refresh  # noqa: PLC0415
        refresh_task = asyncio.create_task(background_refresh(interval_seconds=900))
    except Exception:  # noqa: BLE001
        refresh_task = None

    yield

    if placement_task:
        placement_task.cancel()
    if heartbeat_task:
        heartbeat_task.cancel()
    if workflow_event_task:
        workflow_event_task.cancel()
    seed_task.cancel()
    cleanup_task.cancel()
    if refresh_task:
        refresh_task.cancel()
    async with _heartbeat_lock:
        for name in list(_agent_heartbeats):
            logger.info("agent_unregistered", agent=name, source="shutdown")
            del _agent_heartbeats[name]
    if manager:
        try:
            await manager.close()
        except Exception:  # noqa: BLE001
            pass
    logger.info("api.shutdown")


async def _seed_demo_families() -> None:
    """Insert 2 demo foster families if the families table is empty."""
    from .db import get_pool as _gp  # noqa: PLC0415
    pool = _gp()
    if pool is None:
        return
    try:
        async with pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM families")
            if count and count > 0:
                return  # already seeded
            await conn.execute(
                """
                INSERT INTO families
                    (family_id, name, location, capacity, total_capacity, available_capacity, active,
                     experience, experience_level, specializations, languages, languages_arr,
                     special_needs_trained, accepts_siblings, sibling_group_capable,
                     emergency_available, max_age, can_take_siblings, has_animals)
                VALUES
                    ('F-DEMO01', 'The Johnson Family', 'Springfield, IL', 3, 3, 3, TRUE,
                     'high', 'high', 'Trauma-informed care, adolescent support',
                     'English, Spanish', ARRAY['English','Spanish'],
                     TRUE, TRUE, TRUE, TRUE, 17, TRUE, FALSE),
                    ('F-DEMO02', 'The Williams Family', 'Chicago, IL', 2, 2, 2, TRUE,
                     'medium', 'medium', 'Early childhood, developmental support',
                     'English', ARRAY['English'],
                     FALSE, FALSE, FALSE, FALSE, 10, FALSE, TRUE)
                ON CONFLICT (family_id) DO NOTHING
                """
            )
            logger.info("api.seed_demo_families.done")
    except Exception as exc:  # noqa: BLE001
        logger.warning("api.seed_demo_families.error", error=str(exc))


# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Artifex Agent Swarm API",
    description="Production multi-agent system powered by LangGraph + NATS + Temporal",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://artifex-mir62bp4v-allu-saatvika-reddys-projects.vercel.app",
        "https://artifex-woad-beta.vercel.app",
        "http://localhost:3000", "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://localhost:5173", "http://127.0.0.1:5173",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

FastAPIInstrumentor.instrument_app(app)

# ── Include route modules ─────────────────────────────────────────────────────
from .routes.dashboard  import router as dashboard_router   # noqa: E402
from .routes.families   import router as families_router    # noqa: E402
from .routes.placements import router as placements_router  # noqa: E402
from .routes.referral   import router as referral_router    # noqa: E402
from .routes.children   import router as children_router    # noqa: E402
from .routes.audit      import router as audit_router       # noqa: E402
from .routes.crisis    import router as crisis_router     # noqa: E402
from .routes.ml_audit  import router as ml_audit_router   # noqa: E402
from .routes.fairness   import router as fairness_router    # noqa: E402
from .routes.twin      import router as twin_router       # noqa: E402
from .routes.timeline  import router as timeline_router   # noqa: E402
from .websockets.dashboard import router as ws_dashboard_router  # noqa: E402
from .websockets.logs      import router as ws_logs_router       # noqa: E402
from .websockets.workflow  import router as ws_workflow_router   # noqa: E402
from .websockets.child     import router as ws_child_router      # noqa: E402

app.include_router(dashboard_router)
app.include_router(families_router)
app.include_router(placements_router)
app.include_router(referral_router)
app.include_router(children_router)
app.include_router(audit_router)
app.include_router(crisis_router)
app.include_router(ml_audit_router)
app.include_router(fairness_router)
app.include_router(twin_router)
app.include_router(timeline_router)
app.include_router(ws_dashboard_router)
app.include_router(ws_logs_router)
app.include_router(ws_workflow_router)
app.include_router(ws_child_router)


# ── Core swarm routes (kept in main.py – they depend on Temporal client) ─────

@app.post("/swarm/run", response_model=RunResponse, status_code=202)
async def run_swarm(
    request: RunRequest,
    settings: dict = Depends(get_settings),
) -> RunResponse:
    workflow_id = f"artifex-{uuid.uuid4().hex[:12]}"
    trace_id = uuid.uuid4().hex
    logger.info("api.run_swarm", goal=request.goal[:80], workflow_id=workflow_id)
    try:
        client = await get_temporal_client()
        await client.start_workflow(
            "ArtifexSwarmWorkflow", request.goal,
            id=workflow_id, task_queue=settings["temporal_task_queue"],
        )
        REQUEST_COUNT.labels(endpoint="/swarm/run", status="202").inc()
        return RunResponse(workflow_id=workflow_id, trace_id=trace_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("api.run_swarm.error", error=str(exc))
        REQUEST_COUNT.labels(endpoint="/swarm/run", status="500").inc()
        raise HTTPException(status_code=500, detail=f"Failed to start workflow: {exc}") from exc


@app.get("/swarm/status/{workflow_id}", response_model=StatusResponse)
async def get_status(workflow_id: str) -> StatusResponse:
    try:
        client = await get_temporal_client()
        handle = client.get_workflow_handle(workflow_id)
        desc = await handle.describe()
        status_map = {
            WorkflowExecutionStatus.RUNNING: "running",
            WorkflowExecutionStatus.COMPLETED: "completed",
            WorkflowExecutionStatus.FAILED: "failed",
            WorkflowExecutionStatus.CANCELED: "canceled",
            WorkflowExecutionStatus.TERMINATED: "terminated",
            WorkflowExecutionStatus.TIMED_OUT: "timed_out",
        }
        status_str = status_map.get(desc.status, "unknown")
        result = None
        if desc.status == WorkflowExecutionStatus.COMPLETED:
            result = await handle.result()
        REQUEST_COUNT.labels(endpoint="/swarm/status", status="200").inc()
        return StatusResponse(workflow_id=workflow_id, status=status_str, result=result)
    except Exception as exc:  # noqa: BLE001
        logger.exception("api.get_status.error", workflow_id=workflow_id, error=str(exc))
        raise HTTPException(status_code=404, detail=f"Workflow not found: {workflow_id}") from exc


@app.get("/health")
async def health() -> dict[str, Any]:
    """
    Deep health check – probes PostgreSQL, NATS, and Temporal.
    Returns per-service status and latency so the monitoring page
    can display real infrastructure state instead of hardcoded strings.
    """
    import time as _time  # noqa: PLC0415

    result: dict[str, Any] = {"status": "ok", "service": "artifex-api", "services": {}}

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    from .db import get_pool as _get_pool  # noqa: PLC0415
    pool = _get_pool()
    if pool is not None:
        t0 = _time.monotonic()
        try:
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            latency_ms = round((_time.monotonic() - t0) * 1000, 1)
            result["services"]["postgres"] = {"status": "healthy", "latency_ms": latency_ms}
        except Exception as exc:  # noqa: BLE001
            result["services"]["postgres"] = {"status": "unhealthy", "error": str(exc)}
            result["status"] = "degraded"
    else:
        result["services"]["postgres"] = {"status": "unavailable"}
        result["status"] = "degraded"

    # ── NATS ──────────────────────────────────────────────────────────────────
    t0 = _time.monotonic()
    try:
        import nats as nats_lib  # noqa: PLC0415
        nc = await nats_lib.connect(NATS_URL, connect_timeout=2)
        await nc.drain()
        latency_ms = round((_time.monotonic() - t0) * 1000, 1)
        result["services"]["nats"] = {"status": "connected", "latency_ms": latency_ms}
    except Exception as exc:  # noqa: BLE001
        result["services"]["nats"] = {"status": "disconnected", "error": str(exc)}
        result["status"] = "degraded"

    # ── Temporal ─────────────────────────────────────────────────────────────
    t0 = _time.monotonic()
    try:
        from .dependencies import get_settings as _gs  # noqa: PLC0415
        from temporalio.client import Client as _TClient  # noqa: PLC0415
        settings = _gs()
        print("HEALTH TEMPORAL_HOST =", settings["temporal_host"])
        print("HEALTH TEMPORAL_NAMESPACE =", settings["temporal_namespace"])
        client = await _TClient.connect(
            settings["temporal_host"],
            namespace=settings["temporal_namespace"],
        )
        await client.service_client.check_health()
        latency_ms = round((_time.monotonic() - t0) * 1000, 1)
        result["services"]["temporal"] = {"status": "connected", "latency_ms": latency_ms}
    except Exception as exc:  # noqa: BLE001
        # Temporal probe failure is non-fatal – API still serves requests
        result["services"]["temporal"] = {"status": "unavailable", "error": str(exc)[:120]}

    return result


@app.post("/chat")
async def chat(request: Request, settings: dict = Depends(get_settings)) -> dict[str, Any]:
    """
    AI assistant endpoint – calls Groq directly for instant responses.
    Falls back to Temporal workflow if Groq is unavailable.
    """
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        body = await request.json()
        question = (
            body.get("message")
            or body.get("goal")
            or body.get("question")
            or body.get("query", "")
        )
    else:
        raw = await request.body()
        question = raw.decode("utf-8").strip()
    if not question:
        raise HTTPException(status_code=400, detail="Empty question")

    # ── Try Groq directly first (fast, no Temporal needed) ────────────────────
    if GROQ_API_KEY:
        try:
            import httpx as _httpx  # noqa: PLC0415
            from .db import get_all_placements as _get_placements  # noqa: PLC0415

            # Build context from live DB data
            placements = await _get_placements()
            pending_count = sum(1 for p in placements if p.get("status") in ("pending", "pending_supervisor"))
            approved_count = sum(1 for p in placements if p.get("status") == "approved")
            total_count = len(placements)

            system_prompt = (
                "You are an AI assistant for the Artifex foster care orchestration platform. "
                "You help caseworkers and supervisors manage foster care placements, track workflows, "
                "and make data-driven decisions. Be concise, helpful, and professional.\n\n"
                f"Current system state: {total_count} total placements, "
                f"{pending_count} pending approval, {approved_count} approved.\n"
                "You can help with: workflow status, placement matching, risk assessment, "
                "approval processes, family matching criteria, and system navigation."
            )

            async with _httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {GROQ_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "llama-3.1-8b-instant",
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": question},
                        ],
                        "temperature": 0.7,
                        "max_tokens": 1024,
                    },
                )
                if resp.status_code == 200:
                    answer = resp.json()["choices"][0]["message"]["content"].strip()
                    REQUEST_COUNT.labels(endpoint="/chat", status="200").inc()
                    logger.info("api.chat.groq_response", question=question[:60])
                    return {"id": f"chat-{uuid.uuid4().hex[:8]}", "message": answer, "sources": []}
        except Exception as exc:  # noqa: BLE001
            logger.warning("api.chat.groq_error", error=str(exc))

    # ── Fallback: try Temporal workflow ───────────────────────────────────────
    workflow_id = f"chat-{uuid.uuid4().hex[:12]}"
    try:
        client = await get_temporal_client()
        handle = await client.start_workflow(
            "ArtifexSwarmWorkflow", question,
            id=workflow_id, task_queue=settings["temporal_task_queue"],
        )
        REQUEST_COUNT.labels(endpoint="/chat", status="202").inc()
    except Exception as exc:  # noqa: BLE001
        REQUEST_COUNT.labels(endpoint="/chat", status="503").inc()
        # Both Groq and Temporal failed – return a helpful error message
        return {
            "id": f"chat-{uuid.uuid4().hex[:8]}",
            "message": (
                "I'm having trouble connecting to the AI backend right now. "
                "Please check that the GROQ_API_KEY is set in your .env file and try again."
            ),
            "sources": [],
        }

    deadline = time.monotonic() + 60.0
    result: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        try:
            from temporalio.client import WorkflowExecutionStatus as WES  # noqa: PLC0415
            desc = await handle.describe()
            if desc.status == WES.COMPLETED:
                result = await handle.result()
                break
            elif desc.status in (WES.FAILED, WES.CANCELED, WES.TERMINATED, WES.TIMED_OUT):
                raise HTTPException(status_code=500, detail=f"Workflow ended: {desc.status.name}")
        except HTTPException:
            raise
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(2)

    if result is None:
        REQUEST_COUNT.labels(endpoint="/chat", status="504").inc()
        return {
            "id": f"chat-{uuid.uuid4().hex[:8]}",
            "message": "The AI assistant timed out. Please try again.",
            "sources": [],
        }

    REQUEST_COUNT.labels(endpoint="/chat", status="200").inc()
    final = result.get("final_answer", result)
    if isinstance(final, dict):
        answer = final.get("answer", "")
        sources = final.get("sources", [])
    elif isinstance(final, list) and final:
        answer = final[0].get("payload", {}).get("text", str(final[0]))
        sources = []
    else:
        answer = str(final) if final else "No answer returned."
        sources = []
    return {
        "id": f"chat-{uuid.uuid4().hex[:8]}",
        "message": answer or "No answer returned.",
        "sources": sources,
    }


def _normalize_foster_workflow_id(raw_id: str) -> str:
    if not isinstance(raw_id, str):
        return str(raw_id)
    trimmed = raw_id.strip()
    if trimmed.lower().startswith("foster-"):
        return trimmed
    m = re.match(r"^CHILD-(\d+)$", trimmed, re.IGNORECASE)
    if m:
        return f"foster-{m.group(1)}"
    m2 = re.match(r"^CH-(\d+)$", trimmed, re.IGNORECASE)
    if m2:
        return f"foster-{m2.group(1)}"
    if re.fullmatch(r"\d+", trimmed):
        return f"foster-{trimmed}"
    return f"foster-{trimmed}"


@app.get("/foster/status/{workflow_id}")
async def get_foster_status(workflow_id: str) -> dict[str, Any]:
    workflow_id = _normalize_foster_workflow_id(workflow_id)

    # ── Always read from DB first (works without Temporal) ───────────────────
    timeline = await get_workflow_timeline(workflow_id, limit=200)
    wf_db = await get_workflow_status_db(workflow_id)
    prediction = await get_latest_prediction(workflow_id)

    # Also pull placement row for child_id / family data
    placement_row: dict[str, Any] = {}
    from .db import get_pool as _gp  # noqa: PLC0415
    pool = _gp()
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT child_id, family_id, family_json, risk_score, status "
                    "FROM placements WHERE workflow_id = $1",
                    workflow_id,
                )
                if row:
                    placement_row = dict(row)
                    fj = placement_row.get("family_json")
                    if isinstance(fj, str):
                        try:
                            placement_row["family_json"] = json.loads(fj)
                        except (json.JSONDecodeError, TypeError):
                            pass
        except Exception:  # noqa: BLE001
            pass

    # ── Extract prediction data ───────────────────────────────────────────────
    recommended_family: Any = None
    match_score = confidence_score = risk_score = None
    feature_importance = top_matches = None

    if prediction:
        rec = prediction.get("recommended")
        if isinstance(rec, dict):
            recommended_family = rec.get("family") or rec
        elif isinstance(rec, str):
            try:
                rec_parsed = json.loads(rec)
                recommended_family = rec_parsed.get("family") or rec_parsed
            except (json.JSONDecodeError, TypeError):
                recommended_family = rec
        else:
            recommended_family = rec
        match_score = prediction.get("score")
        confidence_score = prediction.get("confidence")
        risk_score = prediction.get("risk_score")
        feature_importance = prediction.get("feature_importance") or None
        top_matches = prediction.get("top_matches") or None

    # Fall back to placement row for risk_score
    if risk_score is None and placement_row.get("risk_score"):
        risk_score = float(placement_row["risk_score"])

    # Family from placement row
    if recommended_family is None:
        fj = placement_row.get("family_json")
        if isinstance(fj, dict):
            recommended_family = fj

    capacity = None
    if isinstance(recommended_family, dict):
        capacity = recommended_family.get("capacity") or recommended_family.get("available_capacity")

    current_stage = (wf_db and wf_db.get("current_stage")) or (
        timeline[-1]["stage"] if timeline else None
    )
    progress = (wf_db and wf_db.get("progress")) or 0
    status_val = (
        (wf_db and wf_db.get("status"))
        or placement_row.get("status")
        or "pending"
    )
    child_id = placement_row.get("child_id") or (wf_db and wf_db.get("child_id"))

    db_result = {
        "workflow_id":        workflow_id,
        "status":             status_val,
        "active":             status_val not in ("approved", "rejected", "closed"),
        "child_id":           child_id,
        "family_id":          placement_row.get("family_id") or (wf_db and wf_db.get("family_id")),
        "recommended_family": (
            recommended_family.get("name") or recommended_family.get("family_id")
            if isinstance(recommended_family, dict) else recommended_family
        ),
        "match_score":        match_score,
        "confidence_score":   confidence_score,
        "risk_score":         risk_score,
        "capacity":           capacity,
        "current_stage":      current_stage,
        "progress":           progress,
        "timeline":           timeline,
        "feature_importance": feature_importance,
        "top_matches":        top_matches,
    }

    # ── Optional Temporal fallback with timeout and caching ───────────────────
    temporal_status = None
    if not wf_db or (wf_db.get("status") in ("pending", "running", "in_progress", "in-progress")):
        temporal_status = await _read_temporal_status(workflow_id)
    if isinstance(temporal_status, dict):
        if db_result["match_score"] is None:
            db_result["match_score"] = temporal_status.get("match_score")
        if db_result["confidence_score"] is None:
            db_result["confidence_score"] = temporal_status.get("confidence_score")
        if db_result["risk_score"] is None:
            db_result["risk_score"] = temporal_status.get("risk_score")
        if db_result["current_stage"] is None:
            db_result["current_stage"] = temporal_status.get("current_stage")
        if db_result["progress"] is None or db_result["progress"] == 0:
            db_result["progress"] = temporal_status.get("progress") or 0
        db_result["active"] = temporal_status.get("active", db_result["active"])

    # If nothing found at all, return 404
    if not timeline and not wf_db and not placement_row:
        raise HTTPException(status_code=404, detail=f"Workflow not found: {workflow_id}")

    return db_result


@app.get("/workflow/{workflow_id}")
async def workflow_summary(workflow_id: str) -> dict[str, Any]:
    wf = await get_workflow_status_db(workflow_id)
    timeline = await get_workflow_timeline(workflow_id, limit=50)
    if not wf:
        try:
            client = await get_temporal_client()
            handle = client.get_workflow_handle(workflow_id)
            desc = await handle.describe()
            from temporalio.client import WorkflowExecutionStatus as WES  # noqa: PLC0415
            status_map = {
                WES.RUNNING: "running", WES.COMPLETED: "completed",
                WES.FAILED: "failed", WES.CANCELED: "canceled",
                WES.TERMINATED: "terminated", WES.TIMED_OUT: "timed_out",
            }
            wf = {"workflow_id": workflow_id, "status": status_map.get(desc.status, "unknown"),
                  "current_stage": None, "progress": 0, "updated_at": None}
        except Exception:
            wf = {"workflow_id": workflow_id, "status": "unknown", "current_stage": None, "progress": 0}
    return {**wf, "timeline": timeline}


@app.get("/workflow/{workflow_id}/timeline")
async def workflow_timeline(workflow_id: str) -> dict[str, Any]:
    timeline = await get_workflow_timeline(workflow_id, limit=500)
    return {"workflow_id": workflow_id, "timeline": timeline}


@app.get("/workflow/{workflow_id}/progress")
async def workflow_progress(workflow_id: str) -> dict[str, Any]:
    wf = await get_workflow_status_db(workflow_id)
    if not wf:
        return {"workflow_id": workflow_id, "progress": 0}
    return {"workflow_id": workflow_id, "progress": wf.get("progress", 0), "status": wf.get("status")}


@app.post("/api/foster_home")
async def register_foster_home(
    home: dict[str, Any],
    request: Request,
    user: dict = Depends(get_current_user),
) -> dict[str, str]:
    """Register a new foster home (legacy endpoint – prefer POST /families)."""
    from .db import log_action as _log  # noqa: PLC0415
    pool = __import__("api.db", fromlist=["get_pool"]).get_pool()
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                family_id = f"F-{uuid.uuid4().hex[:6].upper()}"
                total_cap = int(home.get("capacity", 1))
                exp = home.get("experience", "new")
                languages_str = home.get("languages", "")
                languages_arr = [
                    p.strip() for p in languages_str.split(",") if p.strip()
                ]
                await conn.execute(
                    """
                    INSERT INTO families
                        (family_id, name, location,
                         capacity, total_capacity, active,
                         experience, experience_level,
                         specializations, languages, languages_arr,
                         special_needs_trained, accepts_siblings, sibling_group_capable,
                         emergency_available,
                         max_age, can_take_siblings, has_animals, updated_at)
                    VALUES ($1,$2,$3,$4,$5,TRUE,$6,$7,$8,$9,$10::text[],
                            $11,$12,$13,$14,$15,$16,$17,NOW())
                    """,
                    family_id,
                    home.get("name", "Unknown"),
                    home.get("location", ""),
                    total_cap,
                    total_cap,
                    exp,
                    exp,
                    home.get("specializations", ""),
                    languages_str,
                    languages_arr,                                                    # $10 – distinct from $9
                    bool(home.get("special_needs_trained", False)),
                    bool(home.get("accepts_siblings", False)),
                    bool(home.get("accepts_siblings", False)),
                    bool(home.get("emergency_available", False) or home.get("accepts_emergency", False)),
                    int(home.get("max_age", 18)),
                    bool(home.get("can_take_siblings", False) or home.get("accepts_siblings", False)),
                    bool(home.get("has_animals", False)),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("api.foster_home.db_error", error=str(exc))
            raise HTTPException(status_code=500, detail=f"Database error: {exc}") from exc
    try:
        manager = NATSManager(NATS_URL)
        await manager.publish("events.family_update", home)
    except Exception:  # noqa: BLE001
        pass
    await _log(
        user_id=user["user_id"], role=user["role"],
        action="REGISTER_FOSTER_HOME",
        target_type="family", target_id=home.get("name", "unknown"),
        details={"location": home.get("location"), "capacity": home.get("capacity"),
                 "specializations": home.get("specializations")},
        request=request,
    )
    return {"status": "ok", "message": "Foster home registered"}


@app.post("/api/incident")
async def report_incident(
    data: dict[str, Any],
    request: Request,
    user: dict = Depends(get_current_user),
) -> dict[str, str]:
    from .db import log_action as _log  # noqa: PLC0415
    try:
        manager = NATSManager(NATS_URL)
        await manager.publish("events.check_in", {
            **data,
            "event_type": "incident",
            "timestamp": __import__("datetime").datetime.now().isoformat(),
        })
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to publish incident: {exc}") from exc
    await _log(
        user_id=user["user_id"], role=user["role"],
        action="REPORT_INCIDENT",
        target_type="placement", target_id=data.get("workflow_id", "unknown"),
        details={"type": data.get("type"), "severity": data.get("severity"),
                 "notes": data.get("notes", "")[:200]},
        request=request,
    )
    return {"status": "ok", "message": "Incident logged – swarm will update risk score"}


@app.get("/api/search_families")
async def search_families(q: str = "") -> dict[str, Any]:
    if not q.strip():
        return {"results": [], "query": q}
    try:
        manager = NATSManager(NATS_URL)
        response = await manager.request(
            "agent.retriever.search", {"query": q, "top_k": 10}, timeout=5.0
        )
        return {"results": response.get("results", []), "query": q, "source": "vector"}
    except Exception:  # noqa: BLE001
        pass
    from .db import get_pool as _gp  # noqa: PLC0415
    pool = _gp()
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id::text, family_id, name, location, capacity,
                           experience, specializations,
                           special_needs_trained, accepts_siblings, max_age
                    FROM families
                    WHERE name ILIKE $1 OR location ILIKE $1
                       OR specializations ILIKE $1 OR languages ILIKE $1
                    ORDER BY updated_at DESC LIMIT 10
                    """,
                    f"%{q}%",
                )
            return {
                "results": [dict(r) for r in rows],
                "query": q,
                "source": "database",
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("api.search_families.db_error", error=str(exc))
    return {"results": [], "query": q, "source": "none"}


@app.post("/emergent/run", status_code=202)
async def emergent_run(
    request: EmergentRunRequest,
    settings: dict = Depends(get_settings),
) -> dict[str, str]:
    workflow_id = f"emergent-{uuid.uuid4().hex[:12]}"
    try:
        client = await get_temporal_client()
        await client.start_workflow(
            "EmergentSwarmWorkflow", request.goal,
            id=workflow_id, task_queue=settings["temporal_task_queue"],
        )
        REQUEST_COUNT.labels(endpoint="/emergent/run", status="202").inc()
        return {"workflow_id": workflow_id, "status": "started",
                "message": "Emergent swarm auction initiated – agents are bidding"}
    except Exception as exc:  # noqa: BLE001
        REQUEST_COUNT.labels(endpoint="/emergent/run", status="500").inc()
        raise HTTPException(status_code=500, detail=f"Failed to start emergent workflow: {exc}") from exc


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics() -> str:
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/api/login", response_model=TokenResponse)
async def login(creds: LoginRequest) -> TokenResponse:
    """
    Authenticate with email + password and receive a JWT Bearer token.

    Demo credentials (override via env vars):
      admin@artifex.local / admin123
      supervisor@artifex.local / supervisor123
      caseworker@artifex.local / caseworker123
    """
    user = await authenticate_user(creds.email, creds.password)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(user_id=user["user_id"], role=user["role"])
    logger.info("api.login.success", user_id=user["user_id"], role=user["role"])
    return TokenResponse(
        access_token=token,
        role=user["role"],
        user_id=user["user_id"],
    )
