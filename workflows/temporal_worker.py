"""
temporal_worker.py – Temporal workflow that wraps the LangGraph execution.

Each LangGraph node becomes a Temporal @activity.defn so that:
  • Retries are handled by Temporal (durable, survives crashes).
  • Each activity has its own timeout and retry policy.
  • The workflow state is persisted in Temporal's event history.

Run with:
  python -m workflows.temporal_worker
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from typing import Any

from dotenv import load_dotenv

load_dotenv()  # load .env before Temporal reads env vars

import logging
import structlog
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.common import RetryPolicy

# structlog is safe to use in activities and main() – they run outside the sandbox.
# Inside @workflow.defn methods we MUST use workflow.logger (sandbox-safe).
logger = structlog.get_logger()

# Standard logger for the worker process (also sandbox-safe, used in main())
_process_logger = logging.getLogger("artifex.temporal_worker")

TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "artifex-queue")
TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "default")

_retry_policy = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
)


# ── Activity NATS helper ──────────────────────────────────────────────────────

async def _nats_request(subject: str, payload: dict[str, Any], timeout: float = 60.0) -> dict[str, Any]:
    """
    Open a fresh NATS connection, make one request-reply call, then close.
    Activities are short-lived so a per-call connection is the safest pattern –
    it avoids stale singleton state across Temporal activity retries.
    """
    import json
    import nats as nats_lib

    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    nc = await nats_lib.connect(nats_url)
    try:
        data = json.dumps(payload).encode()
        msg = await nc.request(subject, data, timeout=timeout)
        return json.loads(msg.data.decode())
    finally:
        await nc.drain()


# ── Activities (one per graph node) ──────────────────────────────────────────

@activity.defn(name="planner_activity")
async def planner_activity(goal: str, workflow_id: str, retry_count: int, error: str) -> dict:
    from nats_client.subjects import Subjects

    subject = Subjects.PLANNER_REPLAN if (retry_count > 0 and error) else Subjects.PLANNER_REQUEST
    payload: dict[str, Any] = {"goal": goal, "workflow_id": workflow_id}
    if error:
        payload["error"] = error
        payload["original_goal"] = goal

    response = await _nats_request(subject, payload)
    return response.get("plan", {"tasks": []})


@activity.defn(name="retriever_activity")
async def retriever_activity(task: dict, workflow_id: str) -> dict:
    from nats_client.subjects import Subjects

    response = await _nats_request(
        Subjects.RETRIEVER_INBOX,
        {"task": task, "remaining_tasks": [], "workflow_id": workflow_id},
    )
    return response.get("result", {})


@activity.defn(name="executor_activity")
async def executor_activity(task: dict, workflow_id: str) -> dict:
    """
    Route an execution task through the dispatcher so the AgentRegistry can
    select the best available executor based on capabilities and performance.
    Falls back to EXECUTOR_INBOX if the dispatcher is not running.
    """
    from nats_client.subjects import Subjects

    # Tasks with an explicit agent field go through the dispatcher;
    # the dispatcher forwards to the correct executor inbox.
    target = "agent.executor.request" if task.get("agent") else Subjects.EXECUTOR_INBOX

    response = await _nats_request(
        target,
        {"task": task, "remaining_tasks": [], "workflow_id": workflow_id},
    )
    return response.get("result", {})


@activity.defn(name="validator_activity")
async def validator_activity(task: dict, result: dict, goal: str, workflow_id: str) -> dict:
    from nats_client.subjects import Subjects

    response = await _nats_request(
        Subjects.VALIDATOR_INBOX,
        {
            "task": task,
            "result": result,
            "remaining_tasks": [],
            "workflow_id": workflow_id,
            "original_goal": goal,
        },
    )
    return response


@activity.defn(name="validator_a_activity")
async def validator_a_activity(task: dict, result: dict, goal: str, workflow_id: str) -> dict:
    """Send validation request to validator-a (llama-3.1-8b-instant)."""
    from nats_client.subjects import Subjects

    response = await _nats_request(
        Subjects.VALIDATOR_A_INBOX,
        {
            "task": task,
            "result": result,
            "remaining_tasks": [],
            "workflow_id": workflow_id,
            "original_goal": goal,
        },
        timeout=90.0,
    )
    return response


@activity.defn(name="validator_b_activity")
async def validator_b_activity(task: dict, result: dict, goal: str, workflow_id: str) -> dict:
    """Send validation request to validator-b (gemma2-9b-it)."""
    from nats_client.subjects import Subjects

    response = await _nats_request(
        Subjects.VALIDATOR_B_INBOX,
        {
            "task": task,
            "result": result,
            "remaining_tasks": [],
            "workflow_id": workflow_id,
            "original_goal": goal,
        },
        timeout=90.0,
    )
    return response


@activity.defn(name="consensus_activity")
async def consensus_activity(
    v1: dict, v2: dict, task: dict, result: dict
) -> dict:
    """
    Combine two validator verdicts into a single consensus decision.

    Agreement rules:
      • Both valid                → consensus valid, use v1's final_answer
      • Both failed               → consensus failed, surface v1's error
      • One valid, one failed     → log disagreement; require v1 (primary) to be valid
      • Either has status "completed" → treat as valid

    Returns the same shape as a single validator response so the workflow
    can use it as a drop-in replacement.
    """
    def _is_valid(v: dict) -> bool:
        return v.get("status") in ("valid", "completed") or v.get("valid", False)

    a_valid = _is_valid(v1)
    b_valid = _is_valid(v2)

    if a_valid and b_valid:
        logger.info("consensus.both_valid",
                    task_id=task.get("id"), task_type=task.get("type"))
        return {
            "status":       v1.get("status", "completed"),
            "final_answer": v1.get("final_answer") or v2.get("final_answer"),
            "consensus":    "full",
        }

    if not a_valid and not b_valid:
        logger.warning("consensus.both_failed",
                       task_id=task.get("id"),
                       error_a=v1.get("error"), error_b=v2.get("error"))
        return {
            "status":  "failed",
            "error":   v1.get("error") or v2.get("error") or "both validators rejected result",
            "consensus": "none",
        }

    # Disagreement — log it and defer to the primary validator (v1)
    logger.warning(
        "consensus.disagreement",
        task_id=task.get("id"),
        a_valid=a_valid, b_valid=b_valid,
        a_status=v1.get("status"), b_status=v2.get("status"),
    )
    if a_valid:
        return {
            "status":       v1.get("status", "completed"),
            "final_answer": v1.get("final_answer"),
            "consensus":    "partial",
            "warning":      "validator-b disagreed",
        }
    return {
        "status":  "failed",
        "error":   v1.get("error", "primary validator rejected result"),
        "consensus": "partial",
        "warning": "validator-a rejected, validator-b accepted",
    }


@activity.defn(name="summarize_activity")
async def summarize_activity(results: list[dict], goal: str) -> str:
    """
    LLM-synthesised summary of parallel subtask results.
    Used by ParallelSubtaskWorkflow when aggregator="summary".
    """
    from langchain_groq import ChatGroq  # noqa: PLC0415
    from langchain.schema import HumanMessage, SystemMessage  # noqa: PLC0415

    llm = ChatGroq(model="llama-3.1-8b-instant", temperature=0.0)

    snippets = []
    for r in results:
        if not r.get("ok"):
            continue
        res = r.get("result", {})
        answer = (
            res.get("result", {}).get("answer")
            or res.get("answer")
            or str(res)[:300]
        )
        snippets.append(f"Subtask {r['subtask_id']}: {answer[:400]}")

    if not snippets:
        return "No valid subtask results to summarise."

    prompt = (
        f"Original goal: {goal}\n\n"
        f"Subtask results:\n" + "\n\n".join(snippets) +
        "\n\nSynthesize a single, concise, accurate answer to the original goal "
        "using all the subtask results above."
    )
    try:
        resp = await llm.ainvoke([
            SystemMessage(content="You are a synthesis expert. Combine multiple results into one clear answer."),
            HumanMessage(content=prompt),
        ])
        return resp.content.strip()
    except Exception as exc:  # noqa: BLE001
        logger.warning("summarize_activity.error", error=str(exc))
        return "\n\n".join(snippets)


@activity.defn(name="broadcast_executor_activity")
async def broadcast_executor_activity(task: dict, workflow_id: str) -> list[dict]:
    """
    Send the same task to all three model-specific executor instances in
    parallel and collect their responses. Used by agent-voting workflow.

    Returns a list of result dicts (one per executor, with model label).
    """
    import asyncio as _asyncio  # noqa: PLC0415

    subjects = [
        ("executor-llama",   "agent.executor_llama.inbox"),
        ("executor-gemma",   "agent.executor_gemma.inbox"),
        ("executor-mixtral", "agent.executor_mixtral.inbox"),
    ]

    async def _call(label: str, subject: str) -> dict:
        try:
            resp = await _nats_request(
                subject,
                {"task": task, "remaining_tasks": [], "workflow_id": workflow_id},
                timeout=45.0,
            )
            return {"model": label, "result": resp.get("result", {}), "ok": True}
        except Exception as exc:  # noqa: BLE001
            logger.warning("broadcast_executor.error", model=label, error=str(exc))
            return {"model": label, "result": {}, "ok": False, "error": str(exc)}

    results = await _asyncio.gather(*[_call(lbl, subj) for lbl, subj in subjects])
    return list(results)


@activity.defn(name="voting_validator_activity")
async def voting_validator_activity(
    task: dict, all_results: list[dict], goal: str
) -> dict:
    """
    Score each executor's result using an LLM judge and return the best one.
    Falls back to the first successful result if scoring fails.
    """
    from langchain_groq import ChatGroq  # noqa: PLC0415
    from langchain.schema import HumanMessage  # noqa: PLC0415

    llm = ChatGroq(model="gemma2-9b-it", temperature=0.0)

    valid_results = [r for r in all_results if r.get("ok")]
    if not valid_results:
        return {"status": "failed", "error": "all executor instances failed"}

    if len(valid_results) == 1:
        return {"status": "completed", "final_answer": valid_results[0]["result"],
                "votes": [{"model": valid_results[0]["model"], "score": 1.0}]}

    # Score each result
    scores: list[tuple[float, dict]] = []
    for r in valid_results:
        res = r.get("result", {})
        answer = res.get("result", {}).get("answer") or res.get("answer") or str(res)[:300]
        prompt = (
            f"Goal: {goal}\n"
            f"Model: {r['model']}\n"
            f"Answer: {answer[:400]}\n\n"
            "Rate this answer's accuracy and relevance on a scale of 0.0 to 1.0. "
            "Return JSON only: {{\"score\": <float>}}"
        )
        try:
            resp = await llm.ainvoke([HumanMessage(content=prompt)])
            import json as _j  # noqa: PLC0415
            raw = resp.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1].lstrip("json").strip()
            data = _j.loads(raw)
            score = float(data.get("score", 0.5))
        except Exception:  # noqa: BLE001
            score = 0.5
        scores.append((score, r))
        logger.info("voting_validator.scored", model=r["model"], score=score)

    # Pick the highest-scoring result
    scores.sort(key=lambda x: x[0], reverse=True)
    best_score, best = scores[0]
    return {
        "status":       "completed",
        "final_answer": best["result"],
        "votes":        [{"model": r["model"], "score": s} for s, r in scores],
        "winner":       best["model"],
        "confidence":   best_score,
    }


@activity.defn(name="spawn_specialist_activity")
async def spawn_specialist_activity(
    domain: str, query: str, workflow_id: str
) -> dict:
    """
    Spawn an ephemeral SpecialistAgent for the given domain, send it the
    query via NATS request-reply, then shut it down.

    The agent runs as an asyncio task inside the worker process – no extra
    container or Kubernetes pod required. For production, replace with a
    Kubernetes Job or a pre-warmed specialist pool.
    """
    import asyncio as _asyncio  # noqa: PLC0415
    from agents.specialist import SpecialistAgent  # noqa: PLC0415

    agent_id = f"specialist-{workflow_id}-{domain}"
    agent    = SpecialistAgent(agent_id=agent_id, domain=domain)

    # Start the agent in the background; it will subscribe and wait for one query
    agent_task = _asyncio.create_task(agent.start(), name=agent_id)

    # Give the agent a moment to connect and subscribe
    await _asyncio.sleep(1.5)

    try:
        response = await _nats_request(
            f"agent.{agent_id}.inbox",
            {"query": query, "workflow_id": workflow_id},
            timeout=60.0,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("spawn_specialist.query_error", domain=domain, error=str(exc))
        response = {"result": {"answer": f"Specialist unavailable: {exc}"}}
    finally:
        # Send shutdown signal (agent will stop after answering)
        try:
            import nats as nats_lib  # noqa: PLC0415
            nats_url = os.getenv("NATS_URL", "nats://nats:4222")
            nc = await nats_lib.connect(nats_url)
            await nc.publish(f"agent.{agent_id}.shutdown", b"{}")
            await nc.drain()
        except Exception:  # noqa: BLE001
            pass
        agent_task.cancel()

    logger.info("spawn_specialist.done", domain=domain, agent_id=agent_id)
    return response.get("result", response)

@workflow.defn(name="ArtifexSwarmWorkflow")
class ArtifexSwarmWorkflow:
    """
    Durable Temporal workflow that orchestrates the Artifex agent swarm.
    Survives crashes – Temporal replays the event history on restart.
    """

    @workflow.run
    async def run(self, goal: str) -> dict[str, Any]:
        workflow_id = workflow.info().workflow_id
        # workflow.logger is the only safe logger inside the Temporal sandbox –
        # it never creates threading.Lock objects during replay.
        workflow.logger.info(f"temporal_workflow.started goal={goal!r} workflow_id={workflow_id}")

        retry_count = 0
        max_retries = 3
        error = ""
        final_answer = None

        while retry_count <= max_retries:
            # ── Plan ──────────────────────────────────────────────────────────
            plan = await workflow.execute_activity(
                planner_activity,
                args=[goal, workflow_id, retry_count, error],
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=_retry_policy,
            )

            # ── Parallel plan branch ──────────────────────────────────────────
            if plan.get("parallel"):
                subtasks   = plan.get("subtasks", [])
                aggregator = plan.get("aggregator", "concatenate")
                workflow.logger.info(
                    f"temporal_workflow.parallel subtasks={len(subtasks)} "
                    f"aggregator={aggregator}"
                )

                # Agent-voting: broadcast the first subtask to all model-specific
                # executors and let the voting validator pick the best answer.
                if aggregator == "vote" and subtasks:
                    first_subtask = subtasks[0]
                    all_results = await workflow.execute_activity(
                        broadcast_executor_activity,
                        args=[first_subtask, workflow_id],
                        start_to_close_timeout=timedelta(seconds=120),
                        retry_policy=_retry_policy,
                    )
                    voted = await workflow.execute_activity(
                        voting_validator_activity,
                        args=[first_subtask, all_results, goal],
                        start_to_close_timeout=timedelta(seconds=60),
                        retry_policy=_retry_policy,
                    )
                    workflow.logger.info(
                        f"temporal_workflow.vote_winner "
                        f"winner={voted.get('winner')} "
                        f"confidence={voted.get('confidence', 0):.2f}"
                    )
                    final_answer = voted.get("final_answer")
                else:
                    # concatenate / summary: run subtasks as parallel child workflows
                    parallel_result = await workflow.execute_child_workflow(
                        "ParallelSubtaskWorkflow",
                        args=[subtasks, aggregator, goal],
                        id=f"parallel-{workflow_id}",
                        task_queue=TASK_QUEUE,
                        execution_timeout=timedelta(seconds=300),
                    )
                    final_answer = parallel_result.get("final_answer")
                break

            tasks: list[dict] = plan.get("tasks", [])

            if not tasks:
                break

            # ── Execute each task ─────────────────────────────────────────────
            last_result: dict = {}
            for task in tasks:
                task_type = task.get("type", "execute")

                if task_type == "retrieve":
                    last_result = await workflow.execute_activity(
                        retriever_activity,
                        args=[task, workflow_id],
                        start_to_close_timeout=timedelta(seconds=120),
                        retry_policy=_retry_policy,
                    )
                elif task_type == "spawn_specialist":
                    domain = task.get("params", {}).get("domain", "general")
                    query  = task.get("params", {}).get("query", goal)
                    last_result = await workflow.execute_activity(
                        spawn_specialist_activity,
                        args=[domain, query, workflow_id],
                        start_to_close_timeout=timedelta(seconds=90),
                        retry_policy=_retry_policy,
                    )
                else:
                    last_result = await workflow.execute_activity(
                        executor_activity,
                        args=[task, workflow_id],
                        start_to_close_timeout=timedelta(seconds=120),
                        retry_policy=_retry_policy,
                    )

                # ── Dual-validator consensus ──────────────────────────────
                # Run both validators in parallel; combine with consensus_activity.
                # Falls back gracefully if validator-a/b are not deployed
                # (Temporal will retry and eventually time out to the error path).
                v1, v2 = await asyncio.gather(
                    workflow.execute_activity(
                        validator_a_activity,
                        args=[task, last_result, goal, workflow_id],
                        start_to_close_timeout=timedelta(seconds=90),
                        retry_policy=_retry_policy,
                    ),
                    workflow.execute_activity(
                        validator_b_activity,
                        args=[task, last_result, goal, workflow_id],
                        start_to_close_timeout=timedelta(seconds=90),
                        retry_policy=_retry_policy,
                    ),
                )

                validation = await workflow.execute_activity(
                    consensus_activity,
                    args=[v1, v2, task, last_result],
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=_retry_policy,
                )

                status = validation.get("status", "failed")
                if status == "failed":
                    error = validation.get("error", "validation failed")
                    retry_count += 1
                    break  # replan
                elif status == "completed":
                    final_answer = validation.get("final_answer")
                    break
            else:
                # All tasks completed without failure
                final_answer = last_result.get("result") or last_result.get("documents")
                break

            if final_answer is not None:
                break

        workflow.logger.info(f"temporal_workflow.completed workflow_id={workflow_id}")
        return {
            "workflow_id": workflow_id,
            "goal": goal,
            "final_answer": final_answer,
            "retry_count": retry_count,
        }




@workflow.defn(name="TaskWorkerWorkflow")
class TaskWorkerWorkflow:
    """
    Lightweight child workflow that executes a single task.
    Spawned in parallel batches by ParallelSubtaskWorkflow / ArtifexSwarmWorkflow.

    Handles all task types including spawn_specialist so that parallel plans
    can mix search, direct_answer, and specialist subtasks freely.
    """

    @workflow.run
    async def run(self, task: dict) -> dict:
        workflow_id = workflow.info().workflow_id
        task_type = task.get("type", "execute")
        workflow.logger.info(f"task_worker.started task_type={task_type} workflow_id={workflow_id}")

        if task_type == "retrieve":
            result = await workflow.execute_activity(
                retriever_activity,
                args=[task, workflow_id],
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=_retry_policy,
            )
        elif task_type == "spawn_specialist":
            domain = task.get("params", {}).get("domain", "general")
            query  = task.get("params", {}).get("query", "")
            result = await workflow.execute_activity(
                spawn_specialist_activity,
                args=[domain, query, workflow_id],
                start_to_close_timeout=timedelta(seconds=90),
                retry_policy=_retry_policy,
            )
        else:
            result = await workflow.execute_activity(
                executor_activity,
                args=[task, workflow_id],
                start_to_close_timeout=timedelta(seconds=120),
                retry_policy=_retry_policy,
            )

        workflow.logger.info(f"task_worker.completed task_type={task_type} workflow_id={workflow_id}")
        return result

# ── Foster care activities ────────────────────────────────────────────────────


@activity.defn(name="load_child_profile_activity")
async def load_child_profile_activity(child_id: str) -> dict:
    """Load a child's full profile from PostgreSQL by child_id."""
    import asyncpg  # noqa: PLC0415

    db_url = _os_mod.getenv(
        "DATABASE_URL", "postgresql://artifex:artifex123@postgres:5432/placements"
    )
    try:
        conn = await asyncpg.connect(db_url, timeout=3.0)
        try:
            row = await conn.fetchrow(
                "SELECT * FROM children WHERE child_id = $1", child_id
            )
            if row is None:
                logger.warning("load_child_profile.not_found", child_id=child_id)
                return {}
            return dict(row)
        finally:
            await conn.close()
    except Exception as exc:
        logger.warning("load_child_profile.db_error", child_id=child_id, error=str(exc))
        return {}


async def _load_families_db() -> list[dict]:
    """Load all available families from PostgreSQL by family_id."""
    import asyncpg  # noqa: PLC0415

    db_url = _os_mod.getenv(
        "DATABASE_URL", "postgresql://artifex:artifex123@postgres:5432/placements"
    )
    try:
        conn = await asyncpg.connect(db_url, timeout=3.0)
        try:
            rows = await conn.fetch(
                """
                SELECT f.*,
                  (f.total_capacity - COALESCE(
                    (SELECT COUNT(*) FROM active_placements ap WHERE ap.family_id = f.family_id AND ap.status = 'active'), 0
                  )) AS available_capacity
                FROM families f
                WHERE f.active = TRUE
                  AND (f.total_capacity - COALESCE(
                    (SELECT COUNT(*) FROM active_placements ap WHERE ap.family_id = f.family_id AND ap.status = 'active'), 0
                  )) > 0
                ORDER BY f.name
                """
            )
            return [dict(r) for r in rows]
        finally:
            await conn.close()
    except Exception as exc:
        logger.warning("foster.load_families_db_error", error=str(exc))
        return []

# ── XGBoost risk model (loaded once at worker startup) ────────────────────────
import json as _json_mod
import os as _os_mod
from pathlib import Path as _Path

# Prefer /app/models (container path); fall back to local models/ for dev runs
_MODEL_PATH = _Path(_os_mod.getenv("MODEL_PATH", "/app/models/risk_model.pkl"))
_FEATURE_COLS_PATH = _Path(_os_mod.getenv("FEATURE_COLS_PATH", "/app/models/feature_columns.json"))

# Also check local relative paths when running outside Docker
if not _MODEL_PATH.exists():
    _MODEL_PATH = _Path("models/risk_model.pkl")
if not _FEATURE_COLS_PATH.exists():
    _FEATURE_COLS_PATH = _Path("models/feature_columns.json")

_risk_model = None
_feature_columns: list[str] = []

def _load_risk_model() -> None:
    """Load the XGBoost model and feature column list from disk (once)."""
    global _risk_model, _feature_columns
    try:
        import joblib
        if _MODEL_PATH.exists():
            _risk_model = joblib.load(_MODEL_PATH)
            logger.info("foster.risk_model_loaded", path=str(_MODEL_PATH))
        else:
            logger.warning("foster.risk_model_missing", path=str(_MODEL_PATH))
    except Exception as exc:  # noqa: BLE001
        logger.warning("foster.risk_model_load_error", error=str(exc))

    try:
        if _FEATURE_COLS_PATH.exists():
            with open(_FEATURE_COLS_PATH) as f:
                _feature_columns = _json_mod.load(f)
            logger.info("foster.feature_cols_loaded",
                        path=str(_FEATURE_COLS_PATH), cols=len(_feature_columns))
        else:
            logger.warning("foster.feature_cols_missing", path=str(_FEATURE_COLS_PATH))
    except Exception as exc:  # noqa: BLE001
        logger.warning("foster.feature_cols_load_error", error=str(exc))

_load_risk_model()


def _build_feature_row(child: dict) -> dict:
    """
    Build a feature dict aligned to the columns in feature_columns.json.

    Expected columns (from training):
      age, siblings, special_needs,
      reason_Educational Neglect, reason_Medical Neglect, reason_Neglect,
      reason_Other, reason_Physical Abuse, reason_Psychological Abuse,
      reason_Sex Trafficking, reason_Sexual Abuse

    All columns default to 0; only the matching one-hot column is set to 1.
    """
    removal_reason = child.get("removal_reason", "Other")

    # Start with all expected columns zeroed out
    row: dict = {col: 0 for col in _feature_columns}

    # Fill numeric features
    row["age"]           = child.get("age", 10)
    row["siblings"]      = child.get("siblings", 0)
    row["special_needs"] = int(bool(child.get("special_needs", False)))

    # Set the matching one-hot column; fall back to reason_Other if unknown
    one_hot_col = f"reason_{removal_reason}"
    if one_hot_col in row:
        row[one_hot_col] = 1
    elif "reason_Other" in row:
        row["reason_Other"] = 1

    return row


@activity.defn(name="placement_predict_activity")
async def placement_predict_activity(child: dict, workflow_id: str) -> dict:
    """
    Full ML placement prediction using the placement_recommender engine.

    Delegates to services.placement_recommender.recommend_foster_family(),
    which loads available families from PostgreSQL, generates pair features,
    runs ML inference, ranks families, and returns top-N matches.

    IMPORTANT: No heuristic fallback recommendations are allowed. If the
    model is unavailable or no recommendation can be produced, this activity
    raises so the workflow can emit an explicit manual-review status.
    """
    from services.placement_recommender import recommend_foster_family  # noqa: PLC0415

    api_url = os.getenv("API_URL", "http://api:8000")

    async def _log_inference(payload: dict, result: dict, model_ver: str | None) -> None:
        """Fire-and-forget POST to log ML inference to ml_inference_logs."""
        try:
            import httpx as _httpx  # noqa: PLC0415
            async with _httpx.AsyncClient(timeout=3.0) as _client:
                await _client.post(
                    f"{api_url}/foster/internal/ml_inference_log",
                    json={
                        "workflow_id": workflow_id,
                        "child_id": child.get("child_id"),
                        "payload": payload,
                        "result": result,
                        "model_version": model_ver,
                    },
                )
        except Exception:
            pass

    result = await recommend_foster_family(child, top_n=5)
    if not result or not result.get("recommended_family"):
        raise RuntimeError("No recommendation could be produced (no families or model failure).")

    logger.info(
        "placement_predict_activity.recommendation",
        workflow_id=workflow_id,
        child_id=child.get("child_id"),
        family_id=result["recommended_family"].get("family_id"),
        match_score=result.get("match_score"),
        confidence=result.get("confidence_score"),
        risk_score=result.get("risk_score"),
        top_matches=len(result.get("top_matches", [])),
    )
    ret = {
        "recommended_family": result["recommended_family"],
        "match_score": result.get("match_score", 0),
        "confidence": result.get("confidence_score", 0),
        "risk_score": result.get("risk_score", 0),
        "feature_importance": result.get("feature_importance", []),
        "top_matches": result.get("top_matches", []),
        "explanation": result.get("explanation", ""),
        "model_version": result.get("model_version", "xgboost-v1"),
    }
    await _log_inference(child, ret, ret.get("model_version"))

    # Publish live event
    try:
        import json as _json  # noqa: PLC0415
        import nats as nats_lib  # noqa: PLC0415
        _nc = await nats_lib.connect(os.getenv("NATS_URL", "nats://localhost:4222"))
        await _nc.publish(
            "events.live.ml_completed",
            _json.dumps({
                "event": "ml_completed",
                "child_id": child.get("child_id"),
                "workflow_id": workflow_id,
                "recommended_family": ret.get("recommended_family", {}).get("name"),
                "match_score": ret.get("match_score"),
                "risk_score": ret.get("risk_score"),
                "confidence": ret.get("confidence"),
                "model_version": ret.get("model_version"),
                "timestamp": __import__("datetime").datetime.now().isoformat(),
            }).encode(),
        )
        await _nc.drain()
    except Exception:  # noqa: BLE001
        pass
    return ret




@activity.defn(name="record_workflow_event_activity")
async def record_workflow_event_activity(
    workflow_id: str, stage: str, status: str, data: dict | None = None
) -> None:
    """
    Persist a workflow event to the API's HTTP endpoint, which writes to
    the workflow_events and workflow_status tables.
    """
    import httpx  # noqa: PLC0415

    # Use API_URL env var; in Docker this should be http://api:8000
    api_url = os.getenv("API_URL", "http://api:8000")
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(
                f"{api_url}/foster/internal/workflow_event",
                json={
                    "workflow_id": workflow_id,
                    "stage": stage,
                    "status": status,
                    "data": data or {},
                },
            )
    except Exception:  # noqa: BLE001
        # Fallback: try to publish via NATS
        try:
            import json as _json  # noqa: PLC0415
            import nats as nats_lib  # noqa: PLC0415

            nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
            nc = await nats_lib.connect(nats_url)
            await nc.publish(
                "foster.workflow_events",
                _json.dumps({
                    "workflow_id": workflow_id,
                    "stage": stage,
                    "status": status,
                    "data": data or {},
                }).encode(),
            )
            await nc.drain()
        except Exception:  # noqa: BLE001
            logger.warning(
                "record_workflow_event.failed",
                workflow_id=workflow_id,
                stage=stage,
            )


@activity.defn(name="publish_match_activity")
async def publish_match_activity(placement: dict) -> None:
    """
    Publish a placement update to the NATS subject foster.placements.
    The API subscribes to this subject and updates its in-memory store,
    decoupling the worker from the API over a reliable message bus.
    """
    import json as _json
    import nats as nats_lib

    child_id = placement.get("child_id", "unknown")
    # Use same NATS URL as the API for local dev compatibility
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")

    try:
        nc = await nats_lib.connect(nats_url)
        try:
            await nc.publish(
                "foster.placements",
                _json.dumps(placement).encode(),
            )
            logger.info("foster.publish_match_nats", child_id=child_id)
        finally:
            await nc.drain()
    except Exception as exc:  # noqa: BLE001
        logger.error("foster.publish_match_nats_error",
                     child_id=child_id, error=str(exc))


async def _fetch_checkin_trends(child_id: str) -> dict:
    """
    Query check_ins for this child and compute trend features.

    Returns:
        mood_trend:      slope of mood scores over last 5 check-ins (−2..+2)
        incident_rate:   fraction of recent check-ins with incidents (0..1)
        mood_volatility: std dev of mood scores, normalized (0..1)
        checkin_count:   total check-ins found
    """
    import asyncpg  # noqa: PLC0415

    db_url = _os_mod.getenv(
        "DATABASE_URL", "postgresql://artifex:artifex123@postgres:5432/placements"
    )
    try:
        conn = await asyncpg.connect(db_url, timeout=3.0)
        try:
            rows = await conn.fetch(
                """
                SELECT mood_score, incident_reported, timestamp
                FROM check_ins
                WHERE child_id = $1
                ORDER BY timestamp DESC
                LIMIT 10
                """,
                child_id,
            )
            if not rows or len(rows) < 2:
                return {"mood_trend": 0.0, "incident_rate": 0.0, "mood_volatility": 0.0, "checkin_count": len(rows) if rows else 0}

            scores = [r["mood_score"] for r in rows]
            incidents = [r["incident_reported"] for r in rows]
            n = len(scores)

            # Mood trend: slope of scores over check-in index (last 5)
            recent = scores[: min(5, n)]
            if len(recent) >= 2:
                xs = list(range(len(recent)))
                mean_x = sum(xs) / len(xs)
                mean_y = sum(recent) / len(recent)
                num = sum((xs[i] - mean_x) * (recent[i] - mean_y) for i in range(len(recent)))
                den = sum((x - mean_x) ** 2 for x in xs)
                slope = num / den if den != 0 else 0.0
                # Normalise slope to −2..+2 range (each check-in step, max change 4)
                mood_trend = max(-2.0, min(2.0, slope))
            else:
                mood_trend = 0.0

            # Incident rate (last 5 check-ins)
            incident_rate = sum(incidents[:5]) / min(5, n) if n > 0 else 0.0

            # Mood volatility (normalised std dev of last 5 scores)
            recent_scores = scores[: min(5, n)]
            if len(recent_scores) >= 3:
                mean_s = sum(recent_scores) / len(recent_scores)
                variance = sum((s - mean_s) ** 2 for s in recent_scores) / len(recent_scores)
                # std dev ∼0..2 for 1–5 scale, normalise to 0..1
                mood_volatility = min(1.0, (variance ** 0.5) / 2.0)
            else:
                mood_volatility = 0.0

            return {
                "mood_trend": mood_trend,
                "incident_rate": incident_rate,
                "mood_volatility": mood_volatility,
                "checkin_count": n,
            }
        finally:
            await conn.close()
    except Exception as exc:
        logger.warning("foster.checkin_trend_query_error", child_id=child_id, error=str(exc))
        return {"mood_trend": 0.0, "incident_rate": 0.0, "mood_volatility": 0.0, "checkin_count": 0}


@activity.defn(name="compute_risk_activity")
async def compute_risk_activity(
    child: dict, family: dict, score: int, notes: str, previous_risk: float = 0.0
) -> dict:
    """
    XGBoost-based disruption risk model with check-in trend awareness.

    1. Queries check_ins history for the child (mood trend, incidents, volatility).
    2. Computes a base disruption probability from child attributes via XGBoost.
    3. Blends the check-in score into the base.
    4. Computes a trend risk from check-in patterns.
    5. Smooths with previous risk.

    Formula: new_risk = previous_risk * 0.5 + base_risk * 0.3 + trend_risk * 0.2

    Falls back to the rule-based heuristic if the model is unavailable.
    Returns {"risk": float, "explanation": str}.
    """
    import pandas as pd

    child_id = child.get("child_id", "")
    base_risk: float

    # ── Query check-in trends ─────────────────────────────────────────────────
    trends = await _fetch_checkin_trends(child_id)

    # ── XGBoost / rule-based base risk ────────────────────────────────────────
    if _risk_model is not None and _feature_columns:
        row = _build_feature_row(child)
        features_df = pd.DataFrame([row], columns=_feature_columns)
        disruption_prob = float(_risk_model.predict_proba(features_df)[0][1])

        if score:
            score_adjustment = (5 - score) * 0.075
            disruption_prob = min(max(disruption_prob + score_adjustment, 0.0), 1.0)

        base_risk = disruption_prob * 100.0
        model_tag = "xgboost"
    else:
        base_risk = (5 - score) * 15.0
        notes_lower = notes.lower()
        if any(w in notes_lower for w in ("nightmare", "acting out", "aggressive", "refusing")):
            base_risk += 20
        if any(w in notes_lower for w in ("runaway", "self-harm", "crisis", "emergency")):
            base_risk += 30
        if "school" in notes_lower and any(w in notes_lower for w in ("expelled", "suspended", "absent")):
            base_risk += 10
        if any(w in notes_lower for w in ("happy", "settling", "thriving", "bonding", "improving")):
            base_risk -= 15
        if any(w in notes_lower for w in ("school", "friends", "activities")):
            base_risk -= 5
        if child.get("special_needs") and not family.get("special_needs_trained"):
            base_risk += 10
        base_risk = min(max(base_risk, 0.0), 100.0)
        model_tag = "rule-based"

    # ── Trend risk from check-in history ──────────────────────────────────────
    trend_risk = 50.0  # neutral starting point
    if trends["checkin_count"] >= 2:
        # Declining mood increases risk
        trend_risk -= trends["mood_trend"] * 8.0  # each point of downward slope → +8 risk
        # Incidents add risk
        trend_risk += trends["incident_rate"] * 25.0
        # High volatility increases risk
        trend_risk += trends["mood_volatility"] * 15.0
        trend_risk = min(max(trend_risk, 0.0), 100.0)

    # ── Blend with temporal smoothing ─────────────────────────────────────────
    new_risk = previous_risk * 0.5 + base_risk * 0.3 + trend_risk * 0.2
    new_risk = min(max(new_risk, 0.0), 100.0)

    explanation_parts = [
        f"[{model_tag}] Score {score}/5 → base {base_risk:.0f}.",
        f"Trend: mood_slope={trends['mood_trend']:+.2f} incidents={trends['incident_rate']:.0%} "
        f"volatility={trends['mood_volatility']:.2f} → trend_risk {trend_risk:.0f}.",
        f"Previous {previous_risk:.0f} → new {new_risk:.0f}.",
    ]
    if trends["checkin_count"] == 0:
        explanation_parts.insert(1, "No check-in history yet.")

    explanation = " ".join(explanation_parts)

    # Publish live event if risk increased significantly
    if new_risk > previous_risk + 10 and child_id:
        try:
            import json as _json  # noqa: PLC0415
            import nats as nats_lib  # noqa: PLC0415
            _nc = await nats_lib.connect(os.getenv("NATS_URL", "nats://localhost:4222"))
            await _nc.publish(
                "events.live.risk_increased",
                _json.dumps({
                    "event": "risk_increased",
                    "child_id": child_id,
                    "previous_risk": round(previous_risk, 1),
                    "new_risk": round(new_risk, 1),
                    "delta": round(new_risk - previous_risk, 1),
                    "trend": trends,
                    "explanation": explanation[:200],
                    "timestamp": __import__("datetime").datetime.now().isoformat(),
                }).encode(),
            )
            await _nc.drain()
        except Exception:  # noqa: BLE001
            pass

    logger.info(
        "foster.compute_risk",
        child_id=child_id,
        score=score,
        risk=new_risk,
        model=model_tag,
        trend=trends,
        explanation=explanation,
    )

    # ── ML decision audit trail ────────────────────────────────────────────────
    try:
        import asyncpg as _apg
        _audit_url = _os_mod.getenv(
            "DATABASE_URL", "postgresql://artifex:artifex123@postgres:5432/placements"
        )
        _audit_conn = await _apg.connect(_audit_url, timeout=3.0)
        try:
            risk_label = (
                "critical" if new_risk > 80
                else "high" if new_risk > 60
                else "medium" if new_risk > 30
                else "low"
            )
            await _audit_conn.execute(
                """
                INSERT INTO ml_decision_audit
                    (child_id, placement_id, caseworker_id, decision_type,
                     model_name, model_version, input_features,
                     child_demographics, output_score, output_label,
                     output_details)
                VALUES ($1, $2, $3, $4, $5, $6,
                        $7::jsonb, $8::jsonb, $9, $10, $11::jsonb)
                """,
                child_id,
                family.get("family_id"),
                None,
                "risk_score",
                "risk_model",
                model_tag,
                _json_mod.dumps({
                    "age": child.get("age"),
                    "special_needs": int(bool(child.get("special_needs", False))),
                    "siblings": child.get("siblings", 0),
                    "score": score,
                    "mood_trend": trends.get("mood_trend"),
                    "incident_rate": trends.get("incident_rate"),
                    "mood_volatility": trends.get("mood_volatility"),
                    "checkin_count": trends.get("checkin_count"),
                    "previous_risk": previous_risk,
                }),
                _json_mod.dumps({
                    "age": child.get("age"),
                    "gender": child.get("gender"),
                    "race": child.get("race"),
                    "fpl_percent": child.get("fpl_percent"),
                    "zip_code": child.get("zip_code"),
                    "special_needs": bool(child.get("special_needs", False)),
                    "sibling_group": bool(child.get("sibling_group", False)),
                    "emergency_level": child.get("emergency_level", "normal"),
                }),
                new_risk,
                risk_label,
                _json_mod.dumps({
                    "base_risk": round(base_risk, 1),
                    "trend_risk": round(trend_risk, 1),
                    "previous_risk": previous_risk,
                    "explanation": explanation[:500],
                }),
            )
        finally:
            await _audit_conn.close()
    except Exception:  # noqa: BLE001
        logger.warning("compute_risk_activity.audit_error", child_id=child_id)

    return {"risk": new_risk, "explanation": explanation}


@activity.defn(name="send_alert_activity")
async def send_alert_activity(
    family: dict, risk: float, notes: str, child_id: str, child: dict | None = None
) -> None:
    """
    Send a high-risk disruption alert — validated by a consortium of two LLMs
    before firing. Both models must independently agree (average confidence ≥
    CONSORTIUM_THRESHOLD) before the alert reaches caseworkers.

    Args:
        family:   matched foster family dict
        risk:     ML-computed risk score (0–100)
        notes:    caseworker check-in notes
        child_id: child identifier string
        child:    full child profile dict (used for richer LLM context)
    """
    import json as _json
    import os as _os
    from tools.consortium_validator import ConsortiumValidator

    family_id   = family.get("family_id", "unknown")
    family_name = family.get("name", family_id)
    child_ctx   = child or {"child_id": child_id}

    # ── Consortium validation ─────────────────────────────────────────────────
    validator = ConsortiumValidator()   # reads CONSORTIUM_MODELS / CONSORTIUM_THRESHOLD from env

    try:
        validation = await validator.validate(child_ctx, family, risk, notes)
        conf    = validation["confidence"]
        passed  = validation["valid"]
        details = validation["details"]

        logger.info(
            "foster.consortium_validation",
            child_id=child_id,
            passed=passed,
            confidence=conf,
            votes=[{"model": d["model"], "valid": d["valid"]} for d in details],
        )

        if not passed:
            logger.info(
                "alert.rejected_by_consortium",
                child_id=child_id,
                confidence=conf,
                threshold=validator.threshold,
                reasons=[d["reason"] for d in details],
            )
            return

    except Exception as exc:  # noqa: BLE001
        # Consortium failure → fail open (send alert) so no real crisis is missed
        logger.warning("foster.consortium_error", child_id=child_id, error=str(exc))
        conf    = 0.0
        details = []

    # ── Send alert ────────────────────────────────────────────────────────────
    print(
        f"\n🔴 HIGH DISRUPTION RISK ALERT  (consortium confidence: {conf:.0%})\n"
        f"   Child ID  : {child_id}\n"
        f"   Family    : {family_name} ({family_id})\n"
        f"   Risk Score: {risk:.0f}%\n"
        f"   Notes     : {notes}\n"
        f"   Validation: {[{'model': d['model'], 'valid': d['valid'], 'reason': d['reason']} for d in details]}\n"
        f"   Action    : Immediate caseworker review required.\n",
        flush=True,
    )
    logger.warning(
        "foster.high_risk_alert",
        child_id=child_id,
        family_id=family_id,
        risk=risk,
        consortium_confidence=conf,
    )

    import nats as nats_lib  # noqa: PLC0415
    nats_url = _os.getenv("NATS_URL", "nats://nats:4222")

    # Publish case_escalated live event
    try:
        _nc = await nats_lib.connect(nats_url)
        await _nc.publish(
            "events.live.case_escalated",
            _json.dumps({
                "event": "case_escalated",
                "child_id": child_id,
                "family_id": family_id,
                "family_name": family_name,
                "risk_score": risk,
                "notes": notes[:200],
                "consortium_confidence": conf,
                "timestamp": __import__("datetime").datetime.now().isoformat(),
            }).encode(),
        )
        await _nc.drain()
    except Exception:  # noqa: BLE001
        pass

    try:
        nc = await nats_lib.connect(nats_url)
        await nc.publish(
            "foster.alerts",
            _json.dumps({
                "child_id":             child_id,
                "family_id":            family_id,
                "risk":                 risk,
                "notes":                notes,
                "consortium_confidence": conf,
            }).encode(),
        )
        await nc.drain()
    except Exception as exc:  # noqa: BLE001
        logger.warning("foster.alert_nats_error", error=str(exc))


# ── Worker entrypoint ─────────────────────────────────────────────────────────

async def main() -> None:
    """Connect to Temporal with retries, then start the worker."""

    max_attempts = 10
    for attempt in range(1, max_attempts + 1):
        try:
            client = await Client.connect(TEMPORAL_HOST, namespace=TEMPORAL_NAMESPACE)
            break
        except Exception as exc:  # noqa: BLE001
            if attempt == max_attempts:
                raise
            wait = min(5 * attempt, 30)
            logger.warning(
                "temporal.connect_retry",
                attempt=attempt,
                wait_seconds=wait,
                error=str(exc),
            )
            await asyncio.sleep(wait)

    # Import here (not at module level) so Temporal's sandbox validator
    # doesn't try to replay foster_workflow.py imports during ArtifexSwarmWorkflow replay.
    from workflows.foster_workflow import FosterPlacementWorkflow  # noqa: PLC0415
    from workflows.parallel_workflow import ParallelSubtaskWorkflow  # noqa: PLC0415
    from workflows.fairness_workflow import (  # noqa: PLC0415
        WeeklyFairnessWorkflow,
        compute_fairness_metrics_activity,
    )
    from workflows.emergent_workflow import (  # noqa: PLC0415
        EmergentSwarmWorkflow,
        announce_task_activity,
        wait_for_team_activity,
        wait_for_result_activity,
    )

    from temporalio.worker import UnsandboxedWorkflowRunner

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            ArtifexSwarmWorkflow,
            FosterPlacementWorkflow,
            TaskWorkerWorkflow,
            ParallelSubtaskWorkflow,
            EmergentSwarmWorkflow,
            WeeklyFairnessWorkflow,
        ],
        activities=[
            planner_activity,
            retriever_activity,
            executor_activity,
            validator_activity,
            validator_a_activity,
            validator_b_activity,
            consensus_activity,
            summarize_activity,
            broadcast_executor_activity,
            voting_validator_activity,
            spawn_specialist_activity,
            placement_predict_activity,
            publish_match_activity,
            record_workflow_event_activity,
            compute_risk_activity,
            send_alert_activity,
            compute_fairness_metrics_activity,
            announce_task_activity,
            wait_for_team_activity,
            wait_for_result_activity,
        ],
        workflow_runner=UnsandboxedWorkflowRunner(),
    )

    logger.info("temporal_worker.starting", task_queue=TASK_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
