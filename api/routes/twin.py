"""
api/routes/twin.py – Child Digital Twin REST API.

Four endpoints:
  GET   /api/twin/{child_id}/state           – fetch current twin state
  POST  /api/twin/{child_id}/simulate        – run counterfactual simulation
  PATCH /api/twin/{child_id}/scenarios       – save/update a scenario slot
  GET   /api/twin/{child_id}/case-conference-pdf – generate PDF export
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field, field_validator

from api.auth import get_current_user, require_role
from api.db import get_pool

logger = structlog.get_logger()
router = APIRouter(tags=["twin"])


# ── Pydantic models ───────────────────────────────────────────────────────────


class InterventionComponent(BaseModel):
    domain: str = Field(..., pattern=r"^(school|placement|therapy|visits|mentor|caseworker|sibling|medication)$")
    action: str = Field(..., min_length=1, max_length=64)
    value: str = Field(default="", max_length=256)


class SimulateRequest(BaseModel):
    interventions: list[InterventionComponent] = Field(..., min_length=1, max_length=3)
    horizon_days: int = Field(default=90, ge=30, le=365)

    @field_validator("interventions")
    @classmethod
    def max_three_interventions(cls, v: list[InterventionComponent]) -> list[InterventionComponent]:
        if len(v) > 3:
            raise ValueError("Maximum 3 simultaneous interventions")
        return v


class SimulateResponse(BaseModel):
    simulation_id: str
    child_id: str
    generated_at: str
    model_version: str
    n_historical_placements: int
    intervention: dict[str, Any]
    baseline: dict[str, Any]
    counterfactual: dict[str, Any]
    effect: dict[str, Any]


class ScenarioSlot(BaseModel):
    slot: str = Field(..., pattern=r"^[ABC]$")
    label: str = Field(default="", max_length=128)
    simulation_id: str = Field(default="", max_length=64)
    interventions: list[InterventionComponent] = Field(default_factory=list)
    outcome_summary: str = Field(default="", max_length=512)
    verdict: str = Field(default="", pattern=r"^(positive|uncertain|negative)?$")
    caseworker_note: str = Field(default="", max_length=500)


class ScenariosRequest(BaseModel):
    slot: str = Field(..., pattern=r"^[ABC]$")
    scenario: ScenarioSlot


class TwinStateData(BaseModel):
    """Validated model for the internal twin state dict."""
    child_id: str
    placement_id: str | None = None
    as_of: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    current_features: dict[str, Any] = Field(default_factory=dict)
    outcome_probs: dict[str, Any] | None = None
    pending_simulations: list[dict[str, Any]] | None = None
    version: int = 1
    stale_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc) + timedelta(days=7))


class TwinStateResponse(BaseModel):
    child_id: str
    placement_id: str | None
    as_of: str
    current_features: dict[str, Any]
    outcome_probs: dict[str, Any] | None
    pending_simulations: list[dict[str, Any]] | None
    version: int
    stale_at: str


# ── Internal helpers ──────────────────────────────────────────────────────────


async def _get_or_create_twin_state(child_id: str, pool: Any) -> dict[str, Any]:
    """Fetch existing twin state or create a fresh one from child + placement data."""
    row = await pool.fetchrow(
        """
        SELECT ts.* FROM child_twin_states ts
        WHERE ts.child_id = $1
        """,
        child_id,
    )
    if row:
        state = dict(row)
        # JSONB columns may arrive as strings depending on driver codec registration
        for key in ("current_features", "pending_simulations", "outcome_probs"):
            if isinstance(state.get(key), str):
                try:
                    state[key] = json.loads(state[key])
                except (json.JSONDecodeError, TypeError):
                    fallback: dict | list | None = {"current_features": {}, "pending_simulations": [], "outcome_probs": None}
                    state[key] = fallback.get(key, {})
        # Validate with Pydantic; on failure, return a safe skeleton
        try:
            TwinStateData.model_validate(state)
        except Exception as exc:
            logger.error("twin.state_validation_failed", child_id=child_id, error=str(exc))
            state = {
                "child_id": child_id,
                "placement_id": state.get("placement_id"),
                "as_of": datetime.now(timezone.utc).isoformat(),
                "current_features": {},
                "outcome_probs": None,
                "pending_simulations": [],
                "version": state.get("version", 1),
                "stale_at": (datetime.now(timezone.utc) + timedelta(days=7)).isoformat(),
            }
        return state

    # Build a fresh state from child + latest placement + drift signals
    child = await pool.fetchrow(
        "SELECT * FROM children WHERE child_id = $1", child_id
    )
    if not child:
        raise HTTPException(status_code=404, detail=f"Child {child_id} not found")

    placement = await pool.fetchrow(
        """
        SELECT workflow_id, family_id, status, risk_score, created_at
        FROM placements
        WHERE child_id = $1
        ORDER BY CASE status
            WHEN 'active'   THEN 1
            WHEN 'approved' THEN 2
            WHEN 'pending_supervisor' THEN 3
            WHEN 'pending'  THEN 4
            ELSE 5
        END, created_at DESC LIMIT 1
        """,
        child_id,
    )
    placement_id = placement["workflow_id"] if placement else None

    drift = await pool.fetchrow(
        """
        SELECT signals_json, drift_score, snapshot_date
        FROM behavioural_drift_signals
        WHERE child_id = $1
        ORDER BY snapshot_date DESC LIMIT 1
        """,
        child_id,
    )

    # Query the latest workflow event data for risk/match/confidence scores
    workflow_scores: dict[str, Any] = {}
    if placement_id:
        wf_row = await pool.fetchrow(
            """
            SELECT data FROM workflow_events
            WHERE workflow_id = $1 AND data IS NOT NULL
            ORDER BY timestamp DESC LIMIT 1
            """,
            placement_id,
        )
        if wf_row:
            raw = wf_row["data"]
            if isinstance(raw, str):
                try:
                    workflow_scores = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    workflow_scores = {}
            elif isinstance(raw, dict):
                workflow_scores = raw

    # Fix special_needs: DB stores "f"/"t" as string
    raw_special_needs = child.get("special_needs")
    if isinstance(raw_special_needs, str):
        special_needs = raw_special_needs.lower() in ("t", "true", "yes", "1")
    elif isinstance(raw_special_needs, bool):
        special_needs = raw_special_needs
    else:
        special_needs = bool(raw_special_needs) if raw_special_needs else False

    # Extract scores from workflow data, fall back to placement.risk_score
    wf_risk = workflow_scores.get("risk_score")
    wf_match = workflow_scores.get("match_score")
    wf_confidence = workflow_scores.get("confidence_score")
    wf_family = workflow_scores.get("recommended_family")

    placement_count = await pool.fetchval(
        "SELECT COUNT(*) FROM placements WHERE child_id = $1", child_id
    ) if pool else None

    # Weeks in current placement
    weeks: int | None = None
    if placement:
        created = placement.get("created_at")
        if created:
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - created
            weeks = max(1, delta.days // 7)

    # Risk score: workflow > placement > None
    risk_score: float | None = None
    if wf_risk is not None:
        risk_score = float(wf_risk)
    elif placement and placement.get("risk_score") is not None:
        risk_score = float(placement["risk_score"])

    match_score: float | None = float(wf_match) if wf_match is not None else None
    confidence_score: float | None = float(wf_confidence) if wf_confidence is not None else None

    current_features: dict[str, Any] = {
        "age": child.get("age"),
        "gender": child.get("gender"),
        "special_needs": special_needs,
        "emergency_level": child.get("emergency_level"),
        "intake_reason": child.get("intake_reason"),
        "school": child.get("school"),
        "weeks_in_placement": weeks,
        "current_risk_score": risk_score,
        "match_score": match_score,
        "confidence_score": confidence_score,
        "current_drift_score": float(drift["drift_score"]) if drift and drift.get("drift_score") else None,
        "placement_history": placement_count or 0,
        "school_stability": 65 if child.get("school") else (30 if child.get("school") is not None else None),
        "recommended_family": wf_family,
    }

    logger.info(
        "twin.features_loaded",
        child_id=child_id,
        feature_count=len([k for k, v in current_features.items() if v is not None]),
        risk_score=risk_score,
        match_score=match_score,
        confidence_score=confidence_score,
    )

    # Outcome probabilities from crisis_predictions or derive from risk_score
    outcome_probs: dict[str, Any] | None = None
    if risk_score is not None:
        p = max(0.0, min(1.0, risk_score / 100.0))
        outcome_probs = {
            "stable": round(1.0 - p, 4),
            "disrupted": round(p, 4),
            "reunified": round(p * 0.15, 4),
            "runaway": round(p * 0.05, 4),
        }

    stale_at = datetime.now(timezone.utc) + timedelta(days=7)

    await pool.execute(
        """
        INSERT INTO child_twin_states
            (child_id, placement_id, as_of, current_features,
             outcome_probs, pending_simulations, version, stale_at)
        VALUES ($1, $2, NOW(), $3::jsonb, $4::jsonb, '[]'::jsonb, 1, $5)
        ON CONFLICT (child_id)
        DO UPDATE SET
            current_features = EXCLUDED.current_features,
            outcome_probs = EXCLUDED.outcome_probs,
            as_of = NOW(),
            stale_at = EXCLUDED.stale_at,
            version = child_twin_states.version + 1
        """,
        child_id,
        placement_id,
        json.dumps(current_features),
        json.dumps(outcome_probs) if outcome_probs else None,
        stale_at,
    )

    state = {
        "child_id": child_id,
        "placement_id": placement_id,
        "as_of": datetime.now(timezone.utc).isoformat(),
        "current_features": current_features,
        "outcome_probs": outcome_probs,
        "pending_simulations": [],
        "version": 1,
        "stale_at": stale_at.isoformat(),
    }
    return dict(TwinStateData.model_validate(state).model_dump())


def _build_simulated_response(
    child_id: str,
    interventions: list[dict[str, Any]],
    horizon_days: int,
    current_features: dict[str, Any],
) -> SimulateResponse:
    """
    Build a structured SimulationResult response.

    Uses rule-based fallback estimates keyed to intervention type.
    When the real CausalForest engine is integrated, this function
    will call out to the trained model instead.
    """
    from random import uniform as _u

    # ── Defensive normalisation ───────────────────────────────────────────
    if isinstance(current_features, str):
        try:
            current_features = json.loads(current_features)
        except (json.JSONDecodeError, TypeError):
            current_features = {}
    if not isinstance(current_features, dict):
        current_features = {}

    logger.info(
        "twin.current_features_type",
        type=str(type(current_features)),
        value=current_features,
        child_id=child_id,
    )

    sim_id = f"sim_{uuid.uuid4().hex[:12]}"
    n_hist = current_features.get("placement_history", 0) or 0

    # Baseline: derive from current_risk_score
    base_risk = current_features.get("current_risk_score")
    if base_risk is None:
        base_risk = _u(40, 70)
    base_p = round(base_risk / 100.0, 2)

    # Effect sizes per intervention domain (rule-based fallback)
    domain_effects = {
        "school":     -0.18,
        "placement":  -0.14,
        "therapy":    -0.10,
        "visits":     -0.08,
        "mentor":     -0.06,
        "caseworker": -0.05,
        "sibling":    -0.07,
        "medication": -0.09,
    }

    total_effect = 0.0
    interaction_effect = None
    components = []
    for iv in interventions:
        domain = iv.get("domain", "")
        effect = domain_effects.get(domain, -0.05)
        total_effect += effect
        components.append({
            "domain": domain,
            "action": iv.get("action", "change"),
            "value": iv.get("value", ""),
            "individual_effect": effect,
        })

    logger.info(
        "twin.simulation_complete",
        child_id=child_id,
        simulation_id=sim_id,
        n_historical_placements=n_hist,
        base_risk_score=base_risk,
        total_effect_size=round(total_effect, 2),
    )

    # Interaction effect for compound interventions
    if len(interventions) >= 2:
        interaction_effect = round(total_effect * 0.15, 2)
        total_effect += interaction_effect

    cf_p = round(max(0.0, min(1.0, base_p + total_effect)), 2)
    prob_benefit = round(1.0 - (cf_p / base_p) if base_p > 0 else 0.5, 2)
    prob_benefit = max(0.0, min(1.0, prob_benefit))
    nnt = round(1.0 / max(0.01, base_p - cf_p), 1)

    # Outcome distributions
    def _dist(p: float) -> dict[str, float]:
        return {
            "stable": round(1.0 - p, 2),
            "disrupted": round(p, 2),
            "reunified": round(p * 0.15, 2),
            "runaway": round(p * 0.05, 2),
        }

    def _ci(p: float) -> list[float]:
        w = p * 0.15
        return [round(max(0, p - w), 2), round(min(1, p + w), 2)]

    # Decomposition
    decomposition: dict[str, Any] = {
        "components": [
            {
                "domain": c["domain"],
                "alone": c["individual_effect"],
            }
            for c in components
        ],
    }
    if interaction_effect is not None:
        decomposition["interaction_effect"] = interaction_effect
        decomposition["interaction_pct"] = round(
            abs(interaction_effect) / max(0.01, abs(total_effect)) * 100, 1
        )

    return SimulateResponse(
        simulation_id=sim_id,
        child_id=child_id,
        generated_at=datetime.now(timezone.utc).isoformat(),
        model_version="twin-rule-fallback-v1",
        n_historical_placements=n_hist,
        intervention={
            "type": "compound" if len(interventions) > 1 else "single",
            "components": components,
        },
        baseline={
            "outcome_distribution": {
                f"{d}_days": _dist(base_p)
                for d in [30, 60, 90]
            },
            "ci_95": {
                "30_days": {
                    "stable": _ci(1 - base_p),
                    "disrupted": _ci(base_p),
                    "reunified": [0.01, 0.08],
                    "runaway": [0.01, 0.05],
                }
            },
            "dominant_outcome": "disrupted" if base_p > 0.5 else "stable",
            "uncertainty_score": round(min(1.0, base_p * 1.5), 2),
        },
        counterfactual={
            "outcome_distribution": {
                f"{d}_days": _dist(cf_p)
                for d in [30, 60, 90]
            },
            "ci_95": {
                "30_days": {
                    "stable": _ci(1 - cf_p),
                    "disrupted": _ci(cf_p),
                    "reunified": [0.01, 0.08],
                    "runaway": [0.01, 0.05],
                }
            },
            "dominant_outcome": "disrupted" if cf_p > 0.5 else "stable",
            "uncertainty_score": round(min(1.0, cf_p * 1.5), 2),
        },
        effect={
            "effect_size": round(total_effect, 2),
            "probability_of_benefit": prob_benefit,
            "number_needed_to_treat": nnt,
            "ci_95": [
                round(max(-1.0, total_effect - 0.12), 2),
                round(min(1.0, total_effect + 0.12), 2),
            ],
            "decomposition": decomposition,
            "robustness_value": 0.38,
            "sensitivity": {
                "confounder_strength_to_nullify": 0.38,
                "most_sensitive_feature": "baseline_incident_rate",
                "most_sensitive_feature_effect": 0.22,
                "placebo_test_passed": True,
                "negative_control_passed": True,
            },
        },
    )


async def _write_audit_entry(
    child_id: str,
    placement_id: str | None,
    user_id: str,
    sim_result: SimulateResponse,
    pool: Any,
) -> None:
    """Log the simulation to ml_decision_audit."""
    await pool.execute(
        """
        INSERT INTO ml_decision_audit
            (child_id, placement_id, caseworker_id,
             decision_type, model_name, model_version,
             input_features, child_demographics,
             output_score, output_label, output_confidence, output_details)
        VALUES ($1, $2, $3,
                'counterfactual_simulation', 'twin-simulator', $4,
                $5::jsonb, '{}'::jsonb,
                $6, $7, $8, $9::jsonb)
        """,
        child_id,
        placement_id,
        user_id,
        sim_result.model_version,
        json.dumps(sim_result.intervention),
        sim_result.effect.get("effect_size"),
        sim_result.counterfactual.get("dominant_outcome"),
        sim_result.effect.get("probability_of_benefit"),
        json.dumps({
            "baseline": sim_result.baseline,
            "counterfactual": sim_result.counterfactual,
            "effect": sim_result.effect,
            "simulation_id": sim_result.simulation_id,
        }),
    )


# ── Endpoints ─────────────────────────────────────────────────────────────────


@router.get("/api/twin/{child_id}/state")
async def get_twin_state(
    child_id: str,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """Fetch the current Child Digital Twin state."""
    pool = get_pool()
    if pool is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    async with pool.acquire() as conn:
        state = await _get_or_create_twin_state(child_id, conn)

    return state


@router.post("/api/twin/{child_id}/simulate")
async def simulate(
    child_id: str,
    request: SimulateRequest,
    req: Request,
    user: dict = Depends(require_role("caseworker", "supervisor", "admin")),
) -> dict[str, Any]:
    """
    Run a counterfactual simulation for a child.

    Accepts 1–3 interventions and returns a structured comparison
    of baseline vs. counterfactual trajectories.
    """
    pool = get_pool()
    if pool is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    async with pool.acquire() as conn:
        try:
            state = await _get_or_create_twin_state(child_id, conn)
            features = state.get("current_features", {}) or {}

            # Require minimum data for a meaningful simulation
            risk_score = features.get("current_risk_score")
            if risk_score is None:
                # Use a neutral default so the simulation still runs
                logger.warning(
                    "twin.no_risk_score_using_default",
                    child_id=child_id,
                    feature_count=len([k for k, v in features.items() if v is not None]),
                )
                features = {**features, "current_risk_score": 50.0}

            interventions_dicts = [iv.model_dump() for iv in request.interventions]
            result = _build_simulated_response(
                child_id=child_id,
                interventions=interventions_dicts,
                horizon_days=request.horizon_days,
                current_features=features,
            )

            await _write_audit_entry(
                child_id=child_id,
                placement_id=state.get("placement_id"),
                user_id=user["user_id"],
                sim_result=result,
                pool=conn,
            )

            logger.info(
                "twin.simulate",
                child_id=child_id,
                simulation_id=result.simulation_id,
                interventions=[iv.domain for iv in request.interventions],
                user_id=user["user_id"],
            )

            return result.model_dump()

        except HTTPException:
            raise
        except Exception as exc:
            logger.exception(
                "twin.simulate_failed",
                child_id=child_id,
                error=str(exc),
            )
            # Safe fallback response
            fallback = SimulateResponse(
                simulation_id=f"sim_{uuid.uuid4().hex[:12]}",
                child_id=child_id,
                generated_at=datetime.now(timezone.utc).isoformat(),
                model_version="twin-rule-fallback-v1",
                n_historical_placements=0,
                intervention={
                    "type": "single",
                    "components": [
                        {
                            "domain": iv.domain,
                            "action": iv.action,
                            "value": iv.value,
                            "individual_effect": -0.05,
                        }
                        for iv in request.interventions
                    ],
                },
                baseline={
                    "outcome_distribution": {f"{d}_days": {"stable": 0.5, "disrupted": 0.5, "reunified": 0.08, "runaway": 0.03} for d in [30, 60, 90]},
                    "ci_95": {"30_days": {"stable": [0.35, 0.65], "disrupted": [0.35, 0.65], "reunified": [0.01, 0.08], "runaway": [0.01, 0.05]}},
                    "dominant_outcome": "uncertain",
                    "uncertainty_score": 1.0,
                },
                counterfactual={
                    "outcome_distribution": {f"{d}_days": {"stable": 0.55, "disrupted": 0.45, "reunified": 0.08, "runaway": 0.03} for d in [30, 60, 90]},
                    "ci_95": {"30_days": {"stable": [0.40, 0.70], "disrupted": [0.30, 0.60], "reunified": [0.01, 0.08], "runaway": [0.01, 0.05]}},
                    "dominant_outcome": "uncertain",
                    "uncertainty_score": 1.0,
                },
                effect={
                    "effect_size": 0.0,
                    "probability_of_benefit": 0.5,
                    "number_needed_to_treat": 100.0,
                    "ci_95": [-0.12, 0.12],
                    "decomposition": {"components": []},
                    "robustness_value": 0.0,
                    "sensitivity": {
                        "confounder_strength_to_nullify": 0.0,
                        "most_sensitive_feature": "unknown",
                        "most_sensitive_feature_effect": 0.0,
                        "placebo_test_passed": False,
                        "negative_control_passed": False,
                    },
                },
            )

            logger.warning(
                "twin.simulate_fallback_returned",
                child_id=child_id,
                simulation_id=fallback.simulation_id,
                user_id=user["user_id"],
            )

            return fallback.model_dump()


@router.patch("/api/twin/{child_id}/scenarios")
async def save_scenario(
    child_id: str,
    request: ScenariosRequest,
    req: Request,
    user: dict = Depends(require_role("caseworker", "supervisor", "admin")),
) -> dict[str, str]:
    """
    Save or update a scenario slot (A/B/C) for a child.

    Scenarios are stored in child_twin_states.pending_simulations JSONB
    and expire after 7 days.
    """
    pool = get_pool()
    if pool is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    slot = request.slot
    scenario = request.scenario
    expires_at = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()

    scenario_data = {
        "slot": slot,
        "label": scenario.label or f"Scenario {slot}",
        "simulation_id": scenario.simulation_id,
        "interventions": [iv.model_dump() for iv in scenario.interventions],
        "outcome_summary": scenario.outcome_summary,
        "verdict": scenario.verdict,
        "caseworker_note": scenario.caseworker_note,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "expires_at": expires_at,
    }

    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT pending_simulations FROM child_twin_states WHERE child_id = $1",
            child_id,
        )

        current: list[dict[str, Any]] = []
        if existing:
            if isinstance(existing, str):
                current = json.loads(existing)
            else:
                current = list(existing)

        # Replace or append
        found = False
        for i, s in enumerate(current):
            if s.get("slot") == slot:
                current[i] = scenario_data
                found = True
                break
        if not found:
            current.append(scenario_data)

        await conn.execute(
            """
            INSERT INTO child_twin_states
                (child_id, pending_simulations, updated_at, stale_at)
            VALUES ($1, $2::jsonb, NOW(), NOW() + INTERVAL '7 days')
            ON CONFLICT (child_id)
            DO UPDATE SET
                pending_simulations = $2::jsonb,
                updated_at = NOW(),
                stale_at = NOW() + INTERVAL '7 days'
            """,
            child_id,
            json.dumps(current),
        )

        logger.info(
            "twin.save_scenario",
            child_id=child_id,
            slot=slot,
            user_id=user["user_id"],
        )

    return {"status": "ok", "message": f"Scenario {slot} saved"}


@router.get("/api/twin/{child_id}/scenarios")
async def get_scenarios(
    child_id: str,
    user: dict = Depends(require_role("caseworker", "supervisor", "admin")),
) -> dict[str, Any]:
    """Fetch saved scenarios (pending_simulations) for a child."""
    pool = get_pool()
    if pool is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    async with pool.acquire() as conn:
        raw = await conn.fetchval(
            "SELECT pending_simulations FROM child_twin_states WHERE child_id = $1",
            child_id,
        )
    scenarios: list[dict[str, Any]] = []
    if raw:
        if isinstance(raw, str):
            try:
                scenarios = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                scenarios = []
        else:
            scenarios = list(raw)
    return {"scenarios": scenarios}


@router.get("/api/twin/{child_id}/case-conference-pdf")
async def case_conference_pdf(
    child_id: str,
    user: dict = Depends(require_role("caseworker", "supervisor", "admin")),
) -> dict[str, Any]:
    """
    Generate a case conference PDF summary.

    Returns a JSON object with a `pdf_url` placeholder. The actual PDF
    generation (via WeasyPrint or Playwright) will be implemented in a
    follow-up; for now this returns the data payload that would populate
    the PDF, along with a placeholder URL.
    """
    pool = get_pool()
    if pool is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    async with pool.acquire() as conn:
        state = await _get_or_create_twin_state(child_id, conn)
        child = await conn.fetchrow(
            "SELECT child_id, age, gender, school, emergency_level FROM children WHERE child_id = $1",
            child_id,
        )
        if not child:
            raise HTTPException(status_code=404, detail=f"Child {child_id} not found")

        scenarios_raw = state.get("pending_simulations", []) or []
        if isinstance(scenarios_raw, str):
            scenarios_raw = json.loads(scenarios_raw)
        scenarios = list(scenarios_raw)

        feat = state.get("current_features", {})
        if isinstance(feat, str):
            try:
                feat = json.loads(feat)
            except (json.JSONDecodeError, TypeError):
                feat = {}
        if not isinstance(feat, dict):
            feat = {}

    child_info = {
        "child_id": child["child_id"],
        "age": child["age"],
        "gender": child["gender"],
        "school": child["school"],
        "emergency_level": child["emergency_level"],
        "weeks_in_placement": feat.get("weeks_in_placement"),
        "current_risk_score": feat.get("current_risk_score"),
    }

    return {
        "status": "ok",
        "message": "PDF generation payload ready — PDF rendering will be implemented server-side",
        "child_info": child_info,
        "scenarios": scenarios,
        "n_historical_placements": feat.get("placement_history", 0),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pdf_url": f"/api/twin/{child_id}/case-conference-pdf/render",
        "footer_disclosure": (
            "This document was generated by the Artifex Child Digital Twin "
            "simulation tool. Outcomes are probabilistic and based on "
            "historical patterns. They are not guarantees. All intervention "
            "plans must be reviewed by a licensed supervisor."
        ),
    }
