# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Bayesian Structural Time Series (BSTS) Causal Impact Engine (Task 24).

Answers the retrospective question: "What *would* have happened to metric Y
during the event window if the marketing event had NOT occurred?"

Architecture
------------
    CausalImpactEngine
        ├── __init__(data, n_draws, n_chains, n_warmup, seed)
        ├── _smooth_zero_inflated(y)    — rolling-mean guard for sparse daily data
        ├── _validate_ratio()          — enforce 4:1 pre:post baseline requirement
        ├── _detrend_by_controls()     — OLS regression on control covariates
        ├── _build_model(y_obs)        — LocalLinearTrend + Seasonal(7) + LinearRegression
        ├── _merge_chains(samples)     — flatten [draws, chains, ...] → [draws*chains, ...]
        ├── _compute_rhat(samples)     — max R-hat from HMC chain samples
        ├── fit_and_forecast()         — full pipeline; returns CausalImpactResult
        └── write_to_bq(result)        — streams causal_impact_runs + causal_impact_metrics

JAX / TFP-JAX Backend
----------------------
All statistical computations use:
    tensorflow_probability.substrates.jax  (tfp_jax)
    tensorflow_probability.substrates.jax.sts

TFP-JAX does NOT require TensorFlow Core. It runs on JAX/XLA directly and is
compatible with the XLA device configuration established in meridian_analyst_engine.py.

Install:
    pip install 'paid-media-agent[causal]'
    # Or combined with MMM: pip install 'paid-media-agent[mmm,causal]'

Model Composition
-----------------
    LocalLinearTrend    — captures macro level + slope drift in the pre-period
    Seasonal(7, 1)      — 7-day weekly seasonality (one step per season for daily data)
    LinearRegression(K) — ingests K independent control series vectors (non-contaminated)

Control Series Rationale
------------------------
Control series must be channels / metrics NOT affected by the intervention:
    Example: if Google Ads spend halted → Meta, LinkedIn, organic sessions are valid controls
    Invalid: the target channel itself, or any directly correlated upstream metric

If no control series are provided, the model runs LocalLinearTrend + Seasonal only.
This is valid but produces wider credible intervals.

Counterfactual Construction
---------------------------
1. Optionally regress y_pre on control_pre via OLS; obtain residuals_pre.
2. Fit BSTS on residuals_pre (or y_pre if no controls) via HMC.
3. Forecast residuals for T_post steps into the future.
4. Re-add the OLS regression adjustment for the post period.
5. absolute_effect_t = y_post[t] - counterfactual_mean_t
6. posterior_tail_probability = P(Σ_t counterfactual_t ≥ Σ_t y_post_t | data)

Output Tables (paid-media-schema / 10_causal_impact.sql)
---------------------------------------------------------
    causal_impact_runs       — one row per run: metadata, model config, diagnostics
    causal_impact_metrics    — cumulative, average_daily, and per-day effect estimates
"""
from __future__ import annotations

import json
import math
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np
import structlog

log = structlog.get_logger()

# ── JAX / XLA configuration ────────────────────────────────────────────────────
# Mirror the config in meridian_analyst_engine.py so both engines share the same
# XLA device pool when co-deployed on a Cloud Run instance.
_xla_flags = os.getenv("XLA_FLAGS", "")
if "--xla_force_host_platform_device_count" not in _xla_flags:
    os.environ["XLA_FLAGS"] = (
        (_xla_flags + " " if _xla_flags else "")
        + "--xla_force_host_platform_device_count=4"
    ).strip()
if not os.getenv("JAX_PLATFORM_NAME"):
    os.environ["JAX_PLATFORM_NAME"] = "cpu"

# ── Constants ──────────────────────────────────────────────────────────────────

# Minimum ratio of pre-period to post-period observations.
# The BSTS structural trend requires adequate baseline data to stabilize.
# 4:1 is the practical minimum; 8:1 or higher is recommended for noisy metrics.
MIN_PRE_POST_RATIO: float = 4.0

# Zero-inflation threshold: if more than this fraction of values are zero,
# apply rolling-mean smoothing before model fitting.
ZERO_INFLATION_THRESHOLD: float = 0.30

# Rolling average window for zero-inflation smoothing (days).
SMOOTHING_WINDOW: int = 7

# Significance threshold for posterior tail probability.
# posterior_tail_prob < SIGNIFICANCE_THRESHOLD → effect is statistically significant.
SIGNIFICANCE_THRESHOLD: float = 0.10   # 90% posterior certainty

# Weekly seasonal period for daily data.
SEASONAL_PERIODS: int = 7


# ── Data structures ────────────────────────────────────────────────────────────


@dataclass
class CausalInputData:
    """
    Input tensors for the BSTS causal impact analysis.

    Attributes
    ----------
    y_pre : np.ndarray [T_pre]
        Daily target metric values in the pre-intervention baseline window.
    y_post : np.ndarray [T_post]
        Daily target metric actual values in the post-intervention window.
    control_pre : np.ndarray | None [T_pre, K]
        Control covariate matrix for the pre-period. Columns = K independent series.
    control_post : np.ndarray | None [T_post, K]
        Control covariate matrix for the post-period (must match control_pre columns).
    dates_pre : list[str]
        Ordered date strings (YYYY-MM-DD) matching y_pre.
    dates_post : list[str]
        Ordered date strings (YYYY-MM-DD) matching y_post.
    control_names : list[str]
        Names of the K control series (for BQ metadata).
    target_metric : str
        Human-readable label for the target metric.
    target_channel : str | None
        Platform / channel name (e.g. "google_ads"), or None for cross-channel.
    target_geo : str | None
        ISO country code filter, or None for all geos.
    """
    y_pre:          np.ndarray
    y_post:         np.ndarray
    control_pre:    np.ndarray | None
    control_post:   np.ndarray | None
    dates_pre:      list[str]
    dates_post:     list[str]
    control_names:  list[str] = field(default_factory=list)
    target_metric:  str = "conversions"
    target_channel: str | None = None
    target_geo:     str | None = None

    @property
    def n_pre(self) -> int:
        return len(self.y_pre)

    @property
    def n_post(self) -> int:
        return len(self.y_post)

    @property
    def n_controls(self) -> int:
        return self.control_pre.shape[1] if self.control_pre is not None else 0

    @property
    def pre_post_ratio(self) -> float:
        return self.n_pre / max(self.n_post, 1)


@dataclass
class CausalImpactResult:
    """
    Full output of the BSTS causal impact analysis.

    Includes the posterior counterfactual distribution, point-in-time effect
    estimates, cumulative statistics, and MCMC diagnostics.
    """
    run_id:                  str
    # ── Posterior predictive series ──────────────────────────────────────────
    dates_post:              list[str]
    actual_post:             np.ndarray    # [T_post] observed values
    counterfactual_mean:     np.ndarray    # [T_post] posterior mean of counterfactual
    counterfactual_lower:    np.ndarray    # [T_post] 5th percentile
    counterfactual_upper:    np.ndarray    # [T_post] 95th percentile
    # ── Effect series ────────────────────────────────────────────────────────
    absolute_effect:         np.ndarray    # [T_post] actual - counterfactual_mean
    absolute_effect_lower:   np.ndarray    # [T_post] 5th percentile
    absolute_effect_upper:   np.ndarray    # [T_post] 95th percentile
    # ── Cumulative estimates ─────────────────────────────────────────────────
    cumulative_actual:       float
    cumulative_counterfactual: float
    cumulative_effect:       float
    cumulative_effect_lower: float
    cumulative_effect_upper: float
    # ── Summary statistics ───────────────────────────────────────────────────
    relative_effect_pct:     float         # % change vs counterfactual
    posterior_tail_prob:     float         # P(effect ≤ 0 | data) — Bayesian p-value
    is_significant:          bool
    # ── Diagnostics ──────────────────────────────────────────────────────────
    r_hat_max:               float | None
    elapsed_seconds:         float
    n_draws:                 int
    n_chains:                int
    zero_smoothing_applied:  bool
    model_components:        list[str]


# ── Statistical helpers ────────────────────────────────────────────────────────


def smooth_zero_inflated(y: np.ndarray, window: int = SMOOTHING_WINDOW) -> np.ndarray:
    """
    Apply a centered rolling mean to smooth zero-inflated daily metrics.

    Only applied when the zero fraction exceeds ZERO_INFLATION_THRESHOLD (30%).
    Zeros in daily paid-media data typically reflect reporting latency or
    day-of-week effects, not true absence of activity. Smoothing prevents
    the BSTS state-space model from fitting a zero-inflated observation noise
    model instead of the true trend structure.

    Args:
        y:      Daily time series array.
        window: Rolling average window in days (default: 7 = weekly).

    Returns:
        Smoothed array (same length as y), or y unchanged if smoothing not needed.
    """
    zero_fraction = float(np.mean(y == 0))
    if zero_fraction < ZERO_INFLATION_THRESHOLD:
        return y.astype(float)

    log.info(
        "causal_engine.smoothing_applied",
        zero_fraction=round(zero_fraction, 3),
        window=window,
        note=f"{round(zero_fraction*100)}% zeros detected; applying {window}-day rolling mean.",
    )
    half = window // 2
    smoothed = np.zeros(len(y), dtype=float)
    for i in range(len(y)):
        lo = max(0, i - half)
        hi = min(len(y), i + half + 1)
        smoothed[i] = float(np.mean(y[lo:hi]))
    return smoothed


# ── Core engine ────────────────────────────────────────────────────────────────


class CausalImpactEngine:
    """
    BSTS causal impact engine using tensorflow_probability.substrates.jax.sts.

    Parameters
    ----------
    data : CausalInputData
        Pre/post metric arrays and optional control covariate matrices.
    n_draws : int
        Post-warmup HMC draws per chain (default 200).
    n_chains : int
        Parallel MCMC chains (default 4, matching XLA device count).
    n_warmup : int
        HMC warmup/adaptation steps per chain (default 100).
    seed : int
        JAX PRNG seed for reproducibility.
    """

    def __init__(
        self,
        data:     CausalInputData,
        n_draws:  int = 200,
        n_chains: int = 4,
        n_warmup: int = 100,
        seed:     int = 42,
    ) -> None:
        self.data     = data
        self.n_draws  = n_draws
        self.n_chains = n_chains
        self.n_warmup = n_warmup
        self.seed     = seed
        self.run_id   = str(uuid.uuid4())

        log.info(
            "causal_engine.init",
            run_id=self.run_id,
            n_pre=data.n_pre,
            n_post=data.n_post,
            pre_post_ratio=round(data.pre_post_ratio, 2),
            n_controls=data.n_controls,
            target_metric=data.target_metric,
        )

    # ── Dependency check ───────────────────────────────────────────────────────

    @staticmethod
    def _check_deps() -> None:
        missing = []
        for pkg in ["tensorflow_probability", "jax", "numpy"]:
            try:
                __import__(pkg)
            except ImportError:
                missing.append(pkg)
        if missing:
            raise ImportError(
                f"Missing causal impact dependencies: {', '.join(missing)}. "
                "Install with: pip install 'paid-media-agent[causal]'"
            )

    # ── Pre-flight validation ──────────────────────────────────────────────────

    def _validate_ratio(self) -> None:
        """
        Enforce the 4:1 minimum pre:post ratio for structural trend stabilization.

        The BSTS LocalLinearTrend component needs sufficient baseline data to
        separate the organic trend from the noise level. With fewer than 4× the
        pre-period observations relative to post-period, the model systematically
        underestimates uncertainty in the counterfactual forecast.
        """
        ratio = self.data.pre_post_ratio
        if ratio < MIN_PRE_POST_RATIO:
            raise ValueError(
                f"Pre-period too short for reliable BSTS inference. "
                f"Got {self.data.n_pre} pre-period vs. {self.data.n_post} post-period "
                f"observations (ratio = {ratio:.2f}). "
                f"Minimum required: {MIN_PRE_POST_RATIO}:1. "
                f"Extend the pre_period_from date by at least "
                f"{int(self.data.n_post * MIN_PRE_POST_RATIO) - self.data.n_pre} more days."
            )
        if self.data.n_pre < SEASONAL_PERIODS * 2:
            raise ValueError(
                f"Pre-period must span at least {SEASONAL_PERIODS * 2} days (2 full weeks) "
                f"for the weekly Seasonal component to fit. Got {self.data.n_pre} days."
            )
        log.debug(
            "causal_engine.ratio_ok",
            n_pre=self.data.n_pre,
            n_post=self.data.n_post,
            ratio=round(ratio, 2),
        )

    def _validate_inputs(self) -> None:
        """
        Reject NaN/inf and malformed shapes BEFORE HMC. A single NaN propagates
        through the posterior silently and HMC returns garbage with no error,
        so these are hard failures with actionable messages, not warnings.
        """
        for name, arr in (("y_pre", self.data.y_pre), ("y_post", self.data.y_post)):
            a = np.asarray(arr, dtype=float)
            if a.ndim != 1:
                raise ValueError(
                    f"{name} must be a 1-D series, got shape {a.shape}. "
                    "Pass one observation per day."
                )
            if a.size == 0:
                raise ValueError(f"{name} is empty — no observations in the window.")
            bad = ~np.isfinite(a)
            if bad.any():
                idx = np.flatnonzero(bad)[:5].tolist()
                raise ValueError(
                    f"{name} contains {int(bad.sum())} NaN/inf value(s) "
                    f"(first indices: {idx}). Clean or impute the source series — "
                    "HMC produces silently invalid posteriors on non-finite input."
                )

        for name, mat, expected_rows in (
            ("control_pre", self.data.control_pre, self.data.n_pre),
            ("control_post", self.data.control_post, self.data.n_post),
        ):
            if mat is None:
                continue
            m = np.asarray(mat, dtype=float)
            if m.ndim != 2:
                raise ValueError(
                    f"{name} must be a 2-D [T, K] matrix, got shape {m.shape}."
                )
            if m.shape[0] != expected_rows:
                raise ValueError(
                    f"{name} has {m.shape[0]} rows but the target series has "
                    f"{expected_rows} observations — control series must align 1:1 by date."
                )
            if not np.isfinite(m).all():
                raise ValueError(
                    f"{name} contains NaN/inf values. Clean the control series first."
                )

        cp, cq = self.data.control_pre, self.data.control_post
        if (cp is None) != (cq is None):
            raise ValueError(
                "control_pre and control_post must both be provided or both be None."
            )
        if cp is not None and cq is not None and cp.shape[1] != cq.shape[1]:
            raise ValueError(
                f"control_pre has {cp.shape[1]} columns but control_post has "
                f"{cq.shape[1]} — the covariate sets must match."
            )

    # ── Control covariate regression ───────────────────────────────────────────

    def _detrend_by_controls(
        self,
        y_pre: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        OLS regression of y_pre on control_pre; return (residuals_pre, regression_adj_post).

        The regression captures shared variance between the target metric and the
        control series (non-contaminated channels). Fitting BSTS on the residuals
        isolates the structural trend from co-movement with controls, producing a
        tighter counterfactual posterior.

        Returns:
            (residuals_pre, regression_adjustment_post)
            residuals_pre:             [T_pre] OLS residuals from the pre-period
            regression_adjustment_post: [T_post] OLS prediction for post-period
                                        (added back to the BSTS counterfactual)
        """
        ctrl_pre  = self.data.control_pre
        ctrl_post = self.data.control_post

        if ctrl_pre is None or ctrl_post is None:
            return y_pre, np.zeros(self.data.n_post, dtype=float)

        T_pre = self.data.n_pre

        # Augment with intercept column
        X_pre  = np.column_stack([np.ones(T_pre), ctrl_pre])
        X_post = np.column_stack([np.ones(self.data.n_post), ctrl_post])

        # OLS via least-squares: β = (X'X)⁻¹ X'y
        coeffs, *_ = np.linalg.lstsq(X_pre, y_pre, rcond=None)

        residuals_pre        = y_pre       - X_pre  @ coeffs
        regression_adj_post  = X_post @ coeffs

        log.debug(
            "causal_engine.control_regression",
            n_controls=self.data.n_controls,
            residual_std=round(float(np.std(residuals_pre)), 4),
            adj_post_mean=round(float(np.mean(regression_adj_post)), 4),
        )
        return residuals_pre, regression_adj_post

    # ── BSTS model construction ────────────────────────────────────────────────

    def _build_model(self, y_obs: np.ndarray) -> tuple[Any, list[str]]:
        """
        Compose the Bayesian Structural Time Series model.

        Components:
            LocalLinearTrend    — level + slope drift; captures macro trend
            Seasonal(7, 1)      — weekly seasonality for daily observations
            (LinearRegression   — handled via _detrend_by_controls(); not in TFP STS
                                  directly because post-period covariates need separate path)

        Args:
            y_obs: The (possibly residualized) pre-period observation array.

        Returns:
            (model, component_labels): STS model + list of component names for metadata.
        """
        import tensorflow_probability.substrates.jax as tfp_jax
        tfp_sts = tfp_jax.sts
        import jax.numpy as jnp

        y_jnp = jnp.array(y_obs, dtype=jnp.float32)

        # ── LocalLinearTrend ──────────────────────────────────────────────────
        # Models a time-varying level and slope. The slope captures gradual trend
        # growth or decay. For very short pre-periods (< 3 weeks), LocalLevel
        # (slope=0) may be more stable; LocalLinearTrend is the default.
        trend = tfp_sts.LocalLinearTrend(
            observed_time_series=y_jnp,
            name="local_linear_trend",
        )
        component_labels = ["LocalLinearTrend"]

        # ── Seasonal (7-day weekly) ───────────────────────────────────────────
        # Captures day-of-week effects (e.g. weekend dips in B2B conversions).
        # num_steps_per_season=1 means each season lasts exactly one time step.
        seasonal = tfp_sts.Seasonal(
            num_seasons=SEASONAL_PERIODS,
            num_steps_per_season=1,
            observed_time_series=y_jnp,
            name="day_of_week_seasonal",
        )
        component_labels.append(f"Seasonal({SEASONAL_PERIODS})")

        # Compose into a Sum model
        components = [trend, seasonal]
        model = tfp_sts.Sum(components, observed_time_series=y_jnp)

        log.debug(
            "causal_engine.model_built",
            run_id=self.run_id,
            components=component_labels,
            n_obs=len(y_obs),
        )
        return model, component_labels

    # ── Chain flattening ───────────────────────────────────────────────────────

    @staticmethod
    def _merge_chains(samples: Any) -> Any:
        """
        Reshape HMC samples from [num_results, num_chains, ...] to
        [num_results × num_chains, ...] for use with tfp.sts.forecast().

        tfp.sts.fit_with_hmc returns separate chain and result dimensions.
        tfp.sts.forecast expects a single "sample" dimension.
        """
        import jax.numpy as jnp

        def _flatten(v: Any) -> Any:
            arr = jnp.array(v)
            if arr.ndim >= 2:
                n_results, n_chains = arr.shape[:2]
                return arr.reshape(n_results * n_chains, *arr.shape[2:])
            return arr

        # Handle both dict-like and namedtuple-like parameter structures
        if isinstance(samples, dict):
            return {k: _flatten(v) for k, v in samples.items()}
        if hasattr(samples, "_fields"):  # namedtuple
            return type(samples)(*[_flatten(v) for v in samples])
        return _flatten(samples)

    # ── Convergence diagnostics ────────────────────────────────────────────────

    def _compute_rhat(self, samples: Any) -> float | None:
        """
        Compute the maximum R-hat (potential scale reduction factor) across
        all STS parameters using TFP's diagnostic function.

        Iterates over the parameter structure (dict or namedtuple) and extracts
        per-parameter R-hat values. Returns the worst-case (maximum) scalar.

        R-hat interpretation:
            < 1.05  — excellent convergence
            < 1.10  — acceptable
            ≥ 1.10  — increase n_draws or n_warmup
        """
        try:
            import tensorflow_probability.substrates.jax as tfp_jax
            import jax.numpy as jnp

            # Extract iterable of parameter tensors
            if isinstance(samples, dict):
                param_arrays = list(samples.values())
            elif hasattr(samples, "_fields"):
                param_arrays = list(samples)
            else:
                param_arrays = [samples]

            rhat_vals: list[float] = []
            for arr in param_arrays:
                # arr shape: [num_results, num_chains, ...]
                # potential_scale_reduction expects [num_results, num_chains, ...]
                rhat = tfp_jax.mcmc.diagnostic.potential_scale_reduction(
                    jnp.array(arr), split_chains=True
                )
                rhat_vals.extend(
                    float(x)
                    for x in jnp.ravel(rhat)
                    if not jnp.isnan(x)
                )

            return round(max(rhat_vals), 4) if rhat_vals else None

        except Exception as exc:
            log.warning("causal_engine.rhat_unavailable", error=str(exc))
            return None

    # ── Full inference pipeline ────────────────────────────────────────────────

    def fit_and_forecast(self) -> CausalImpactResult:
        """
        Execute the full BSTS causal impact analysis:

        1. Pre-flight validation (4:1 ratio check, minimum length check).
        2. Zero-inflation smoothing of y_pre if needed.
        3. OLS detrending by control covariates (if control_pre provided).
        4. BSTS model construction (LocalLinearTrend + Seasonal).
        5. HMC posterior sampling on pre-period residuals.
        6. Counterfactual forecast for T_post steps.
        7. Re-add OLS regression adjustment to counterfactual.
        8. Compute absolute/relative effects and posterior tail probability.

        Returns:
            CausalImpactResult with the full posterior predictive series,
            cumulative effect statistics, and MCMC diagnostics.
        """
        self._check_deps()

        import jax
        import jax.numpy as jnp
        import tensorflow_probability.substrates.jax as tfp_jax
        tfp_sts = tfp_jax.sts

        self._validate_ratio()
        self._validate_inputs()

        t0 = time.time()

        # ── Step 1: Zero-inflation smoothing ──────────────────────────────────
        y_pre_raw = self.data.y_pre.astype(float)
        y_pre     = smooth_zero_inflated(y_pre_raw)
        zero_smoothed = not np.array_equal(y_pre, y_pre_raw)

        # ── Step 2: OLS detrending by control covariates ──────────────────────
        residuals_pre, regression_adj_post = self._detrend_by_controls(y_pre)

        component_labels_extra = []
        if self.data.n_controls > 0:
            component_labels_extra.append(
                f"LinearRegression({', '.join(self.data.control_names)})"
            )

        # ── Step 3: Build STS model ────────────────────────────────────────────
        model, component_labels = self._build_model(residuals_pre)
        component_labels = component_labels + component_labels_extra

        obs_jnp = jnp.array(residuals_pre, dtype=jnp.float32)

        log.info(
            "causal_engine.hmc_start",
            run_id=self.run_id,
            n_draws=self.n_draws,
            n_chains=self.n_chains,
            n_warmup=self.n_warmup,
            n_pre=self.data.n_pre,
            components=component_labels,
        )

        # ── Step 4: HMC posterior sampling ────────────────────────────────────
        # fit_with_hmc returns:
        #   samples: {param_name: array[n_draws, n_chains, param_shape]}
        #   kernel_results: NUTS diagnostics
        try:
            samples, kernel_results = tfp_sts.fit_with_hmc(
                model=model,
                observed_time_series=obs_jnp,
                num_results=self.n_draws,
                num_warmup_steps=self.n_warmup,
                num_chains=self.n_chains,
                seed=jax.random.PRNGKey(self.seed),
            )
        except Exception as exc:
            log.error("causal_engine.hmc_failed", run_id=self.run_id, error=str(exc))
            raise

        elapsed = time.time() - t0

        # ── Step 5: Convergence diagnostics ───────────────────────────────────
        r_hat_max = self._compute_rhat(samples)
        if r_hat_max is not None and r_hat_max >= 1.10:
            log.warning(
                "causal_engine.convergence_warning",
                r_hat_max=r_hat_max,
                recommendation=(
                    "R-hat ≥ 1.10. Increase n_draws or n_warmup, "
                    "or extend the pre-period baseline."
                ),
            )

        # ── Step 6: Counterfactual forecast ───────────────────────────────────
        # Flatten chains for forecast: [n_draws, n_chains, ...] → [n_draws*n_chains, ...]
        samples_flat = self._merge_chains(samples)

        forecast_dist = tfp_sts.forecast(
            model=model,
            observed_time_series=obs_jnp,
            parameter_samples=samples_flat,
            num_steps_forecast=self.data.n_post,
            include_observation_noise=True,
        )

        # Draw from the posterior predictive distribution
        # forecast_dist: batch_shape=[n_draws*n_chains], event_shape=[T_post]
        n_total = self.n_draws * self.n_chains
        forecast_samples = forecast_dist.sample(
            seed=jax.random.PRNGKey(self.seed + 1)
        )
        # Shape: [n_total, T_post] (samples from distribution for each param sample)
        forecast_samples = jnp.array(forecast_samples)
        if forecast_samples.ndim == 1:
            # Scalar distribution: expand to [n_total, T_post]
            forecast_samples = forecast_samples.reshape(n_total, self.data.n_post)

        # ── Step 7: Re-add regression adjustment ──────────────────────────────
        # counterfactual_residuals + regression_prediction = counterfactual_y
        reg_adj = jnp.array(regression_adj_post, dtype=jnp.float32)  # [T_post]
        counterfactual_samples = forecast_samples + reg_adj[None, :]  # [n_total, T_post]

        # ── Step 8: Posterior summary statistics ──────────────────────────────
        y_post     = jnp.array(self.data.y_post, dtype=jnp.float32)  # [T_post]
        cf_mean    = jnp.mean(counterfactual_samples,          axis=0)  # [T_post]
        cf_lower   = jnp.percentile(counterfactual_samples, 5,  axis=0)
        cf_upper   = jnp.percentile(counterfactual_samples, 95, axis=0)

        # Absolute effect: actual - counterfactual
        # Posterior distribution: actual[t] - counterfactual_samples[:, t]
        effect_samples = y_post[None, :] - counterfactual_samples      # [n_total, T_post]
        eff_mean  = jnp.mean(effect_samples,          axis=0)
        eff_lower = jnp.percentile(effect_samples, 5,  axis=0)
        eff_upper = jnp.percentile(effect_samples, 95, axis=0)

        # Cumulative statistics
        cumul_actual   = float(jnp.sum(y_post))
        cumul_cf_samps = jnp.sum(counterfactual_samples, axis=1)  # [n_total]
        cumul_cf_mean  = float(jnp.mean(cumul_cf_samps))
        cumul_eff_samps = cumul_actual - cumul_cf_samps             # [n_total]
        cumul_eff_mean  = float(jnp.mean(cumul_eff_samps))
        cumul_eff_lower = float(jnp.percentile(cumul_eff_samps, 5))
        cumul_eff_upper = float(jnp.percentile(cumul_eff_samps, 95))

        # Posterior tail probability:
        # P(cumulative_counterfactual ≥ cumulative_actual | pre-period data)
        # = fraction of MCMC samples where the event had no positive effect
        # Low values → high certainty of a positive causal effect.
        posterior_tail_prob = float(jnp.mean(cumul_cf_samps >= cumul_actual))

        relative_effect_pct = (
            (cumul_eff_mean / max(abs(cumul_cf_mean), 1e-9)) * 100.0
        )
        is_significant = posterior_tail_prob < SIGNIFICANCE_THRESHOLD

        log.info(
            "causal_engine.forecast_complete",
            run_id=self.run_id,
            cumulative_actual=round(cumul_actual, 2),
            cumulative_counterfactual=round(cumul_cf_mean, 2),
            cumulative_effect=round(cumul_eff_mean, 2),
            relative_effect_pct=round(relative_effect_pct, 2),
            posterior_tail_prob=round(posterior_tail_prob, 4),
            is_significant=is_significant,
            r_hat_max=r_hat_max,
            elapsed_s=round(elapsed, 1),
        )

        return CausalImpactResult(
            run_id=self.run_id,
            dates_post=self.data.dates_post,
            actual_post=np.array(y_post),
            counterfactual_mean=np.array(cf_mean),
            counterfactual_lower=np.array(cf_lower),
            counterfactual_upper=np.array(cf_upper),
            absolute_effect=np.array(eff_mean),
            absolute_effect_lower=np.array(eff_lower),
            absolute_effect_upper=np.array(eff_upper),
            cumulative_actual=cumul_actual,
            cumulative_counterfactual=cumul_cf_mean,
            cumulative_effect=cumul_eff_mean,
            cumulative_effect_lower=cumul_eff_lower,
            cumulative_effect_upper=cumul_eff_upper,
            relative_effect_pct=relative_effect_pct,
            posterior_tail_prob=posterior_tail_prob,
            is_significant=is_significant,
            r_hat_max=r_hat_max,
            elapsed_seconds=round(elapsed, 1),
            n_draws=self.n_draws,
            n_chains=self.n_chains,
            zero_smoothing_applied=zero_smoothed,
            model_components=component_labels,
        )

    # ── BigQuery output ────────────────────────────────────────────────────────

    def write_to_bq(
        self,
        result:                   CausalImpactResult,
        intervention_description: str | None = None,
        analyst_notes:            str | None = None,
    ) -> dict:
        """
        Stream analysis results to the causal impact output tables.

        Writes:
            causal_impact_runs       — one run metadata row
            causal_impact_metrics    — cumulative, average_daily, and per-day rows

        Returns:
            Dict with run_id and write confirmation.
        """
        from tools.bigquery_client import insert_rows
        now = datetime.now(timezone.utc).isoformat()
        data = self.data
        n_post = data.n_post

        # ── causal_impact_runs row ─────────────────────────────────────────────
        run_row = {
            "run_id":                   result.run_id,
            "target_metric":            data.target_metric,
            "target_channel":           data.target_channel,
            "target_geo":               data.target_geo,
            "intervention_description": intervention_description,
            "pre_period_from":          data.dates_pre[0]  if data.dates_pre  else None,
            "pre_period_to":            data.dates_pre[-1] if data.dates_pre  else None,
            "post_period_from":         data.dates_post[0] if data.dates_post else None,
            "post_period_to":           data.dates_post[-1] if data.dates_post else None,
            "n_pre_periods":            data.n_pre,
            "n_post_periods":           n_post,
            "pre_post_ratio":           round(data.pre_post_ratio, 3),
            "model_components":         json.dumps(result.model_components),
            "control_series_names":     json.dumps(data.control_names),
            "n_control_series":         data.n_controls,
            "zero_smoothing_applied":   result.zero_smoothing_applied,
            "n_draws":                  result.n_draws,
            "n_chains":                 result.n_chains,
            "n_warmup":                 self.n_warmup,
            "r_hat_max":                result.r_hat_max,
            "elapsed_seconds":          result.elapsed_seconds,
            "status":                   "completed",
            "analyst_notes":            analyst_notes,
            "created_by":               "analyst_agent",
            "created_at":               now,
        }
        errors = insert_rows("causal_impact_runs", [run_row])
        if errors:
            log.error("causal_engine.bq_runs_error", errors=errors)
        else:
            log.info("causal_engine.bq_runs_written", run_id=result.run_id)

        # ── causal_impact_metrics rows ─────────────────────────────────────────
        metric_rows: list[dict] = []
        cf_ci_width_cumul = result.cumulative_effect_upper - result.cumulative_effect_lower

        # Row 1: cumulative
        metric_rows.append({
            "metric_id":                  str(uuid.uuid4()),
            "run_id":                     result.run_id,
            "period_type":                "cumulative",
            "period_date":                None,
            "actual_value":               result.cumulative_actual,
            "counterfactual_mean":        result.cumulative_counterfactual,
            "counterfactual_lower_90":    result.cumulative_counterfactual + result.cumulative_effect_lower - result.cumulative_effect,
            "counterfactual_upper_90":    result.cumulative_counterfactual + result.cumulative_effect_upper - result.cumulative_effect,
            "absolute_effect":            result.cumulative_effect,
            "absolute_effect_lower_90":   result.cumulative_effect_lower,
            "absolute_effect_upper_90":   result.cumulative_effect_upper,
            "relative_effect_pct":        round(result.relative_effect_pct, 4),
            "posterior_tail_probability": round(result.posterior_tail_prob, 6),
            "is_significant":             result.is_significant,
            "counterfactual_ci_width":    round(cf_ci_width_cumul, 4),
            "created_at":                 now,
        })

        # Row 2: average_daily
        avg_actual   = result.cumulative_actual   / max(n_post, 1)
        avg_cf       = result.cumulative_counterfactual / max(n_post, 1)
        avg_eff      = result.cumulative_effect   / max(n_post, 1)
        avg_eff_low  = result.cumulative_effect_lower / max(n_post, 1)
        avg_eff_high = result.cumulative_effect_upper / max(n_post, 1)

        metric_rows.append({
            "metric_id":                  str(uuid.uuid4()),
            "run_id":                     result.run_id,
            "period_type":                "average_daily",
            "period_date":                None,
            "actual_value":               round(avg_actual, 4),
            "counterfactual_mean":        round(avg_cf, 4),
            "counterfactual_lower_90":    round(avg_cf + avg_eff_low - avg_eff, 4),
            "counterfactual_upper_90":    round(avg_cf + avg_eff_high - avg_eff, 4),
            "absolute_effect":            round(avg_eff, 4),
            "absolute_effect_lower_90":   round(avg_eff_low, 4),
            "absolute_effect_upper_90":   round(avg_eff_high, 4),
            "relative_effect_pct":        round(result.relative_effect_pct, 4),
            "posterior_tail_probability": round(result.posterior_tail_prob, 6),
            "is_significant":             result.is_significant,
            "counterfactual_ci_width":    round(cf_ci_width_cumul / max(n_post, 1), 4),
            "created_at":                 now,
        })

        # Rows 3+: one per post-period day
        for t, date_str in enumerate(result.dates_post):
            cf_ci_w = float(result.counterfactual_upper[t] - result.counterfactual_lower[t])
            metric_rows.append({
                "metric_id":                  str(uuid.uuid4()),
                "run_id":                     result.run_id,
                "period_type":                "daily",
                "period_date":                date_str,
                "actual_value":               round(float(result.actual_post[t]), 4),
                "counterfactual_mean":        round(float(result.counterfactual_mean[t]), 4),
                "counterfactual_lower_90":    round(float(result.counterfactual_lower[t]), 4),
                "counterfactual_upper_90":    round(float(result.counterfactual_upper[t]), 4),
                "absolute_effect":            round(float(result.absolute_effect[t]), 4),
                "absolute_effect_lower_90":   round(float(result.absolute_effect_lower[t]), 4),
                "absolute_effect_upper_90":   round(float(result.absolute_effect_upper[t]), 4),
                "relative_effect_pct":        None,  # per-day relative effect not computed
                "posterior_tail_probability": None,  # per-day tail prob not computed
                "is_significant":             None,
                "counterfactual_ci_width":    round(cf_ci_w, 4),
                "created_at":                 now,
            })

        errors = insert_rows("causal_impact_metrics", metric_rows)
        if errors:
            log.error("causal_engine.bq_metrics_error", errors=errors)
        else:
            log.info(
                "causal_engine.bq_metrics_written",
                run_id=result.run_id,
                n_rows=len(metric_rows),
            )

        return {
            "run_id":          result.run_id,
            "rows_written":    len(metric_rows),
            "bq_tables":       ["causal_impact_runs", "causal_impact_metrics"],
        }


# ── Convenience pipeline ───────────────────────────────────────────────────────


def run_causal_analysis(
    data:                     CausalInputData,
    n_draws:                  int = 200,
    n_chains:                 int = 4,
    n_warmup:                 int = 100,
    seed:                     int = 42,
    write_to_bq:              bool = True,
    intervention_description: str | None = None,
    analyst_notes:            str | None = None,
) -> dict:
    """
    End-to-end causal impact pipeline: validate → fit → forecast → write BQ.

    This is the entry point called by the Analyst agent tool
    _tool_analyze_marketing_intervention().

    Returns a dict containing:
        run_id, result (CausalImpactResult), markdown_summary (str),
        bq_write (dict if write_to_bq else None).
    """
    engine = CausalImpactEngine(
        data=data,
        n_draws=n_draws,
        n_chains=n_chains,
        n_warmup=n_warmup,
        seed=seed,
    )

    result = engine.fit_and_forecast()

    bq_write = None
    if write_to_bq:
        bq_write = engine.write_to_bq(
            result,
            intervention_description=intervention_description,
            analyst_notes=analyst_notes,
        )

    # Build Markdown summary for agent output
    markdown = _build_markdown_summary(result, data, intervention_description)

    return {
        "run_id":           result.run_id,
        "is_significant":   result.is_significant,
        "cumulative_effect": round(result.cumulative_effect, 2),
        "relative_effect_pct": round(result.relative_effect_pct, 2),
        "posterior_tail_prob": round(result.posterior_tail_prob, 4),
        "r_hat_max":        result.r_hat_max,
        "elapsed_seconds":  result.elapsed_seconds,
        "model_components": result.model_components,
        "bq_write":         bq_write,
        "markdown_summary": markdown,
        # Full series for programmatic access
        "series": {
            "dates":                list(result.dates_post),
            "actual":               result.actual_post.tolist(),
            "counterfactual_mean":  result.counterfactual_mean.tolist(),
            "counterfactual_lower": result.counterfactual_lower.tolist(),
            "counterfactual_upper": result.counterfactual_upper.tolist(),
            "absolute_effect":      result.absolute_effect.tolist(),
            "absolute_effect_lower": result.absolute_effect_lower.tolist(),
            "absolute_effect_upper": result.absolute_effect_upper.tolist(),
        },
    }


def _build_markdown_summary(
    result: CausalImpactResult,
    data:   CausalInputData,
    intervention_description: str | None,
) -> str:
    """
    Generate a clean Markdown summary table for the analyst agent response.

    Includes: Estimated Absolute Impact, Relative Growth %, Statistical Certainty
    Index (1 - posterior_tail_prob), Bayesian p-value, and convergence status.
    """
    certainty = (1.0 - result.posterior_tail_prob) * 100.0
    sig_emoji = "✅" if result.is_significant else "⚠️"
    sig_label = "Yes" if result.is_significant else "No"

    effect_sign = "+" if result.cumulative_effect >= 0 else ""
    rel_sign    = "+" if result.relative_effect_pct >= 0 else ""

    if result.r_hat_max is not None:
        convergence_str = (
            f"{result.r_hat_max:.3f} ({'✅ converged' if result.r_hat_max < 1.10 else '⚠️ marginal'})"
        )
    else:
        convergence_str = "N/A"

    if result.is_significant:
        interpretation = (
            f"The data provides **{certainty:.0f}% statistical certainty** "
            f"that the intervention caused a meaningful change in {data.target_metric}. "
            f"The BSTS model estimates a cumulative effect of "
            f"{effect_sign}{result.cumulative_effect:,.1f} units over the "
            f"{data.n_post}-day window."
        )
    else:
        interpretation = (
            f"**No statistically significant effect detected** "
            f"(certainty = {certainty:.0f}%, below the 90% threshold). "
            f"The observed change is consistent with normal variation in the counterfactual. "
            f"Consider extending the post-period or adding control series for tighter inference."
        )

    channel_str = data.target_channel or "all channels"
    geo_str     = data.target_geo     or "all geos"
    pre_from    = data.dates_pre[0]  if data.dates_pre  else "—"
    pre_to      = data.dates_pre[-1] if data.dates_pre  else "—"
    post_from   = data.dates_post[0] if data.dates_post else "—"
    post_to     = data.dates_post[-1] if data.dates_post else "—"

    controls_str = ", ".join(data.control_names) if data.control_names else "none"

    return f"""## Causal Impact Analysis — {channel_str} ({data.target_metric})

**Event:** {intervention_description or 'Unspecified marketing event'}
**Target:** {channel_str} · {geo_str}
**Baseline:** {pre_from} → {pre_to} ({data.n_pre} days)
**Event window:** {post_from} → {post_to} ({data.n_post} days)
**Control series:** {controls_str}

| Metric | Value | 90% Credible Interval |
|--------|-------|-----------------------|
| Estimated Absolute Impact | {effect_sign}{result.cumulative_effect:,.1f} units | [{result.cumulative_effect_lower:,.1f}, {result.cumulative_effect_upper:,.1f}] |
| Average Daily Impact | {effect_sign}{result.cumulative_effect / max(data.n_post, 1):,.1f} units/day | — |
| Relative Growth | {rel_sign}{result.relative_effect_pct:.1f}% | — |
| Statistical Certainty Index | **{certainty:.1f}%** | — |
| Posterior Tail Probability | {result.posterior_tail_prob:.4f} | — |
| Significant? | {sig_emoji} {sig_label} | — |
| Convergence (R-hat max) | {convergence_str} | — |

**Interpretation:** {interpretation}

*Run ID: `{result.run_id}` · Model: {', '.join(result.model_components)} · {result.n_draws} draws × {result.n_chains} chains · {result.elapsed_seconds:.0f}s*
"""
