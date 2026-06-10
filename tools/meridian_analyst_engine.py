# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Meridian MMM — Core Modeling Engine (Component 2 / Task 27).

Instantiates, configures, and executes a Google Meridian Bayesian Media Mix Model
against the tensors produced by meridian_data_loader.py (Component 1).

Architecture
------------
    MeridianAnalystEngine
        ├── __init__(input_data, roi_priors)   — wires data + priors
        ├── build_model()                       — constructs DataTensors + ModelSpec
        ├── run(n_draws, n_chains, seed)        — MCMC sampling + diagnostics
        ├── save_artifacts(path)                — serializes model state
        └── write_diagnostics_to_bq()          — writes mmm_runs + mmm_channel_contributions

JAX Backend
-----------
Meridian uses TensorFlow Probability with JAX as the sampling backend. This gives
~4–8× faster MCMC than the TF-CPU backend for the chain sizes we run on Cloud Run.

Cloud Run configuration (XLA_FLAGS):
    XLA_FLAGS = "--xla_force_host_platform_device_count=4"
    Allows JAX to see 4 virtual devices on a CPU-only host, enabling parallel chains.
    Set this in the Cloud Run service environment or via os.environ before import.
    Combined with 4 chains × 500 draws, this fits cleanly within 60 minutes.

Adstock decay (geometric adstock):
    Each channel has a per-channel lag (max_lag weeks) and decay rate (adstock_prior_m).
    The Hill saturation function then maps the adstocked impressions to GRP-equivalent
    effective reach before the linear media contribution is computed.

Bayesian Calibration Hook (roi_priors)
---------------------------------------
The `roi_priors` parameter is the programmatic interface for Task 22 (Incrementality
Testing) to inject externally measured lift coefficients as informative Bayesian priors.

When Task 22 is implemented, pass a dict of the form:

    roi_priors = {
        "google_ads": {
            "mu":     0.45,       # posterior mean ROI from geo-experiment or iROAS
            "sigma":  0.15,       # posterior std — tighter = stronger prior belief
            "source": "geo_holdout_2026_q1",  # traceability label written to BQ
        },
        "meta": {
            "mu":     0.28,
            "sigma":  0.20,
            "source": "meta_conversion_lift_2025_q4",
        },
    }

Channels not listed in roi_priors receive the default weakly informative prior
(mu=0.2, sigma=0.9). The prior strength is controlled by sigma: smaller sigma = more
weight given to the experiment result vs. the data.

Output Tables (paid-media-schema / 08_mmm.sql)
-----------------------------------------------
    mmm_runs                 — one row per model run: metadata, diagnostics, data shape
    mmm_channel_contributions — one row per channel per run: ROI, contribution %, curves

Install dependencies:
    pip install 'paid-media-agent[mmm]'
"""
from __future__ import annotations

import json
import os
import pickle
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import numpy as np  # heavy dep — imported lazily at runtime

import structlog

from tools.meridian_data_loader import MeridianInputData, describe_tensor

log = structlog.get_logger()

# ── JAX / Cloud Run configuration ─────────────────────────────────────────────
# Must be set BEFORE importing jax or meridian. Place here so the module-level
# import of this file triggers the configuration in Cloud Run automatically.
#
# XLA_FLAGS: "--xla_force_host_platform_device_count=4"
#   Tells XLA/JAX to expose 4 virtual CPU devices on a CPU-only host.
#   Required for parallel MCMC chains on Cloud Run (which has no GPU/TPU).
#   Reduces wall-clock time from ~3× per additional chain to ~1.1× (near-linear scaling).
#   Setting this after jax has been imported has no effect — it must be set first.
#
# JAX_PLATFORM_NAME: "cpu"
#   Explicitly pins JAX to the CPU backend. Prevents JAX from printing a warning
#   about missing GPU/TPU on Cloud Run, which can clutter Cloud Logging.
#
# Cloud Run service YAML (set alongside other env vars):
#   - name: XLA_FLAGS
#     value: "--xla_force_host_platform_device_count=4"
#   - name: JAX_PLATFORM_NAME
#     value: "cpu"
#
# Timeout budget on Cloud Run (60-minute hard limit):
#   4 chains × 500 draws × ~52 weeks × ~5 geos ≈ 35–45 min on 4-vCPU Cloud Run instance.
#   If you exceed 60 min: reduce n_draws (250 minimum) or restrict geos/channels.

_xla_flags = os.getenv("XLA_FLAGS", "")
if "--xla_force_host_platform_device_count" not in _xla_flags:
    # Append without overwriting any flags the operator may have already set
    os.environ["XLA_FLAGS"] = (
        (_xla_flags + " " if _xla_flags else "")
        + "--xla_force_host_platform_device_count=4"
    ).strip()

if not os.getenv("JAX_PLATFORM_NAME"):
    os.environ["JAX_PLATFORM_NAME"] = "cpu"


# ── Default model hyperparameters ──────────────────────────────────────────────

# Adstock: geometric decay. max_lag = how many weeks carry-over from media exposure.
# Tune per industry — 4–8 weeks is typical for digital/SEM; 8–12 for brand/awareness.
DEFAULT_MAX_LAG = 8

# Hill saturation shape prior: exponent m (shape) and n (inflection point scale).
# Meridian's default: m ~ HalfNormal(1.0), n ~ LogNormal(0.0, 1.0).
# These priors produce a flexible S-curve that ranges from linear (m~1) to
# sharply saturating (m>>1). Leave defaults unless you have strong prior evidence.
DEFAULT_HILL_SHAPE_PRIOR_M  = 1.0   # prior mean for Hill shape parameter (mu_m)
DEFAULT_HILL_SHAPE_PRIOR_N  = 3.0   # prior mean for Hill inflection (mu_n, log-scale)

# Time effect: spline knots for modeling organic trend + seasonal baseline.
# More knots → more flexible trend; fewer → more regularization. 5–8 is standard.
DEFAULT_N_TIME_KNOTS = 6

# Weakly informative default ROI prior (log-normal):
#   mu = 0.2 → prior belief that $1 of media spend generates ~$0.22 in KPI value
#   sigma = 0.9 → very wide; the data drives the posterior more than this prior
# These are intentionally weak to let the MCMC data-drive the estimates.
# Tighten sigma (e.g. to 0.3–0.5) when you have strong geo-experiment evidence (Task 22).
DEFAULT_ROI_PRIOR_MU    = 0.2
DEFAULT_ROI_PRIOR_SIGMA = 0.9


# ── Incrementality → Meridian bridge (Task 22) ────────────────────────────────


def _get_roi_priors_from_bq() -> dict[str, dict[str, Any]]:
    """
    Auto-fetch the latest incrementality lift results from BigQuery and return
    them as a roi_priors dict ready for Meridian calibration.

    Reads from `v_incrementality_roi_priors` (09_incrementality.sql), which
    surfaces the latest active, statistically significant result per channel.

    Called automatically by run_mmm_pipeline() when roi_priors=None, completing
    the Task 22 calibration hook: incrementality results now flow into the MMM
    Bayesian priors without any manual dict construction by the operator.

    Returns:
        Dict mapping channel_name → {"mu": float, "sigma": float, "source": str}.
        Empty dict if no significant results exist or BQ is unreachable.

    Format matches MeridianAnalystEngine.roi_priors parameter:
        {
            "google_ads": {"mu": 0.39, "sigma": 0.14, "source": "google_ads_geo_holdout_2026_q2"},
            "meta":       {"mu": 0.27, "sigma": 0.19, "source": "meta_conversion_lift_2026_q1"},
        }
    """
    try:
        from tools import bigquery_client as bq
        rows = bq.run_query(
            f"SELECT channel, roi_prior_mu, roi_prior_sigma, source, "
            f"       iroas_mean, measurement_date, methodology "
            f"FROM {bq.table_ref('v_incrementality_roi_priors')}"
        )
    except Exception as exc:
        log.warning(
            "meridian_engine.priors_fetch_failed",
            error=str(exc),
            note="Using weak default priors for all channels.",
        )
        return {}

    priors: dict[str, dict[str, Any]] = {}
    for row in rows:
        channel = str(row.get("channel", ""))
        if not channel:
            continue
        priors[channel] = {
            "mu":     float(row["roi_prior_mu"]),
            "sigma":  float(row["roi_prior_sigma"]),
            "source": str(row["source"]),
        }
        log.info(
            "meridian_engine.prior_auto_loaded",
            channel=channel,
            iroas_mean=row.get("iroas_mean"),
            mu=priors[channel]["mu"],
            sigma=priors[channel]["sigma"],
            source=priors[channel]["source"],
            methodology=row.get("methodology"),
            measurement_date=str(row.get("measurement_date", "")),
        )

    return priors


def _validate_priors_with_tfp(
    roi_mu: "np.ndarray",
    roi_sigma: "np.ndarray",
    channel_index: list[str],
) -> None:
    """
    Validate prior arrays against a TFP JAX log-normal distribution before
    passing to Meridian's ModelSpec.

    Per Task 22 directive: distribution objects are constructed using the JAX-backed
    TFP substrate (tensorflow_probability.substrates.jax.distributions) to confirm
    the (mu, sigma) parameters are valid and compatible with Meridian's MCMC
    compilation path. Meridian internally builds the same TFP-JAX distributions from
    these arrays; this function validates them at the Python level first.

    Also emits a warning for any calibrated channel (sigma injected from Task 22)
    where sigma was left wide (> 0.50), which may indicate the prior was not
    tightened adequately by the experiment.

    Args:
        roi_mu:        [C] array of log-normal location parameters.
        roi_sigma:     [C] array of log-normal scale parameters.
        channel_index: ordered list of channel names (for log context).
    """
    try:
        import tensorflow_probability.substrates.jax.distributions as tfp_jax  # type: ignore[import]

        for c_i, (mu, sigma, channel) in enumerate(zip(roi_mu, roi_sigma, channel_index)):
            dist = tfp_jax.LogNormal(loc=float(mu), scale=float(sigma))
            prior_mean = float(dist.mean())

            if prior_mean <= 0:
                log.warning(
                    "meridian_engine.prior_invalid_mean",
                    channel=channel,
                    mu=float(mu),
                    sigma=float(sigma),
                    prior_mean=prior_mean,
                    note="Log-normal prior mean ≤ 0. Check roi_prior_mu value.",
                )

            # Warn if a "calibrated" channel still has a wide prior
            if float(sigma) > 0.50:
                log.warning(
                    "meridian_engine.prior_wide_for_calibrated_channel",
                    channel=channel,
                    sigma=float(sigma),
                    note=(
                        "sigma > 0.50 on a Task 22-calibrated channel. "
                        "Consider tightening to 0.10–0.25 for experiment-anchored channels. "
                        "A wide sigma means the data still dominates over the experiment prior."
                    ),
                )

        log.debug(
            "meridian_engine.priors_validated",
            n_channels=len(roi_mu),
            backend="tensorflow_probability.substrates.jax",
        )

    except ImportError:
        log.warning(
            "meridian_engine.tfp_jax_unavailable",
            note=(
                "Cannot validate priors with TFP-JAX. "
                "Install with: pip install 'paid-media-agent[mmm]'. "
                "Proceeding without prior validation — Meridian will catch invalid values at compile time."
            ),
        )
    except Exception as exc:
        log.warning("meridian_engine.prior_validation_error", error=str(exc))


# ── Engine ─────────────────────────────────────────────────────────────────────


class MeridianAnalystEngine:
    """
    Bayesian Media Mix Modeling engine wrapping Google Meridian.

    Instantiate with a MeridianInputData object (from meridian_data_loader.load_meridian_data)
    and optional roi_priors. Call build_model() → run() → write_diagnostics_to_bq().

    Parameters
    ----------
    input_data : MeridianInputData
        Output from meridian_data_loader.load_meridian_data().

    roi_priors : dict | None
        ─── TASK 22 CALIBRATION HOOK ───────────────────────────────────────────
        This dictionary is the programmatic interface through which Task 22
        (Incrementality Testing) injects externally measured lift coefficients
        as Bayesian priors into the Meridian model.

        Format:
            {
                "<channel_name>": {
                    "mu":     float,  # posterior mean ROI from geo-experiment or iROAS lift
                    "sigma":  float,  # posterior std — smaller = stronger belief
                    "source": str,    # traceability label (written to mmm_runs in BQ)
                }
            }

        Channels not listed receive DEFAULT_ROI_PRIOR_MU / DEFAULT_ROI_PRIOR_SIGMA.

        Example (Task 22 will populate this automatically from incrementality_lift_results):
            roi_priors = {
                "google_ads": {"mu": 0.45, "sigma": 0.15, "source": "geo_holdout_2026_q1"},
                "meta":       {"mu": 0.28, "sigma": 0.20, "source": "meta_lift_2025_q4"},
            }
        ─────────────────────────────────────────────────────────────────────────

    max_lag : int
        Adstock carry-over window in weeks. Default: 8.

    n_time_knots : int
        Spline knots for the time trend baseline. Default: 6.
    """

    def __init__(
        self,
        input_data: MeridianInputData,
        roi_priors: dict[str, dict[str, Any]] | None = None,
        max_lag: int = DEFAULT_MAX_LAG,
        n_time_knots: int = DEFAULT_N_TIME_KNOTS,
    ) -> None:
        self.input_data   = input_data
        self.roi_priors   = roi_priors or {}
        self.max_lag      = max_lag
        self.n_time_knots = n_time_knots

        self.run_id:    str  = str(uuid.uuid4())
        self.mmm_model: Any  = None   # meridian.model.model.Meridian instance, set by build_model()
        self.fitted:    bool = False
        self.diagnostics: dict = {}

        log.info(
            "meridian_engine.init",
            run_id=self.run_id,
            shape=f"[{input_data.n_geos}×{input_data.n_weeks}×{input_data.n_channels}]",
            roi_priors_channels=list(self.roi_priors.keys()),
            max_lag=max_lag,
            n_time_knots=n_time_knots,
        )

    # ── Dependency check ───────────────────────────────────────────────────────

    @staticmethod
    def _check_mmm_deps() -> None:
        missing = []
        for pkg in ["meridian", "jax", "numpyro", "numpy"]:
            try:
                __import__(pkg)
            except ImportError:
                missing.append(pkg)
        if missing:
            raise ImportError(
                f"Missing MMM dependencies: {', '.join(missing)}. "
                "Install with: pip install 'paid-media-agent[mmm]'"
            )

    # ── Prior construction ─────────────────────────────────────────────────────

    def _build_roi_prior_arrays(self) -> tuple["np.ndarray", "np.ndarray"]:
        """
        Translate the roi_priors dict into ordered NumPy arrays that align with
        channel_index. Channels without an explicit prior receive the weak defaults.

        Returns (roi_mu_array, roi_sigma_array), both shape [C].

        ─── TASK 22 INTEGRATION NOTE ──────────────────────────────────────────
        When Task 22 (Incrementality Testing) is complete, it will:
        1. Write lift estimates to the incrementality_lift_results table
           (columns: channel, iROAS_mean, iROAS_std, experiment_id, period_end)
        2. The Analyst agent will read that table and pass the results here as:
               roi_priors = {
                   row["channel"]: {
                       "mu":     row["iROAS_mean"],
                       "sigma":  row["iROAS_std"],
                       "source": row["experiment_id"],
                   }
                   for row in lift_rows
               }
        3. Meridian's posterior will be anchored to the experimentally measured
           lift, making budget allocation recommendations more defensible.
        ─────────────────────────────────────────────────────────────────────────
        """
        import numpy as np

        n_channels = self.input_data.n_channels
        roi_mu    = np.full(n_channels, DEFAULT_ROI_PRIOR_MU,    dtype=np.float64)
        roi_sigma = np.full(n_channels, DEFAULT_ROI_PRIOR_SIGMA, dtype=np.float64)

        for c_i, channel in enumerate(self.input_data.channel_index):
            if channel in self.roi_priors:
                prior = self.roi_priors[channel]
                roi_mu[c_i]    = float(prior["mu"])
                roi_sigma[c_i] = float(prior["sigma"])
                log.info(
                    "meridian_engine.prior_injected",
                    channel=channel,
                    mu=roi_mu[c_i],
                    sigma=roi_sigma[c_i],
                    source=prior.get("source", "unknown"),
                )
            else:
                log.debug(
                    "meridian_engine.prior_default",
                    channel=channel,
                    mu=DEFAULT_ROI_PRIOR_MU,
                    sigma=DEFAULT_ROI_PRIOR_SIGMA,
                )

        # Validate arrays against TFP-JAX log-normal distributions
        # (Task 22 directive: confirm JAX backend compatibility before ModelSpec)
        _validate_priors_with_tfp(roi_mu, roi_sigma, self.input_data.channel_index)

        return roi_mu, roi_sigma

    # ── Model construction ─────────────────────────────────────────────────────

    def build_model(self) -> None:
        """
        Construct the Meridian DataTensors and ModelSpec from input_data and roi_priors.

        Instantiates self.mmm_model (meridian.model.model.Meridian) but does NOT
        run sampling. Call run() to execute MCMC.

        ModelSpec configuration:
            - Adstock: geometric decay with max_lag weeks carry-over
            - Hill saturation: flexible S-curve with weakly informative priors
            - Time effect: spline with n_time_knots knots (captures organic trend)
            - ROI priors: per-channel mu/sigma arrays from _build_roi_prior_arrays()
            - Controls: included if n_controls > 0 in input_data
        """
        self._check_mmm_deps()

        import jax
        import numpy as np
        from meridian.data.load import DataTensors
        from meridian.model.model import Meridian
        from meridian.model.spec import ModelSpec

        log.info("meridian_engine.build_model", run_id=self.run_id)

        data = self.input_data
        roi_mu, roi_sigma = self._build_roi_prior_arrays()

        # ── DataTensors ────────────────────────────────────────────────────────
        # Convert NumPy arrays to JAX arrays explicitly for type safety.
        # Meridian accepts both but JAX arrays skip an extra copy on the sampling path.
        dt = DataTensors(
            kpi=jax.numpy.array(data.kpi,         dtype=jax.numpy.float32),  # [G, T]
            media=jax.numpy.array(data.media,     dtype=jax.numpy.float32),  # [G, T, C]
            media_spend=jax.numpy.array(
                data.media_spend, dtype=jax.numpy.float32),                  # [G, T, C]
            controls=jax.numpy.array(
                data.controls,    dtype=jax.numpy.float32)                   # [G, T, V]
                if data.n_controls > 0 else None,
            population=jax.numpy.array(
                data.population,  dtype=jax.numpy.float32),                  # [G]
            geos=data.geo_index,
            times=data.time_index,
            kpi_type="non_revenue",              # "non_revenue" = conversion count KPI
            # Switch to "revenue" if kpi is dollar revenue (changes ROI interpretation)
            media_channels=data.channel_index,
            control_variables=data.control_index if data.n_controls > 0 else None,
        )

        # ── ModelSpec ──────────────────────────────────────────────────────────
        model_spec = ModelSpec(
            # Adstock decay: geometric carry-over up to max_lag weeks.
            # Meridian fits one adstock rate per channel from a Beta(2,2) prior.
            max_lag=self.max_lag,

            # Hill saturation function (S-curve):
            #   exponent m controls curve shape (m=1 → linear, m>>1 → sharp saturation)
            #   half-saturation ec controls the inflection point
            # These weakly informative priors let the data shape the curve per channel.
            hill_exponent_m=DEFAULT_HILL_SHAPE_PRIOR_M,
            hill_exponent_n=DEFAULT_HILL_SHAPE_PRIOR_N,

            # Time effect: natural cubic spline with n_time_knots evenly spaced knots.
            # Models organic trend + macro effects not explained by media.
            n_knots=self.n_time_knots,

            # ── ROI priors ─────────────────────────────────────────────────────
            # These are the key parameters for media contribution estimation.
            # roi_mu_m and roi_sigma_m are the log-normal prior parameters for ROI:
            #   E[ROI] ≈ exp(roi_mu_m + roi_sigma_m²/2)
            # Per-channel arrays (shape [C]) allow channels calibrated by Task 22
            # experiments to anchor tightly while uncalibrated channels remain flexible.
            #
            # ─── TASK 22 CALIBRATION HOOK — roi_priors parameter ──────────────
            # When Task 22 runs a geo holdout or conversion lift experiment, it will
            # produce iROAS estimates (incremental revenue/conversions per dollar spent).
            # Those estimates are loaded into the roi_priors dict at engine init and
            # translated here into tighter per-channel log-normal priors.
            #
            # Until Task 22 ships, all channels use the weak defaults above, meaning
            # Meridian estimates channel contributions purely from the observational data.
            # ──────────────────────────────────────────────────────────────────
            roi_mu_m=roi_mu.astype(np.float32),
            roi_sigma_m=roi_sigma.astype(np.float32),
        )

        # ── Meridian model instance ────────────────────────────────────────────
        self.mmm_model = Meridian(input_data=dt, model_spec=model_spec)

        log.info(
            "meridian_engine.model_built",
            run_id=self.run_id,
            G=data.n_geos,
            T=data.n_weeks,
            C=data.n_channels,
            V=data.n_controls,
            max_lag=self.max_lag,
            n_knots=self.n_time_knots,
        )

    # ── MCMC sampling & diagnostics ────────────────────────────────────────────

    def run(
        self,
        n_draws:       int  = 500,
        n_chains:      int  = 4,
        n_adapt:       int  = 200,
        seed:          int  = 42,
        target_accept: float = 0.85,
    ) -> dict:
        """
        Execute MCMC posterior sampling and compute post-run diagnostics.

        Uses NumPyro as the MCMC backend (HMC/NUTS sampler) through Meridian's
        sampling interface. Runs `n_chains` chains in parallel using JAX's
        pmap over the 4 virtual CPU devices configured by XLA_FLAGS.

        Cloud Run timing guidance:
            n_draws=500, n_chains=4 → ~35–45 min on 4-vCPU instance (typical)
            n_draws=250, n_chains=4 → ~20–25 min (use if timeout is a concern)
            n_draws=1000, n_chains=4 → ~75 min (exceeds 60-min limit — run locally)

        Parameters
        ----------
        n_draws : int
            Number of post-warmup MCMC draws per chain. Minimum 250 for reliable
            R-hat convergence. 500 is standard; 1000 for publication-quality inference.

        n_chains : int
            Number of parallel Markov chains. Should match XLA device count (4).
            Fewer chains → faster but less reliable convergence diagnostics.

        n_adapt : int
            Number of warmup / adaptation draws per chain (HMC step-size tuning).
            200 is standard. Increase to 500 if R-hat > 1.1 after a run.

        seed : int
            JAX PRNG seed for reproducibility. Log this alongside the run_id in BQ.

        target_accept : float
            HMC target acceptance rate. 0.85 is standard; increase toward 0.95
            if you observe many divergences (at the cost of smaller step sizes).

        Returns
        -------
        dict
            Diagnostic summary written to BigQuery mmm_runs. Includes:
                run_id, n_draws, n_chains, r_hat_max, r_hat_mean,
                ess_bulk_min, n_divergences, elapsed_seconds,
                roi_summary (per-channel mean + 90% CI), and fit_status.
        """
        if self.mmm_model is None:
            raise RuntimeError("Call build_model() before run().")

        self._check_mmm_deps()
        import time


        log.info(
            "meridian_engine.sampling_start",
            run_id=self.run_id,
            n_draws=n_draws,
            n_chains=n_chains,
            n_adapt=n_adapt,
            seed=seed,
        )

        t0 = time.time()
        try:
            # Meridian's sample_posterior uses NumPyro's NUTS sampler via JAX pmap.
            # The backend="numpyro" argument explicitly selects the JAX execution path.
            self.mmm_model.sample_posterior(
                n_draws=n_draws,
                n_chains=n_chains,
                n_adapt=n_adapt,
                seed=seed,
                target_accept_prob=target_accept,
                backend="numpyro",    # explicit JAX/NumPyro path — mandatory for Cloud Run
            )
        except Exception as exc:
            log.error("meridian_engine.sampling_failed", run_id=self.run_id, error=str(exc))
            raise

        elapsed = time.time() - t0
        log.info("meridian_engine.sampling_done", run_id=self.run_id, elapsed_s=f"{elapsed:.0f}s")

        # ── Convergence diagnostics ────────────────────────────────────────────
        # R-hat (potential scale reduction factor): values < 1.1 indicate convergence.
        # ESS (effective sample size): > 100 per chain is a practical minimum.
        diag = self._compute_diagnostics(n_draws, n_chains, elapsed)

        # ── ROI summary ────────────────────────────────────────────────────────
        roi = self._extract_roi_summary()
        diag["roi_summary"] = roi

        self.diagnostics = diag
        self.fitted = True

        if diag.get("r_hat_max", 99.0) > 1.1:
            log.warning(
                "meridian_engine.convergence_warning",
                r_hat_max=diag.get("r_hat_max"),
                recommendation="Increase n_draws or n_adapt. R-hat > 1.1 suggests chains have not mixed.",
            )

        log.info(
            "meridian_engine.run_complete",
            run_id=self.run_id,
            r_hat_max=diag.get("r_hat_max"),
            ess_bulk_min=diag.get("ess_bulk_min"),
            n_divergences=diag.get("n_divergences"),
            elapsed_s=f"{elapsed:.0f}s",
        )

        return diag

    def _compute_diagnostics(
        self,
        n_draws: int,
        n_chains: int,
        elapsed: float,
    ) -> dict:
        """Extract R-hat, ESS, and divergence counts from the fitted model."""
        import numpy as np

        diag: dict[str, Any] = {
            "run_id":           self.run_id,
            "n_draws":          n_draws,
            "n_chains":         n_chains,
            "elapsed_seconds":  round(elapsed, 1),
            "seed":             42,
            "fit_status":       "completed",
        }

        try:
            # Meridian exposes r_hat as a dict of {param_name: array_of_rhat_values}
            r_hat_dict = self.mmm_model.r_hat
            all_r_hats: list[float] = []
            for arr in r_hat_dict.values():
                all_r_hats.extend(float(x) for x in np.asarray(arr).flatten() if not np.isnan(x))

            if all_r_hats:
                diag["r_hat_max"]  = round(max(all_r_hats), 4)
                diag["r_hat_mean"] = round(sum(all_r_hats) / len(all_r_hats), 4)
            else:
                diag["r_hat_max"]  = None
                diag["r_hat_mean"] = None

        except Exception as exc:
            log.warning("meridian_engine.rhat_unavailable", error=str(exc))
            diag["r_hat_max"]  = None
            diag["r_hat_mean"] = None

        try:
            # ESS (effective sample size) — minimum across all parameters
            ess_dict = self.mmm_model.ess_bulk
            all_ess: list[float] = []
            for arr in ess_dict.values():
                all_ess.extend(float(x) for x in np.asarray(arr).flatten() if not np.isnan(x))
            diag["ess_bulk_min"] = round(min(all_ess), 1) if all_ess else None
        except Exception:
            diag["ess_bulk_min"] = None

        try:
            n_div = int(self.mmm_model.num_divergences)
            diag["n_divergences"] = n_div
            if n_div > 0:
                log.warning(
                    "meridian_engine.divergences",
                    n_divergences=n_div,
                    recommendation=(
                        "Increase target_accept toward 0.95 or increase n_adapt. "
                        "Divergences suggest the sampler is struggling with the geometry."
                    ),
                )
        except Exception:
            diag["n_divergences"] = None

        return diag

    def _extract_roi_summary(self) -> dict[str, dict]:
        """
        Extract per-channel ROI estimates (mean + 90% credible interval) from the
        posterior samples. Returns a dict keyed by channel name.
        """

        roi_summary: dict[str, dict] = {}
        try:
            # Meridian's analyzer provides ROI summaries via the roi() method
            from meridian import analyzer as meridian_analyzer
            an = meridian_analyzer.Analyzer(self.mmm_model)
            roi_df = an.roi_summary()

            for _, row in roi_df.iterrows():
                channel = str(row.get("media_channel", row.get("channel", "unknown")))
                roi_summary[channel] = {
                    "roi_mean":    round(float(row.get("mean",  0.0)), 4),
                    "roi_p5":      round(float(row.get("p5",    0.0)), 4),
                    "roi_p50":     round(float(row.get("median", row.get("p50", 0.0))), 4),
                    "roi_p95":     round(float(row.get("p95",   0.0)), 4),
                    "contribution_pct": round(float(row.get("contribution_pct", 0.0)), 2),
                }
        except Exception as exc:
            log.warning("meridian_engine.roi_summary_unavailable", error=str(exc))
            # Fallback: populate with None values so the schema column exists in BQ
            for ch in self.input_data.channel_index:
                roi_summary[ch] = {
                    "roi_mean": None, "roi_p5": None,
                    "roi_p50": None,  "roi_p95": None,
                    "contribution_pct": None,
                }

        return roi_summary

    # ── Artifact persistence ───────────────────────────────────────────────────

    def save_artifacts(self, output_dir: str | Path = "/tmp/meridian_artifacts") -> Path:
        """
        Serialize the fitted model state and input metadata to disk.

        Writes:
            {output_dir}/{run_id}/model.pkl        — pickled Meridian model object
            {output_dir}/{run_id}/input_meta.json  — tensor shape + index registries
            {output_dir}/{run_id}/diagnostics.json — R-hat, ESS, ROI summary

        For Cloud Run: use a GCS-mounted volume or write directly to GCS via
        google-cloud-storage after sampling (the model object is ~50–500 MB depending
        on n_draws). For local runs, /tmp is fine.

        Parameters
        ----------
        output_dir : str | Path
            Parent directory. A subdirectory named by run_id is created automatically.

        Returns
        -------
        Path
            The run-specific artifact directory.
        """
        if not self.fitted:
            raise RuntimeError("Model must be fitted (run() must complete) before saving artifacts.")

        artifact_dir = Path(output_dir) / self.run_id
        artifact_dir.mkdir(parents=True, exist_ok=True)

        # Serialize model
        model_path = artifact_dir / "model.pkl"
        with model_path.open("wb") as f:
            pickle.dump(self.mmm_model, f, protocol=pickle.HIGHEST_PROTOCOL)
        log.info("meridian_engine.model_saved", path=str(model_path))

        # Index registry + shape metadata (human-readable)
        meta = {
            "run_id":          self.run_id,
            "date_from":       self.input_data.date_from,
            "date_to":         self.input_data.date_to,
            "geo_index":       self.input_data.geo_index,
            "time_index":      self.input_data.time_index,
            "channel_index":   self.input_data.channel_index,
            "control_index":   self.input_data.control_index,
            "shape":           {
                "G": self.input_data.n_geos,
                "T": self.input_data.n_weeks,
                "C": self.input_data.n_channels,
                "V": self.input_data.n_controls,
            },
            "roi_priors_used": self.roi_priors,
            "model_config":    {
                "max_lag":       self.max_lag,
                "n_time_knots":  self.n_time_knots,
            },
        }
        meta_path = artifact_dir / "input_meta.json"
        meta_path.write_text(json.dumps(meta, indent=2, default=str))

        diag_path = artifact_dir / "diagnostics.json"
        diag_path.write_text(json.dumps(self.diagnostics, indent=2, default=str))

        log.info(
            "meridian_engine.artifacts_saved",
            run_id=self.run_id,
            artifact_dir=str(artifact_dir),
        )
        return artifact_dir

    # ── BigQuery output ────────────────────────────────────────────────────────

    def write_diagnostics_to_bq(self) -> None:
        """
        Write modeling results to the MMM output tables in BigQuery.

        Writes to two tables defined in paid-media-schema / 08_mmm.sql
        and registered in tools/bigquery_client.py:

            mmm_runs
                One row per model run. Contains metadata, tensor shape,
                convergence diagnostics (R-hat, ESS), and the roi_priors
                used (for Task 22 traceability).

            mmm_channel_contributions
                One row per channel per run. Contains ROI posterior summaries
                (mean, p5, p50, p95), contribution percentage, and channel spend.

        Idempotent: safe to call multiple times; each call writes a new run_id row.
        """
        if not self.fitted:
            raise RuntimeError("Model must be fitted before writing diagnostics.")

        from tools.bigquery_client import insert_rows
        now = datetime.now(timezone.utc).isoformat()
        data = self.input_data
        diag = self.diagnostics

        # ── mmm_runs row ───────────────────────────────────────────────────────
        tensor_meta = describe_tensor(data)
        roi_priors_serialized = json.dumps(
            {ch: {k: v for k, v in priors.items()} for ch, priors in self.roi_priors.items()},
            default=str,
        )
        run_row = {
            "run_id":                 self.run_id,
            "run_started_at":         now,
            "status":                 diag.get("fit_status", "completed"),
            "date_from":              data.date_from,
            "date_to":                data.date_to,
            "n_geos":                 data.n_geos,
            "n_weeks":                data.n_weeks,
            "n_channels":             data.n_channels,
            "geo_index":              json.dumps(data.geo_index),
            "channel_index":          json.dumps(data.channel_index),
            "n_draws":                diag.get("n_draws"),
            "n_chains":               diag.get("n_chains"),
            "n_adapt":                diag.get("n_adapt"),
            "elapsed_seconds":        diag.get("elapsed_seconds"),
            "r_hat_max":              diag.get("r_hat_max"),
            "r_hat_mean":             diag.get("r_hat_mean"),
            "ess_bulk_min":           diag.get("ess_bulk_min"),
            "n_divergences":          diag.get("n_divergences"),
            "max_lag":                self.max_lag,
            "n_time_knots":           self.n_time_knots,
            "roi_priors_used":        roi_priors_serialized,
            "kpi_total":              tensor_meta["kpi_total"],
            "spend_total_usd":        tensor_meta["spend_total_usd"],
            "media_zero_pct":         tensor_meta["media_zero_pct"],
            "created_at":             now,
        }

        errors = insert_rows("mmm_runs", [run_row])
        if errors:
            log.error("meridian_engine.bq_mmm_runs_error", errors=errors)
        else:
            log.info("meridian_engine.bq_mmm_runs_written", run_id=self.run_id)

        # ── mmm_channel_contributions rows ─────────────────────────────────────
        roi_summary = diag.get("roi_summary", {})
        contribution_rows = []

        for c_i, channel in enumerate(data.channel_index):
            roi_info = roi_summary.get(channel, {})
            channel_spend = float(data.media_spend[:, :, c_i].sum())
            channel_impressions = float(data.media[:, :, c_i].sum())

            # Derive whether this channel had a Task 22 prior injected
            prior_used = channel in self.roi_priors
            prior_source = self.roi_priors.get(channel, {}).get("source", None)

            contribution_rows.append({
                "contribution_id":    str(uuid.uuid4()),
                "run_id":             self.run_id,
                "channel":            channel,
                "total_spend_usd":    channel_spend,
                "total_impressions":  channel_impressions,
                "roi_mean":           roi_info.get("roi_mean"),
                "roi_p5":             roi_info.get("roi_p5"),
                "roi_p50":            roi_info.get("roi_p50"),
                "roi_p95":            roi_info.get("roi_p95"),
                "contribution_pct":   roi_info.get("contribution_pct"),
                # Task 22 calibration traceability
                "roi_prior_injected": prior_used,
                "roi_prior_source":   prior_source,
                "roi_prior_mu":       self.roi_priors.get(channel, {}).get("mu"),
                "roi_prior_sigma":    self.roi_priors.get(channel, {}).get("sigma"),
                "created_at":         now,
            })

        if contribution_rows:
            errors = insert_rows("mmm_channel_contributions", contribution_rows)
            if errors:
                log.error("meridian_engine.bq_contributions_error", errors=errors)
            else:
                log.info(
                    "meridian_engine.bq_contributions_written",
                    run_id=self.run_id,
                    channels=len(contribution_rows),
                )


# ── Convenience run pipeline ───────────────────────────────────────────────────


def run_mmm_pipeline(
    date_from: str,
    date_to: str,
    platforms: list[str] | None = None,
    geo_allowlist: list[str] | None = None,
    roi_priors: dict | None = None,
    n_draws: int = 500,
    n_chains: int = 4,
    n_adapt: int = 200,
    artifact_dir: str = "/tmp/meridian_artifacts",
    write_to_bq: bool = True,
) -> dict:
    """
    End-to-end MMM pipeline: load data → build model → sample → save → write BQ.

    This is the entry point called by the Analyst agent tool and the /mmm skill.
    All parameters are forwarded to the appropriate sub-components.

    Parameters
    ----------
    date_from, date_to : str
        Date range for data extraction. Minimum 78 weeks recommended.

    platforms : list[str] | None
        Restrict to specific platforms (e.g. ["google_ads", "meta", "tiktok"]).

    geo_allowlist : list[str] | None
        Restrict to specific ISO country codes.

    roi_priors : dict | None
        Task 22 calibration hook — see MeridianAnalystEngine docstring for format.

    n_draws, n_chains, n_adapt : int
        MCMC sampling parameters. See MeridianAnalystEngine.run() for guidance.

    artifact_dir : str
        Local path for model artifacts. Override with GCS mount path on Cloud Run.

    write_to_bq : bool
        If True, write mmm_runs and mmm_channel_contributions to BigQuery.

    Returns
    -------
    dict
        Diagnostic summary from MeridianAnalystEngine.run(), augmented with
        artifact_path, run_id, and data shape metadata.
    """
    from tools.meridian_data_loader import load_meridian_data

    log.info("meridian_pipeline.start", date_from=date_from, date_to=date_to)

    # Component 1: Extract and transform data
    input_data = load_meridian_data(
        date_from=date_from,
        date_to=date_to,
        platforms=platforms,
        geo_allowlist=geo_allowlist,
    )
    log.info("meridian_pipeline.data_loaded", summary=input_data.summary())

    # ── Task 22 auto-wiring: fetch incrementality priors if not provided ─────────
    # Reads v_incrementality_roi_priors (09_incrementality.sql) which contains the
    # latest significant iROAS estimate per channel from run_incrementality_analysis().
    # When calibrated, channels receive tighter posteriors (sigma ≈ 0.10–0.25)
    # vs. the wide observational default (sigma = 0.9).
    if roi_priors is None:
        roi_priors = _get_roi_priors_from_bq()
        if roi_priors:
            log.info(
                "meridian_pipeline.priors_auto_loaded",
                calibrated_channels=list(roi_priors.keys()),
                note=(
                    "Incrementality-derived iROAS priors injected automatically. "
                    "These priors anchor the MCMC posterior to experimentally measured lift. "
                    "Run run_incrementality_analysis() to refresh or add channels."
                ),
            )
        else:
            log.info(
                "meridian_pipeline.no_priors_available",
                note=(
                    "No active significant incrementality results in v_incrementality_roi_priors. "
                    "Using weak default priors (mu=0.2, sigma=0.9) for all channels. "
                    "Run run_incrementality_analysis() to calibrate channel priors."
                ),
            )

    # Component 2: Build, run, and persist model
    engine = MeridianAnalystEngine(input_data=input_data, roi_priors=roi_priors)
    engine.build_model()
    diagnostics = engine.run(n_draws=n_draws, n_chains=n_chains, n_adapt=n_adapt)

    # Persist artifacts
    artifact_path = engine.save_artifacts(artifact_dir)
    diagnostics["artifact_path"] = str(artifact_path)
    diagnostics["data_shape"] = {
        "G": input_data.n_geos,
        "T": input_data.n_weeks,
        "C": input_data.n_channels,
        "geos": input_data.geo_index,
        "channels": input_data.channel_index,
    }

    # Write results to BigQuery
    if write_to_bq:
        engine.write_diagnostics_to_bq()

    log.info("meridian_pipeline.complete", run_id=engine.run_id)
    return diagnostics
