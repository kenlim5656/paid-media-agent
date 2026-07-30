#!/usr/bin/env python3
# Copyright 2026 @kenlim5656. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/kenlim5656/paid-media-suite

"""
tools/generate_sandbox_data.py — Synthetic Sandbox Data Generator

Synthesizes a cohesive 90-day B2B enterprise dataset and streams it into BigQuery.
All dates are anchored backward from today so rolling windows are fully populated.

Three forensic anomaly traps are injected for agent validation:

  Trap A — Salesforce Overwrite Loop (Task 37)
    150 leads where a paid click token (gclid / fbclid) exists on the analytics
    session, but the CRM LeadSource is overwritten to an offline label
    ("Content Syndication Bulk Upload", "Offline Partner Webinar", etc.).
    Session timestamp is 5 days before CRM creation date, simulating the
    retroactive database overwrite pattern.

  Trap B — Stealth Offline Demand Surge (Task 24 / BSTS)
    A 5-day window starting 45 days ago receives a 400% spike in organic/direct
    web sessions and corresponding Closed-Won revenue in US geo. Paid media
    spend remains completely flat during this window, creating a clean
    counterfactual for the BSTS causal impact model.

  Trap C — ICP Vertical Shift (Task 35 / Lookalike Mutation)
    In the last 30 days, Logistics & Supply Chain companies with ARR > $120K
    represent 45% of total closed revenue (up from a ~10% baseline). This
    guarantees the v_lookalike_mutation_seed view detects clear firmographic
    drift and returns a heavy over-index score.

Usage:
    python tools/generate_sandbox_data.py                    # 90-day default
    python tools/generate_sandbox_data.py --days 60          # custom window
    python tools/generate_sandbox_data.py --wipe-clean       # truncate tables first
    python tools/generate_sandbox_data.py --days 90 --wipe-clean

Required:
    GCP credentials with BigQuery write access (Application Default or GOOGLE_APPLICATION_CREDENTIALS).
    Tables must already exist — run 'python schemas/bigquery_tables.py' first if needed.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import structlog

from tools import bigquery_client as bq

log = structlog.get_logger()

# ══════════════════════════════════════════════════════════════════════════════
# ── Global constants
# ══════════════════════════════════════════════════════════════════════════════

RANDOM_SEED  = 42
BATCH_SIZE   = 500        # rows per streaming insert batch

# ── Trap timing ───────────────────────────────────────────────────────────────
TRAP_A_COUNT        = 150   # overwritten attribution leads to inject
TRAP_B_DAYS_AGO     = 45    # surge window starts N days before today
TRAP_B_DURATION     = 5     # duration in days
TRAP_B_MULTIPLIER   = 5.0   # 5× baseline = 400% increase above normal
TRAP_C_WINDOW       = 30    # logistics over-index spans the last N days
TRAP_C_SHARE        = 0.45  # target closed-revenue share for logistics

# ── Offline lead sources used in Trap A ──────────────────────────────────────
_TRAP_A_LEAD_SOURCES = [
    "Content Syndication Bulk Upload",
    "Offline Partner Webinar",
    "Trade Show Badge Scan",
    "Inbound Direct — Unknown",
]

# ── Pipeline stages ───────────────────────────────────────────────────────────
_PIPELINE_STAGES = [
    "prospect", "qualified", "sql", "opportunity",
    "proposal", "negotiation", "closed_won", "closed_lost",
]

# ══════════════════════════════════════════════════════════════════════════════
# ── Platform configuration
# ══════════════════════════════════════════════════════════════════════════════

_PLATFORM_CFG: dict[str, dict] = {
    "google_ads": {
        "base_spend": 950, "spend_std": 110,
        "cpm": 14.50, "ctr": 0.068, "cvr": 0.085,
        "roi_mean": 2.85, "roi_p5": 1.80, "roi_p50": 2.80, "roi_p95": 4.10,
        "contribution_pct": 0.34,
    },
    "meta": {
        "base_spend": 650, "spend_std": 85,
        "cpm": 9.80, "ctr": 0.021, "cvr": 0.028,
        "roi_mean": 2.20, "roi_p5": 1.20, "roi_p50": 2.15, "roi_p95": 3.50,
        "contribution_pct": 0.24,
    },
    "linkedin": {
        "base_spend": 480, "spend_std": 65,
        "cpm": 28.50, "ctr": 0.016, "cvr": 0.045,
        "roi_mean": 3.10, "roi_p5": 1.90, "roi_p50": 3.05, "roi_p95": 4.80,
        "contribution_pct": 0.22,
    },
    "tiktok": {
        "base_spend": 290, "spend_std": 50,
        "cpm": 8.20, "ctr": 0.019, "cvr": 0.018,
        "roi_mean": 1.85, "roi_p5": 0.90, "roi_p50": 1.80, "roi_p95": 3.20,
        "contribution_pct": 0.12,
    },
    "reddit_ads": {
        "base_spend": 145, "spend_std": 28,
        "cpm": 7.40, "ctr": 0.013, "cvr": 0.022,
        "roi_mean": 1.65, "roi_p5": 0.80, "roi_p50": 1.60, "roi_p95": 2.90,
        "contribution_pct": 0.08,
    },
}

# ── Campaign definitions ──────────────────────────────────────────────────────
_CAMPAIGNS: list[dict] = [
    # Google Ads
    {"platform": "google_ads", "campaign_id": "ga_brand_001",    "campaign_name": "Google — Brand Search",           "objective": "CONVERSIONS",         "spend_weight": 0.35},
    {"platform": "google_ads", "campaign_id": "ga_nonbrand_001", "campaign_name": "Google — Non-Brand Search",        "objective": "CONVERSIONS",         "spend_weight": 0.30},
    {"platform": "google_ads", "campaign_id": "ga_pmax_001",     "campaign_name": "Google — Performance Max",         "objective": "CONVERSIONS",         "spend_weight": 0.20},
    {"platform": "google_ads", "campaign_id": "ga_display_001",  "campaign_name": "Google — Display Retargeting",     "objective": "AWARENESS",           "spend_weight": 0.15},
    # Meta
    {"platform": "meta",       "campaign_id": "meta_tof_001",    "campaign_name": "Meta — TOF Prospecting Video",     "objective": "AWARENESS",           "spend_weight": 0.30},
    {"platform": "meta",       "campaign_id": "meta_rtg_001",    "campaign_name": "Meta — Retargeting Conversions",   "objective": "CONVERSIONS",         "spend_weight": 0.35},
    {"platform": "meta",       "campaign_id": "meta_lla_001",    "campaign_name": "Meta — Lookalike 2% Audience",     "objective": "LEAD_GENERATION",     "spend_weight": 0.20},
    {"platform": "meta",       "campaign_id": "meta_comp_001",   "campaign_name": "Meta — Competitor Retargeting",    "objective": "CONVERSIONS",         "spend_weight": 0.15},
    # LinkedIn
    {"platform": "linkedin",   "campaign_id": "li_sc_001",       "campaign_name": "LinkedIn — Sponsored Content TOF", "objective": "BRAND_AWARENESS",     "spend_weight": 0.40},
    {"platform": "linkedin",   "campaign_id": "li_demo_001",     "campaign_name": "LinkedIn — Demo Request",          "objective": "WEBSITE_CONVERSIONS", "spend_weight": 0.35},
    {"platform": "linkedin",   "campaign_id": "li_inmail_001",   "campaign_name": "LinkedIn — InMail Outreach",       "objective": "LEAD_GENERATION",     "spend_weight": 0.25},
    # TikTok
    {"platform": "tiktok",     "campaign_id": "tt_aware_001",    "campaign_name": "TikTok — Brand Awareness",         "objective": "REACH",               "spend_weight": 0.55},
    {"platform": "tiktok",     "campaign_id": "tt_lead_001",     "campaign_name": "TikTok — Lead Generation",         "objective": "LEAD_GENERATION",     "spend_weight": 0.45},
    # Reddit Ads
    {"platform": "reddit_ads", "campaign_id": "rd_devops_001",   "campaign_name": "Reddit — r/devops Community",      "objective": "TRAFFIC",             "spend_weight": 0.40},
    {"platform": "reddit_ads", "campaign_id": "rd_rtg_001",      "campaign_name": "Reddit — Site Retargeting",        "objective": "CONVERSIONS",         "spend_weight": 0.35},
    {"platform": "reddit_ads", "campaign_id": "rd_comp_001",     "campaign_name": "Reddit — Competitor Community",    "objective": "AWARENESS",           "spend_weight": 0.25},
]

# ── Geographic distribution (ISO-2 + weight) ─────────────────────────────────
_GEO_POOL = [
    ("US", 0.55), ("CA", 0.08), ("GB", 0.10), ("DE", 0.07),
    ("AU", 0.05), ("FR", 0.04), ("NL", 0.03), ("SG", 0.03),
    ("IN", 0.03), ("IE", 0.02),
]
_GEO_CODES = [g[0] for g in _GEO_POOL]
_GEO_PROBS = [g[1] for g in _GEO_POOL]

# ── Company fixtures (50 fictional B2B companies) ─────────────────────────────
# Fields: name, domain, industry, sub, employees, hq, type, icp, arr, target, tier
#
# Indices 13–19 are the Logistics & Supply Chain companies targeted by Trap C.
_COMPANIES_RAW: list[dict] = [
    # ── Tier 1 target accounts ────────────────────────────────────────────────
    {"name": "Axion Software",       "domain": "axionsoftware.io",       "industry": "Software & SaaS",          "sub": "DevOps",                   "employees": "200-1000", "hq": "US", "type": "Private",   "icp": 92, "arr": 340000,  "target": True,  "tier": "tier1"},
    {"name": "Meridian Finance",     "domain": "meridianfinance.com",    "industry": "Financial Technology",     "sub": "B2B Payments",             "employees": "200-1000", "hq": "US", "type": "Series B",  "icp": 88, "arr": 280000,  "target": True,  "tier": "tier1"},
    {"name": "Vertex AI Labs",       "domain": "vertexailabs.com",       "industry": "Software & SaaS",          "sub": "AI/ML",                    "employees": "50-200",   "hq": "US", "type": "Series A",  "icp": 90, "arr": 195000,  "target": True,  "tier": "tier1"},
    {"name": "Cascade Analytics",   "domain": "cascadeanalytics.io",    "industry": "Software & SaaS",          "sub": "Business Intelligence",    "employees": "200-1000", "hq": "GB", "type": "Private",   "icp": 85, "arr": 215000,  "target": True,  "tier": "tier1"},
    {"name": "NovaMed Health",      "domain": "novamedhealth.com",      "industry": "Healthcare IT",            "sub": "EHR/EMR",                  "employees": "200-1000", "hq": "US", "type": "Series C",  "icp": 82, "arr": 390000,  "target": True,  "tier": "tier1"},
    # ── Tier 2 target accounts ────────────────────────────────────────────────
    {"name": "PivotPoint CRM",      "domain": "pivotpointcrm.com",      "industry": "Software & SaaS",          "sub": "CRM",                      "employees": "50-200",   "hq": "US", "type": "Series A",  "icp": 78, "arr": 145000,  "target": True,  "tier": "tier2"},
    {"name": "DataVault Security",  "domain": "datavaultsec.com",       "industry": "Cybersecurity",            "sub": "Data Loss Prevention",     "employees": "200-1000", "hq": "US", "type": "Series B",  "icp": 80, "arr": 265000,  "target": True,  "tier": "tier2"},
    {"name": "Vortex Cloud",        "domain": "vortexcloud.io",         "industry": "Software & SaaS",          "sub": "Cloud Infrastructure",     "employees": "200-1000", "hq": "US", "type": "Series C",  "icp": 83, "arr": 305000,  "target": True,  "tier": "tier2"},
    {"name": "Quantum Analytics",   "domain": "quantumanalytics.io",    "industry": "Software & SaaS",          "sub": "Data Analytics",           "employees": "50-200",   "hq": "NL", "type": "Series A",  "icp": 79, "arr": 135000,  "target": True,  "tier": "tier2"},
    {"name": "PinPoint Sales",      "domain": "pinpointsales.io",       "industry": "Software & SaaS",          "sub": "Sales Intelligence",       "employees": "50-200",   "hq": "US", "type": "Series A",  "icp": 85, "arr": 165000,  "target": True,  "tier": "tier2"},
    {"name": "Cobalt Cybersec",     "domain": "cobaltcybersec.com",     "industry": "Cybersecurity",            "sub": "Penetration Testing",      "employees": "50-200",   "hq": "US", "type": "Private",   "icp": 77, "arr": 195000,  "target": True,  "tier": "tier2"},
    {"name": "Starpoint Data",      "domain": "starpointdata.io",       "industry": "Software & SaaS",          "sub": "Data Infrastructure",      "employees": "50-200",   "hq": "US", "type": "Series A",  "icp": 81, "arr": 175000,  "target": True,  "tier": "tier2"},
    {"name": "Praxis Network",      "domain": "praxisnetwork.io",       "industry": "Software & SaaS",          "sub": "Network Management",       "employees": "50-200",   "hq": "US", "type": "Series A",  "icp": 77, "arr": 128000,  "target": True,  "tier": "tier2"},
    # ── Logistics & Supply Chain (Trap C targets, indices 13–19) ─────────────
    {"name": "Freightworks Inc",    "domain": "freightworks.com",       "industry": "Logistics & Supply Chain", "sub": "Freight Management",       "employees": "200-1000", "hq": "US", "type": "Private",   "icp": 71, "arr": 185000,  "target": False, "tier": None},
    {"name": "PortalShip Logistics","domain": "portalship.io",          "industry": "Logistics & Supply Chain", "sub": "Last-Mile Delivery",       "employees": "200-1000", "hq": "US", "type": "Series A",  "icp": 68, "arr": 140000,  "target": False, "tier": None},
    {"name": "Nexus Supply Co",     "domain": "nexussupply.com",        "industry": "Logistics & Supply Chain", "sub": "Procurement",              "employees": "1000+",    "hq": "US", "type": "Private",   "icp": 65, "arr": 245000,  "target": False, "tier": None},
    {"name": "RouteOptix",          "domain": "routeoptix.io",          "industry": "Logistics & Supply Chain", "sub": "Route Optimization",       "employees": "50-200",   "hq": "US", "type": "Series B",  "icp": 73, "arr": 160000,  "target": False, "tier": None},
    {"name": "ClearChain Systems",  "domain": "clearchain.com",         "industry": "Logistics & Supply Chain", "sub": "Supply Chain Visibility",  "employees": "200-1000", "hq": "GB", "type": "Series A",  "icp": 70, "arr": 210000,  "target": False, "tier": None},
    {"name": "Apex Transport",      "domain": "apextransport.io",       "industry": "Logistics & Supply Chain", "sub": "Fleet Management",         "employees": "200-1000", "hq": "US", "type": "Private",   "icp": 52, "arr": 130000,  "target": False, "tier": None},
    {"name": "Meridian Logistics",  "domain": "meridianlogistics.au",   "industry": "Logistics & Supply Chain", "sub": "Third-Party Logistics",    "employees": "200-1000", "hq": "AU", "type": "Private",   "icp": 63, "arr": 175000,  "target": False, "tier": None},
    # ── Non-target, mixed verticals ───────────────────────────────────────────
    {"name": "Bluepeak Media",      "domain": "bluepeakmedia.com",      "industry": "Media & Publishing",       "sub": "Digital Publishing",       "employees": "50-200",   "hq": "US", "type": "Private",   "icp": 42, "arr": 72000,   "target": False, "tier": None},
    {"name": "VistaLegal",          "domain": "vistalegal.io",          "industry": "Legal Technology",         "sub": "Legal Ops",                "employees": "50-200",   "hq": "US", "type": "Seed",      "icp": 60, "arr": 88000,   "target": False, "tier": None},
    {"name": "Ironridge Mfg",       "domain": "ironridgemfg.com",       "industry": "Manufacturing",            "sub": "Industrial Equipment",     "employees": "1000+",    "hq": "US", "type": "Public",    "icp": 38, "arr": 1200000, "target": False, "tier": None},
    {"name": "ClearPoint Insurance","domain": "clearpointins.com",      "industry": "Insurance Technology",     "sub": "Commercial Lines",         "employees": "200-1000", "hq": "US", "type": "Private",   "icp": 55, "arr": 150000,  "target": False, "tier": None},
    {"name": "Archon Consulting",   "domain": "archonconsulting.com",   "industry": "Professional Services",    "sub": "Management Consulting",    "employees": "200-1000", "hq": "US", "type": "Partnership","icp": 48, "arr": 320000,  "target": False, "tier": None},
    {"name": "Zephyr Biotech",      "domain": "zephyrbiotech.com",      "industry": "Life Sciences",            "sub": "Biotech",                  "employees": "50-200",   "hq": "US", "type": "Series B",  "icp": 62, "arr": 125000,  "target": False, "tier": None},
    {"name": "Oceanic Retail",      "domain": "oceanicretail.com",      "industry": "Retail Technology",        "sub": "E-Commerce",               "employees": "200-1000", "hq": "AU", "type": "Private",   "icp": 35, "arr": 68000,   "target": False, "tier": None},
    {"name": "Harborview Education","domain": "harborviewedu.com",      "industry": "EdTech",                   "sub": "Enterprise Learning",      "employees": "50-200",   "hq": "US", "type": "Seed",      "icp": 50, "arr": 58000,   "target": False, "tier": None},
    {"name": "Cloudrise Energy",    "domain": "cloudriseenergy.io",     "industry": "CleanTech",                "sub": "Energy Management",        "employees": "50-200",   "hq": "DE", "type": "Series A",  "icp": 58, "arr": 92000,   "target": False, "tier": None},
    {"name": "Blackstone Legal AI", "domain": "blackstonelegalai.com",  "industry": "Legal Technology",         "sub": "Contract AI",              "employees": "50-200",   "hq": "GB", "type": "Series A",  "icp": 67, "arr": 105000,  "target": False, "tier": None},
    {"name": "Northspar Retail",    "domain": "northspar.com",          "industry": "Retail Technology",        "sub": "POS Systems",              "employees": "200-1000", "hq": "CA", "type": "Private",   "icp": 33, "arr": 82000,   "target": False, "tier": None},
    {"name": "Solstice Wellness",   "domain": "solsticewellness.com",   "industry": "Healthcare IT",            "sub": "Mental Health Tech",       "employees": "50-200",   "hq": "US", "type": "Series A",  "icp": 59, "arr": 78000,   "target": False, "tier": None},
    {"name": "Atlas Compliance",    "domain": "atlascompliance.com",    "industry": "RegTech",                  "sub": "Compliance Automation",    "employees": "50-200",   "hq": "IE", "type": "Series A",  "icp": 73, "arr": 115000,  "target": False, "tier": None},
    {"name": "Beacon Telecom",      "domain": "beacontelecom.com",      "industry": "Telecommunications",       "sub": "UCaaS",                    "employees": "200-1000", "hq": "US", "type": "Private",   "icp": 44, "arr": 230000,  "target": False, "tier": None},
    {"name": "Irongate Commerce",   "domain": "irongatecommerce.io",    "industry": "E-Commerce Technology",   "sub": "Headless Commerce",        "employees": "50-200",   "hq": "US", "type": "Series B",  "icp": 66, "arr": 142000,  "target": False, "tier": None},
    {"name": "Redwood Research",    "domain": "redwoodresearch.com",    "industry": "Professional Services",    "sub": "Market Research",          "employees": "50-200",   "hq": "GB", "type": "Private",   "icp": 37, "arr": 68000,   "target": False, "tier": None},
    {"name": "SkyBridge Fintech",   "domain": "skybridgefintech.io",    "industry": "Financial Technology",     "sub": "Digital Banking",          "employees": "200-1000", "hq": "SG", "type": "Series B",  "icp": 71, "arr": 275000,  "target": False, "tier": None},
    {"name": "WaveRider Media",     "domain": "waveridermedia.com",     "industry": "AdTech",                   "sub": "Connected TV",             "employees": "50-200",   "hq": "US", "type": "Series A",  "icp": 54, "arr": 89000,   "target": False, "tier": None},
    {"name": "Granite HR Systems",  "domain": "granitehr.com",          "industry": "HR Technology",            "sub": "HRIS",                     "employees": "200-1000", "hq": "US", "type": "Private",   "icp": 68, "arr": 148000,  "target": False, "tier": None},
    {"name": "Meridian Robotics",   "domain": "meridianrobotics.com",   "industry": "Manufacturing",            "sub": "Industrial Robotics",      "employees": "200-1000", "hq": "DE", "type": "Series C",  "icp": 58, "arr": 420000,  "target": False, "tier": None},
    {"name": "Oasis Payments",      "domain": "oasispayments.io",       "industry": "Financial Technology",     "sub": "Payment Processing",       "employees": "200-1000", "hq": "IN", "type": "Series B",  "icp": 70, "arr": 295000,  "target": False, "tier": None},
    {"name": "Prism ERP",           "domain": "prismerp.io",            "industry": "Software & SaaS",          "sub": "ERP",                      "employees": "1000+",    "hq": "DE", "type": "Public",    "icp": 76, "arr": 820000,  "target": True,  "tier": "tier2"},
    {"name": "FlowState HR",        "domain": "flowstatehr.com",        "industry": "HR Technology",            "sub": "Workforce Management",     "employees": "50-200",   "hq": "AU", "type": "Private",   "icp": 74, "arr": 110000,  "target": True,  "tier": "tier2"},
    {"name": "RealEdge PropTech",   "domain": "realedge.io",            "industry": "PropTech",                 "sub": "Commercial RE",            "employees": "50-200",   "hq": "CA", "type": "Seed",      "icp": 72, "arr": 98000,   "target": True,  "tier": "tier2"},
    {"name": "Gridwell Utilities",  "domain": "gridwellutilities.com",  "industry": "Utilities Technology",     "sub": "Smart Grid",               "employees": "1000+",    "hq": "US", "type": "Public",    "icp": 41, "arr": 550000,  "target": False, "tier": None},
    {"name": "Meridian Hospitality","domain": "meridianhospitality.com","industry": "Hospitality Technology",   "sub": "Property Management",      "employees": "50-200",   "hq": "US", "type": "Private",   "icp": 27, "arr": 43000,   "target": False, "tier": None},
    {"name": "Terracotta Foods",    "domain": "terracottafoods.com",    "industry": "Food & Beverage",          "sub": "CPG",                      "employees": "50-200",   "hq": "US", "type": "Private",   "icp": 31, "arr": 45000,   "target": False, "tier": None},
    {"name": "Synergy Mfg",         "domain": "synergymfg.com",         "industry": "Manufacturing",            "sub": "Smart Factory",            "employees": "1000+",    "hq": "DE", "type": "Public",    "icp": 36, "arr": 1800000, "target": False, "tier": None},
    {"name": "Stratum Realty",      "domain": "stratumrealty.com",      "industry": "Real Estate",              "sub": "Commercial",               "employees": "200-1000", "hq": "US", "type": "Private",   "icp": 29, "arr": 95000,   "target": False, "tier": None},
    {"name": "Lighthouse EDU",      "domain": "lighthouseed.io",        "industry": "EdTech",                   "sub": "K-12 Tech",                "employees": "50-200",   "hq": "US", "type": "Non-Profit","icp": 22, "arr": 32000,   "target": False, "tier": None},
]  # 50 total: 13 tier1/tier2 targets + 7 logistics + 30 non-target

assert len(_COMPANIES_RAW) == 50, f"Expected 50 companies, got {len(_COMPANIES_RAW)}"


# ══════════════════════════════════════════════════════════════════════════════
# ── Utility helpers
# ══════════════════════════════════════════════════════════════════════════════

def _uid() -> str:
    return str(uuid.uuid4())


def _ts(dt: datetime) -> str:
    """Render a datetime as an ISO-8601 TIMESTAMP string for BigQuery."""
    return dt.isoformat()


def _weekday_factor(d: date) -> float:
    """
    Return a spend multiplier based on weekday.
    B2B paid media drops ~30% on weekends (lower B2B intent browsing).
    """
    return 0.70 if d.weekday() >= 5 else 1.00


def _batch_insert(table_name: str, rows: list[dict]) -> int:
    """
    Load rows into BigQuery using load jobs (not streaming API).
    Load jobs work immediately on newly created tables and handle bulk data
    more reliably than streaming inserts.
    Returns the total count inserted; logs any errors.
    """
    from google.cloud import bigquery as _bigquery

    from config import settings as _settings

    project  = _settings.gcp_project_id
    dataset  = _settings.gcp_dataset_id
    table_id = f"{project}.{dataset}.{table_name}"
    client   = _bigquery.Client(project=project)
    job_cfg  = _bigquery.LoadJobConfig(
        write_disposition=_bigquery.WriteDisposition.WRITE_APPEND,
        source_format=_bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i : i + BATCH_SIZE]
        try:
            job = client.load_table_from_json(chunk, table_id, job_config=job_cfg)
            job.result()
            inserted += len(chunk)
        except Exception as exc:
            log.warning(
                "sandbox.load_error",
                table=table_name,
                chunk_start=i,
                error=str(exc)[:200],
            )
    return inserted


def _click_token(platform: str, rng: random.Random) -> dict[str, str | None]:
    """Generate a single platform-specific click identifier for a paid session."""
    hex16 = rng.randbytes(8).hex()
    base: dict[str, str | None] = {
        "gclid": None, "fbclid": None, "msclkid": None,
        "ttclid": None, "li_fat_id": None,
    }
    if platform == "google_ads":
        base["gclid"] = f"GCLID_{hex16}"
    elif platform == "meta":
        base["fbclid"] = f"FBCLID_{hex16}"
    elif platform == "linkedin":
        base["li_fat_id"] = f"LIFAT_{hex16}"
    elif platform == "tiktok":
        base["ttclid"] = f"TTCLID_{hex16}"
    # reddit_ads: UTM-only, no proprietary click token
    return base


def _utm_for_platform(platform: str, campaign: dict) -> dict[str, str]:
    source_map = {
        "google_ads":  "google",
        "meta":        "facebook",
        "linkedin":    "linkedin",
        "tiktok":      "tiktok",
        "reddit_ads":  "reddit",
    }
    return {
        "utm_source":   source_map.get(platform, platform),
        "utm_medium":   "cpc",
        "utm_campaign": campaign["campaign_id"],
    }


def _build_companies(rng: random.Random) -> list[dict]:
    """
    Materialise company records with stable UUIDs derived from the fixture list.
    UUID is deterministic per run (seeded), ensuring referential integrity
    across all tables that reference company_id.
    """
    companies = []
    for raw in _COMPANIES_RAW:
        companies.append({
            **raw,
            "company_id": _uid(),
            "enrichment_confidence": round(rng.uniform(0.72, 0.98), 3),
            "crm_account_owner":     rng.choice([
                "alex.chen@company.com", "maya.patel@company.com",
                "james.thornton@company.com", "sofia.reyes@company.com",
            ]),
        })
    return companies


# ══════════════════════════════════════════════════════════════════════════════
# ── Table generators
# ══════════════════════════════════════════════════════════════════════════════

def gen_platform_campaigns(now_ts: str) -> list[dict]:
    rows = []
    for c in _CAMPAIGNS:
        rows.append({
            "campaign_id":           c["campaign_id"],
            "platform_campaign_id":  c["campaign_id"],   # sandbox: same ID
            "campaign_name":         c["campaign_name"],
            "platform":              c["platform"],
            "status":                "ENABLED",
            "objective":             c["objective"],
            "channel":               c["platform"],
            "ingested_at":           now_ts,
            "last_synced_at":        now_ts,
        })
    return rows


def gen_platform_daily_spend(
    days: int,
    today: date,
    rng: random.Random,
    now_ts: str = "",
) -> list[dict]:
    """
    Generate campaign × date × geo spend rows for all non-Reddit platforms.

    For each campaign, the day's total spend is split across 2-3 geo codes
    using the GEO_POOL distribution. Trap B dates (the surge window) receive
    FLAT spend — no change from baseline — to keep paid signals clean for
    BSTS counterfactual inference.
    """
    rows: list[dict] = []
    trap_b_start = today - timedelta(days=TRAP_B_DAYS_AGO)
    trap_b_dates = {
        trap_b_start + timedelta(days=i)
        for i in range(TRAP_B_DURATION)
    }

    for c in _CAMPAIGNS:
        if c["platform"] == "reddit_ads":
            continue  # Reddit goes to its own table
        cfg  = _PLATFORM_CFG[c["platform"]]
        base = cfg["base_spend"] * c["spend_weight"]
        std  = cfg["spend_std"]  * c["spend_weight"]

        for d in (today - timedelta(days=i) for i in range(days)):
            factor = _weekday_factor(d)
            # Trap B: keep paid spend flat (use the seeded baseline, no noise)
            if d in trap_b_dates:
                day_spend = round(base * factor, 2)
            else:
                day_spend = round(max(10.0, rng.gauss(base, std) * factor), 2)

            # Split across 2-3 geos
            n_geos = rng.choice([2, 2, 3])
            geos   = rng.choices(_GEO_CODES, weights=_GEO_PROBS, k=n_geos)
            seen   = set()
            geo_list: list[str] = []
            for g in geos:
                if g not in seen:
                    seen.add(g)
                    geo_list.append(g)
            if not geo_list:
                geo_list = ["US"]

            geo_weights = [_GEO_PROBS[_GEO_CODES.index(g)] for g in geo_list]
            total_weight = sum(geo_weights)
            geo_shares   = [w / total_weight for w in geo_weights]

            for geo, share in zip(geo_list, geo_shares):
                spend = round(day_spend * share, 2)
                impr  = max(1, int(spend / cfg["cpm"] * 1000))
                clicks= max(0, int(impr * cfg["ctr"]))
                convs = round(clicks * cfg["cvr"], 4)

                rows.append({
                    "spend_id":             _uid(),
                    "date":                 d.isoformat(),
                    "platform":             c["platform"],
                    "campaign_id":          c["campaign_id"],
                    "platform_campaign_id": c["campaign_id"],
                    "geo_country_code":     geo,
                    "spend":                spend,
                    "impressions":          impr,
                    "clicks":               clicks,
                    "platform_conversions": round(convs, 4),
                    "ingested_at":          now_ts or _ts(datetime.now(tz=timezone.utc)),
                })
    return rows


def gen_reddit_daily_spend(
    days: int,
    today: date,
    rng: random.Random,
) -> list[dict]:
    """
    Generate reddit_daily_spend rows for Reddit Ads campaigns.
    reddit_daily_spend has richer video + engagement fields than platform_daily_spend.
    """
    rows: list[dict] = []
    run_id = _uid()
    reddit_campaigns = [c for c in _CAMPAIGNS if c["platform"] == "reddit_ads"]
    cfg = _PLATFORM_CFG["reddit_ads"]

    for c in reddit_campaigns:
        base = cfg["base_spend"] * c["spend_weight"]
        std  = cfg["spend_std"]  * c["spend_weight"]
        account_id = "a2_sandbox_reddit_001"

        for d in (today - timedelta(days=i) for i in range(days)):
            factor    = _weekday_factor(d)
            day_spend = round(max(5.0, rng.gauss(base, std) * factor), 2)
            impr      = max(1, int(day_spend / cfg["cpm"] * 1000))
            clicks    = max(0, int(impr * cfg["ctr"]))
            convs     = int(clicks * cfg["cvr"])
            video_p   = int(impr * rng.uniform(0.20, 0.40))

            rows.append({
                "row_id":               _uid(),
                "run_id":               run_id,
                "account_id":           account_id,
                "campaign_id":          c["campaign_id"],
                "campaign_name":        c["campaign_name"],
                "campaign_objective":   c["objective"],
                "ad_group_id":          f"{c['campaign_id']}_ag01",
                "ad_group_name":        f"{c['campaign_name']} — Ad Group 1",
                "date":                 d.isoformat(),
                "time_zone_id":         "America/New_York",
                # Financial
                "spend":                day_spend,
                "cpc":                  round(day_spend / max(clicks, 1), 4),
                "cpm":                  round(day_spend / max(impr, 1) * 1000, 4),
                "ecpm":                 round(day_spend / max(impr, 1) * 1000, 4),
                "cost_per_conversion":  round(day_spend / max(convs, 1), 4),
                # Counts
                "impressions":          impr,
                "clicks":               clicks,
                "conversions":          convs,
                "view_conversions":     int(convs * rng.uniform(0.2, 0.5)),
                "ctr":                  round(clicks / max(impr, 1), 6),
                "video_plays":          video_p,
                "video_views_25pct":    int(video_p * rng.uniform(0.55, 0.75)),
                "video_views_50pct":    int(video_p * rng.uniform(0.35, 0.55)),
                "video_views_75pct":    int(video_p * rng.uniform(0.20, 0.40)),
                "video_views_100pct":   int(video_p * rng.uniform(0.10, 0.25)),
                "video_completion_rate":round(rng.uniform(0.12, 0.28), 4),
                "capture_timestamp":    datetime.now(timezone.utc).isoformat(),
            })
    return rows


def gen_sessions(
    days: int,
    today: date,
    rng: random.Random,
    companies: list[dict],
) -> tuple[list[dict], dict[str, str]]:
    """
    Generate web session rows.

    Returns (session_rows, ga4_client_id_to_domain_map).
    Each session has a ga4_client_id (opaque cookie), click tokens for paid
    sessions, UTM params, and a company domain resolved from the company list.

    Trap B dates receive a 5× multiplier on organic/direct sessions.
    session_id is an opaque analytics identifier — no PII stored.
    """
    trap_b_start = today - timedelta(days=TRAP_B_DAYS_AGO)
    trap_b_dates = {
        trap_b_start + timedelta(days=i)
        for i in range(TRAP_B_DURATION)
    }

    paid_campaigns = [c for c in _CAMPAIGNS if c["platform"] != "reddit_ads"]
    session_rows:   list[dict] = []
    ga4_to_domain:  dict[str, str] = {}  # for CRM linkage

    # Assign each company a pool of recurring ga4_client_ids (simulating repeat visitors)
    company_client_ids: dict[str, list[str]] = {}
    for co in companies:
        n_visitors = max(3, int(co["icp"] / 10))
        company_client_ids[co["domain"]] = [_uid() for _ in range(n_visitors)]
        for cid in company_client_ids[co["domain"]]:
            ga4_to_domain[cid] = co["domain"]

    for d in (today - timedelta(days=i) for i in range(days)):
        base_daily = 120
        weekend_factor = 0.65 if d.weekday() >= 5 else 1.0
        trap_b_factor  = TRAP_B_MULTIPLIER if d in trap_b_dates else 1.0

        # Organic/direct sessions (benefit from Trap B surge)
        n_organic = int(base_daily * 0.55 * weekend_factor * trap_b_factor)
        # Paid sessions (flat — no trap_b_factor for paid)
        n_paid    = int(base_daily * 0.45 * weekend_factor)

        day_sessions: list[dict] = []

        # ── Organic/direct sessions ──────────────────────────────────────────
        for _ in range(n_organic):
            co  = rng.choice(companies)
            cid = rng.choice(company_client_ids[co["domain"]])
            # Trap B: organic sessions concentrated in US geo
            geo = "US" if d in trap_b_dates else rng.choices(_GEO_CODES, weights=_GEO_PROBS, k=1)[0]

            day_sessions.append({
                "session_id":       _uid(),
                "session_source":   "ga4",
                "session_start_at": d.isoformat() + "T00:00:00Z",
                "ga4_client_id":    cid,
                "gclid":            None,
                "fbclid":           None,
                "msclkid":          None,
                "ttclid":           None,
                "li_fat_id":        None,
                "utm_source":       rng.choice([None, None, "google", "bing"]),
                "utm_medium":       rng.choice([None, None, "organic", "direct"]),
                "utm_campaign":     None,
                "country":          geo,
                "ingested_at":      _ts(datetime.now(tz=timezone.utc)),
            })

        # ── Paid sessions ────────────────────────────────────────────────────
        for _ in range(n_paid):
            co       = rng.choice(companies)
            cid      = rng.choice(company_client_ids[co["domain"]])
            campaign = rng.choice(paid_campaigns)
            tokens   = _click_token(campaign["platform"], rng)
            utms     = _utm_for_platform(campaign["platform"], campaign)
            geo      = rng.choices(_GEO_CODES, weights=_GEO_PROBS, k=1)[0]

            day_sessions.append({
                "session_id":       _uid(),
                "session_source":   "ga4",
                "session_start_at": d.isoformat() + "T00:00:00Z",
                "ga4_client_id":    cid,
                **tokens,
                **utms,
                "country":          geo,
                "ingested_at":      _ts(datetime.now(tz=timezone.utc)),
            })

        session_rows.extend(day_sessions)

    return session_rows, ga4_to_domain


def gen_crm_leads(
    days: int,
    today: date,
    rng: random.Random,
    companies: list[dict],
    ga4_to_domain: dict[str, str],
    session_rows: list[dict],
) -> list[dict]:
    """
    Generate crm_leads_staging rows (normal + Trap A overwritten leads).

    Normal leads: a fraction of paid sessions convert to a CRM lead within
    1-3 days. lead_source reflects the actual paid channel.

    Trap A (150 leads): sessions with gclid/fbclid are matched with a CRM
    lead where lead_source is an offline label ("Content Syndication Bulk
    Upload" etc.). The session event_date is 5 days BEFORE created_at.
    """
    lead_rows: list[dict] = []

    # Normal lead generation (non-Trap sessions → CRM)
    paid_sessions = [
        s for s in session_rows
        if s.get("gclid") or s.get("fbclid") or s.get("li_fat_id") or s.get("ttclid")
    ]
    # ~8% of paid sessions generate a lead
    lead_sessions = rng.sample(paid_sessions, min(int(len(paid_sessions) * 0.08), days * 4))

    for s in lead_sessions:
        domain = ga4_to_domain.get(s["ga4_client_id"])
        if not domain:
            domain = rng.choice(companies)["domain"]

        session_date = date.fromisoformat(s["session_start_at"][:10])
        lead_date    = session_date + timedelta(days=rng.randint(0, 3))
        if lead_date > today:
            lead_date = today

        # Determine lead_source from click token (clean attribution)
        if s.get("gclid"):
            source = "Paid Search — Google"
        elif s.get("fbclid"):
            source = "Paid Social — Meta"
        elif s.get("li_fat_id"):
            source = "Paid Social — LinkedIn"
        elif s.get("ttclid"):
            source = "Paid Social — TikTok"
        else:
            source = "Paid — UTM CPC"

        lead_dt = datetime.combine(lead_date, datetime.min.time(), tzinfo=timezone.utc)
        lead_rows.append({
            "lead_id":               _uid(),
            "ga_client_id":          s["ga4_client_id"],
            "company_domain":        domain,
            "lead_source":           source,
            "utm_source":            s.get("utm_source"),
            "utm_medium":            s.get("utm_medium"),
            "created_at":            _ts(lead_dt),
            "lead_source_updated_at":None,
            "systemmodstamp":        None,
            "updated_at":            _ts(lead_dt),
            "last_modified_at":      None,
        })

    # ── Trap A: Salesforce Overwrite Loop (150 leads) ─────────────────────────
    # Pick 150 paid sessions (with gclid or fbclid) that will have their
    # lead_source overwritten to an offline value in the CRM.
    trap_a_candidates = [
        s for s in paid_sessions
        if s.get("gclid") or s.get("fbclid")
    ]
    trap_a_sample = rng.sample(trap_a_candidates, min(TRAP_A_COUNT, len(trap_a_candidates)))

    for s in trap_a_sample:
        domain = ga4_to_domain.get(s["ga4_client_id"])
        if not domain:
            domain = rng.choice(companies)["domain"]

        session_date = date.fromisoformat(s["session_start_at"][:10])
        # ▸ CRM lead created 5 days AFTER the session (the overwrite delay)
        lead_date    = session_date + timedelta(days=5)
        if lead_date > today:
            lead_date = today

        # CRM lead_source is an offline label — not the true paid source
        offline_source = rng.choice(_TRAP_A_LEAD_SOURCES)

        lead_dt = datetime.combine(lead_date, datetime.min.time(), tzinfo=timezone.utc)
        # systemmodstamp is set to the overwrite date (= created_at here),
        # masking any prior lead_source assignment
        lead_rows.append({
            "lead_id":               _uid(),
            "ga_client_id":          s["ga4_client_id"],
            "company_domain":        domain,
            "lead_source":           offline_source,
            "utm_source":            s.get("utm_source"),
            "utm_medium":            s.get("utm_medium"),
            "created_at":            _ts(lead_dt),
            "lead_source_updated_at":_ts(lead_dt),
            "systemmodstamp":        _ts(lead_dt),
            "updated_at":            _ts(lead_dt),
            "last_modified_at":      _ts(lead_dt),
        })

    return lead_rows


def gen_crm_opportunities(
    days: int,
    today: date,
    rng: random.Random,
    companies: list[dict],
) -> list[dict]:
    """
    Generate crm_opportunities_staging rows.

    Trap B: several Closed-Won opportunities are placed during the 5-day
    organic surge window for US-headquartered companies.

    Trap C: in the last 30 days, Logistics & Supply Chain companies with
    ARR > $120K are the majority of Closed-Won revenue (45% share).
    """
    rows: list[dict] = []

    trap_b_start  = today - timedelta(days=TRAP_B_DAYS_AGO)
    trap_b_end    = trap_b_start + timedelta(days=TRAP_B_DURATION - 1)
    trap_c_cutoff = today - timedelta(days=TRAP_C_WINDOW)

    logistics_companies = [
        co for co in companies
        if "Logistics" in co["industry"] and co["arr"] > 120_000
    ]

    # Allocate base opportunities (~1-2 per day)
    base_daily_ops = 1.5
    total_base = int(days * base_daily_ops)

    # Target Closed-Won revenue over last 30 days for Trap C calibration
    # We'll track and top-up logistics revenue after generating base deals.
    base_closed_won_value = 0.0
    logistics_closed_won_value = 0.0

    for _ in range(total_base):
        co = rng.choice(companies)
        created_ago  = rng.randint(1, days)
        created_date = today - timedelta(days=created_ago)
        stage        = rng.choice(_PIPELINE_STAGES)
        is_closed    = stage in ("closed_won", "closed_lost")
        closed_at    = None
        deal_value   = round(co["arr"] * rng.uniform(0.05, 0.18), 2)

        if is_closed:
            closed_date = created_date + timedelta(days=rng.randint(7, 45))
            if closed_date > today:
                closed_date = today
            closed_at = _ts(datetime.combine(closed_date, datetime.min.time(), tzinfo=timezone.utc))
            if stage == "closed_won" and closed_date >= trap_c_cutoff:
                base_closed_won_value += deal_value
                if "Logistics" in co["industry"]:
                    logistics_closed_won_value += deal_value

        rows.append({
            "account_id":     _uid(),
            "company_domain": co["domain"],
            "industry":       co["industry"],
            "pipeline_stage": stage,
            "is_closed":      is_closed,
            "deal_value":     deal_value,
            "created_at":     _ts(datetime.combine(created_date, datetime.min.time(), tzinfo=timezone.utc)),
            "closed_at":      closed_at,
        })

    # ── Trap B: Closed-Won deals during the organic surge window ─────────────
    # 4-6 Closed-Won opportunities from US companies during the surge.
    us_companies = [co for co in companies if co["hq"] == "US"]
    for i in range(rng.randint(4, 6)):
        co = rng.choice(us_companies)
        surge_day = trap_b_start + timedelta(days=rng.randint(0, TRAP_B_DURATION - 1))
        if surge_day > today:
            surge_day = today
        rows.append({
            "account_id":     _uid(),
            "company_domain": co["domain"],
            "industry":       co["industry"],
            "pipeline_stage": "closed_won",
            "is_closed":      True,
            "deal_value":     round(co["arr"] * rng.uniform(0.10, 0.20), 2),
            "created_at":     _ts(datetime.combine(surge_day - timedelta(days=30), datetime.min.time(), tzinfo=timezone.utc)),
            "closed_at":      _ts(datetime.combine(surge_day, datetime.min.time(), tzinfo=timezone.utc)),
        })

    # ── Trap C: Logistics Closed-Won top-up to hit 45% revenue share ─────────
    # Compute how many additional logistics deals are needed to reach TRAP_C_SHARE.
    # Target: logistics_revenue / total_revenue = TRAP_C_SHARE
    # ⇒ logistics_needed = (TRAP_C_SHARE × total) - logistics_so_far
    # ⇒ total = base + logistics_needed
    # Solving: logistics_needed = (TRAP_C_SHARE / (1 - TRAP_C_SHARE)) × (base - logistics_so_far)
    non_logistics_base = base_closed_won_value - logistics_closed_won_value
    target_logistics   = (TRAP_C_SHARE / (1 - TRAP_C_SHARE)) * non_logistics_base
    shortfall          = max(0.0, target_logistics - logistics_closed_won_value)

    while shortfall > 5_000 and logistics_companies:
        co          = rng.choice(logistics_companies)
        deal_value  = round(co["arr"] * rng.uniform(0.08, 0.20), 2)
        close_ago   = rng.randint(1, TRAP_C_WINDOW)
        close_date  = today - timedelta(days=close_ago)
        create_date = close_date - timedelta(days=rng.randint(14, 45))

        rows.append({
            "account_id":     _uid(),
            "company_domain": co["domain"],
            "industry":       co["industry"],
            "pipeline_stage": "closed_won",
            "is_closed":      True,
            "deal_value":     deal_value,
            "created_at":     _ts(datetime.combine(create_date, datetime.min.time(), tzinfo=timezone.utc)),
            "closed_at":      _ts(datetime.combine(close_date, datetime.min.time(), tzinfo=timezone.utc)),
        })
        shortfall -= deal_value

    return rows


def gen_company_profiles(
    companies: list[dict],
    now_ts: str,
) -> list[dict]:
    rows = []
    for co in companies:
        rows.append({
            "company_id":            co["company_id"],
            "company_domain":        co["domain"],
            "company_name":          co["name"],
            "industry":              co["industry"],
            "sub_industry":          co["sub"],
            "employee_range":        co["employees"],
            "headquarters_country":  co["hq"],
            "company_type":          co["type"],
            "enrichment_confidence": co["enrichment_confidence"],
            "icp_score":             co["icp"],
            "crm_account_owner":     co["crm_account_owner"],
            "annual_revenue":        float(co["arr"]),
            "created_at":            now_ts,
            "updated_at":            now_ts,
        })
    return rows


def gen_company_engagement(
    companies: list[dict],
    today: date,
    rng: random.Random,
    session_rows: list[dict],
    ga4_to_domain: dict[str, str],
) -> list[dict]:
    """
    Generate one rolling_30d company_engagement row per company.

    Engagement levels are scaled by icp_score and target account status.
    Intent scores are computed to produce realistic ABM tier classification
    (high-intent ≥ 65, discovery 30-64, ICP miss ≥ 45).

    Trap B companies (US-based, non-logistics) get inflated organic sessions
    to reflect the surge window's domain-level impact.
    """
    period_start = today - timedelta(days=30)

    # Build domain → session count from the synthetic session data
    domain_sessions: dict[str, int] = {}
    domain_paid_platforms: dict[str, set[str]] = {}
    for s in session_rows:
        dom = ga4_to_domain.get(s["ga4_client_id"])
        if not dom:
            continue
        event_date = date.fromisoformat(s["session_start_at"][:10])
        if event_date < period_start:
            continue
        domain_sessions[dom] = domain_sessions.get(dom, 0) + 1
        # Infer which paid platform this session came from
        if s.get("gclid"):
            domain_paid_platforms.setdefault(dom, set()).add("google_ads")
        elif s.get("fbclid"):
            domain_paid_platforms.setdefault(dom, set()).add("meta")
        elif s.get("li_fat_id"):
            domain_paid_platforms.setdefault(dom, set()).add("linkedin")
        elif s.get("ttclid"):
            domain_paid_platforms.setdefault(dom, set()).add("tiktok")

    rows = []
    for co in companies:
        dom    = co["domain"]
        icp    = co["icp"]
        target = co["target"]
        arr    = co["arr"]

        # Base engagement proportional to icp_score and account tier
        base_sessions = max(3, int(icp / 5) + rng.randint(-3, 8))
        total_sessions = domain_sessions.get(dom, base_sessions)

        paid_platforms = sorted(domain_paid_platforms.get(dom, set()))
        if not paid_platforms:
            # Give target accounts at least 1 paid platform exposure
            if target:
                paid_platforms = [rng.choice(["google_ads", "meta", "linkedin"])]

        paid_sessions_count = int(total_sessions * rng.uniform(0.25, 0.55))
        n_unique_days = min(30, max(1, int(total_sessions / rng.uniform(1.5, 4.0))))

        # Intent score (40% base, 20% velocity, 25% depth, 15% multi-channel)
        base_intent     = min(100, icp * rng.uniform(0.70, 1.15))
        pricing_visits  = max(0, rng.randint(0, 4) if icp > 65 else rng.randint(0, 1))
        demo_visits     = max(0, rng.randint(0, 3) if icp > 70 else 0)
        contact_visits  = max(0, rng.randint(0, 2) if icp > 60 else 0)
        docs_visits     = max(0, rng.randint(0, 5) if icp > 50 else rng.randint(0, 2))
        depth_score     = min(25, pricing_visits * 5 + demo_visits * 5 + contact_visits * 4 + docs_visits * 2)
        intent_score    = round(
            base_intent * 0.40
            + min(20, total_sessions / 3.0) * 0.20
            + depth_score * 0.25
            + min(15, len(paid_platforms) * 5) * 0.15,
            1,
        )

        growth_pct = round(rng.gauss(12 if target else 5, 18), 1)

        # CRM status
        crm_stage  = rng.choice(["prospect", "qualified", "opportunity"]) if target else "prospect"
        is_open_op = crm_stage == "opportunity"

        rows.append({
            "engagement_id":          _uid(),
            "company_id":             co["company_id"],
            "company_domain":         dom,
            "company_name":           co["name"],
            "period_start":           period_start.isoformat(),
            "period_end":             today.isoformat(),
            "period_type":            "rolling_30d",
            "generated_at":           _ts(datetime.now(tz=timezone.utc)),
            "is_target_account":      target,
            "account_tier":           co["tier"] or "unclassified",
            "crm_pipeline_stage":     crm_stage,
            "crm_is_open_opportunity":is_open_op,
            "total_sessions":         total_sessions,
            "total_page_views":       int(total_sessions * rng.uniform(2.5, 5.5)),
            "unique_session_days":    n_unique_days,
            "avg_pages_per_session":  round(rng.uniform(2.0, 6.0), 2),
            "pricing_page_sessions":  pricing_visits,
            "demo_page_sessions":     demo_visits,
            "contact_page_sessions":  contact_visits,
            "docs_sessions":          docs_visits,
            "case_study_sessions":    rng.randint(0, 3) if icp > 55 else 0,
            "blog_sessions":          rng.randint(0, 8),
            "paid_sessions":          paid_sessions_count,
            "paid_platforms_seen":    paid_platforms,
            "paid_campaigns_seen":    [],   # campaign IDs not tracked at this grain in sandbox
            "intent_score":           intent_score,
            "recency_score":          round(rng.uniform(40, 95) if target else rng.uniform(10, 70), 1),
            "frequency_score":        round(n_unique_days / 30.0 * 100, 1),
            "depth_score":            round(depth_score / 25.0 * 100, 1),
            "session_growth_pct":     growth_pct,
            "is_suppressed_tofu":     is_open_op,
            "suppression_reason":     "open_opportunity" if is_open_op else None,
        })

    return rows


def gen_target_account_activity(
    companies: list[dict],
    today: date,
    rng: random.Random,
) -> list[dict]:
    """
    Generate a single today-snapshot row per company in target_account_activity.
    """
    rows = []
    for co in companies:
        target  = co["target"]
        icp     = co["icp"]
        spiking = target and rng.random() < 0.25

        rows.append({
            "activity_id":                 _uid(),
            "company_id":                  co["company_id"],
            "company_domain":              co["domain"],
            "company_name":                co["name"],
            "account_tier":                co["tier"] or "unclassified",
            "date":                        today.isoformat(),
            "generated_at":                _ts(datetime.now(tz=timezone.utc)),
            "intent_spiking":              spiking,
            "web_sessions_7d":             rng.randint(3, 28) if target else rng.randint(0, 8),
            "visited_pricing_today":       target and rng.random() < 0.20,
            "visited_demo_today":          target and rng.random() < 0.15,
            "visited_contact_today":       target and rng.random() < 0.10,
            "paid_touchpoints_30d":        rng.randint(1, 8) if target else rng.randint(0, 3),
            "last_paid_touchpoint_at":     _ts(datetime.combine(
                today - timedelta(days=rng.randint(1, 14)),
                datetime.min.time(),
                tzinfo=timezone.utc,
            )) if rng.random() > 0.3 else None,
            "last_paid_touchpoint_platform": rng.choice(["google_ads", "meta", "linkedin"]) if rng.random() > 0.3 else None,
            "coverage_completeness_score": round(rng.uniform(0.45, 0.92) if target else rng.uniform(0.15, 0.65), 3),
        })
    return rows


def gen_mmm_data(
    days: int,
    today: date,
    rng: random.Random,
) -> tuple[dict, list[dict]]:
    """
    Generate one completed mmm_runs row and five mmm_channel_contributions rows.
    The model run represents a completed Meridian MCMC run from 7 days ago.
    Convergence metrics are set to pass the R-hat < 1.1 threshold.
    """
    run_id     = _uid()
    date_to    = (today - timedelta(days=7)).isoformat()
    date_from  = (today - timedelta(days=days + 7)).isoformat()
    started_at = datetime.combine(
        today - timedelta(days=7), datetime.min.time(), tzinfo=timezone.utc
    )

    run_row = {
        "run_id":           run_id,
        "status":           "completed",
        "date_from":        date_from,
        "date_to":          date_to,
        "n_geos":           8,
        "n_weeks":          max(12, days // 7),
        "n_channels":       5,
        "n_draws":          1000,
        "n_chains":         4,
        "elapsed_seconds":  round(rng.uniform(1800, 3200), 1),
        "r_hat_max":        round(rng.uniform(1.005, 1.035), 4),
        "r_hat_mean":       round(rng.uniform(1.001, 1.020), 4),
        "ess_bulk_min":     rng.randint(520, 820),
        "n_divergences":    rng.randint(0, 4),
        "spend_total_usd":  round(days * sum(c["base_spend"] for c in _PLATFORM_CFG.values()), 2),
        "kpi_total":        round(days * rng.uniform(18, 35), 1),
        "channel_index":    json.dumps(list(_PLATFORM_CFG.keys())),
        "geo_index":        json.dumps(_GEO_CODES),
        "roi_priors_used":  True,
        "run_started_at":   _ts(started_at),
        "created_at":       _ts(started_at),
    }

    total_spend = {
        p: cfg["base_spend"] * days
        for p, cfg in _PLATFORM_CFG.items()
    }

    contrib_rows = []
    for platform, cfg in _PLATFORM_CFG.items():
        spend   = round(total_spend[platform] * rng.uniform(0.92, 1.08), 2)
        impr    = max(1, int(spend / cfg["cpm"] * 1000))
        roi_mu  = cfg["roi_mean"]
        roi_p5  = cfg["roi_p5"]
        roi_p50 = cfg.get("roi_p50", roi_mu * 0.98)
        roi_p95 = cfg["roi_p95"]

        contrib_rows.append({
            "contribution_id":    _uid(),
            "run_id":             run_id,
            "channel":            platform,
            "total_spend_usd":    spend,
            "total_impressions":  impr,
            "roi_mean":           round(roi_mu * rng.uniform(0.90, 1.10), 4),
            "roi_p5":             round(roi_p5  * rng.uniform(0.92, 1.05), 4),
            "roi_p50":            round(roi_p50 * rng.uniform(0.92, 1.05), 4),
            "roi_p95":            round(roi_p95 * rng.uniform(0.95, 1.08), 4),
            "contribution_pct":   round(cfg["contribution_pct"] * rng.uniform(0.90, 1.10), 4),
            "roi_prior_injected": True,
            "roi_prior_source":   "incrementality_experiment",
            "roi_prior_mu":       round(roi_mu,  4),
            "roi_prior_sigma":    round((roi_p95 - roi_p5) / 3.29, 4),
            "created_at":         _ts(started_at),
        })

    return run_row, contrib_rows


# ══════════════════════════════════════════════════════════════════════════════
# ── Wipe helper
# ══════════════════════════════════════════════════════════════════════════════

_WIPE_TABLES = [
    "platform_campaigns",
    "platform_daily_spend",
    "reddit_daily_spend",
    "sessions",
    "crm_leads_staging",
    "crm_opportunities_staging",
    "company_profiles",
    "company_engagement",
    "target_account_activity",
    "mmm_runs",
    "mmm_channel_contributions",
]


def wipe_tables() -> dict[str, str]:
    """
    Delete all rows from each target table.
    Returns a status dict: {table_name: "wiped" | "not_found" | error_message}.
    """
    results: dict[str, str] = {}
    for logical in _WIPE_TABLES:
        try:
            ref = bq.table_ref(logical)
            n   = bq.run_dml(f"DELETE FROM {ref} WHERE TRUE")
            results[logical] = f"wiped ({n} rows)"
        except Exception as exc:
            msg = str(exc)
            if "Not found" in msg or "does not exist" in msg:
                results[logical] = "table_not_found (run schemas/bigquery_tables.py)"
            else:
                results[logical] = f"error: {msg[:80]}"
    return results


# ══════════════════════════════════════════════════════════════════════════════
# ── Terminal dashboard
# ══════════════════════════════════════════════════════════════════════════════

_COL1 = 36
_COL2 = 12
_WIDTH = _COL1 + _COL2 + 4


def _print_banner(title: str) -> None:
    print()
    print("═" * _WIDTH)
    print(f"  {title}")
    print("═" * _WIDTH)


def _print_row(label: str, value: Any, marker: str = "") -> None:
    pad = _COL1 - len(str(label))
    print(f"  {label}{' ' * max(0, pad)}  {str(value):>{_COL2}}  {marker}")


def _print_divider() -> None:
    print("─" * _WIDTH)


def _print_trap(name: str, detail: str, status: str) -> None:
    print(f"  {'▶ ' + name:<{_COL1}}  {status:>{_COL2}}")
    if detail:
        for line in detail.split("\n"):
            print(f"    {line}")


def print_dashboard(
    wipe_results: dict[str, str] | None,
    counts: dict[str, int],
    trap_notes: dict[str, str],
    elapsed: float,
    days: int,
    today: date,
) -> None:
    _print_banner(f"  Sandbox Data Generator — {today.isoformat()}")

    if wipe_results:
        print()
        print("  WIPE RESULTS")
        _print_divider()
        for tbl, status in wipe_results.items():
            _print_row(tbl, status)

    print()
    print("  TABLE ROW COUNTS")
    _print_divider()

    total = 0
    for tbl, n in counts.items():
        _print_row(tbl, f"{n:,}")
        total += n

    _print_divider()
    _print_row("TOTAL ROWS INSERTED", f"{total:,}", "✅")

    print()
    print("  ANOMALY TRAP STATUS")
    _print_divider()
    _print_trap(
        "Trap A — Salesforce Overwrite Loop",
        trap_notes.get("trap_a", ""),
        "✅ INJECTED",
    )
    _print_trap(
        "Trap B — Stealth Offline Demand Surge",
        trap_notes.get("trap_b", ""),
        "✅ INJECTED",
    )
    _print_trap(
        "Trap C — ICP Vertical Shift",
        trap_notes.get("trap_c", ""),
        "✅ INJECTED",
    )

    print()
    print("  GENERATION PARAMETERS")
    _print_divider()
    _print_row("Days of history generated", f"{days}")
    _print_row("Date range", f"{(today - timedelta(days=days-1)).isoformat()} → {today.isoformat()}")
    _print_row("Companies", "50")
    _print_row("Campaigns", f"{len(_CAMPAIGNS)}")
    _print_row("Platforms", ", ".join(_PLATFORM_CFG.keys()))
    _print_row("Random seed", str(RANDOM_SEED))
    _print_row("Elapsed", f"{elapsed:.1f}s")

    print()
    print("  NEXT STEPS")
    _print_divider()
    print("  1. Run the Watchdog agent to validate data capture rates.")
    print("  2. Run the Analyst agent → audit_data_attribution_cleanliness")
    print("     to detect Trap A (150 overwritten attribution leads).")
    print("  3. Run the Analyst agent → run_causal_impact_analysis")
    print("     to detect Trap B (US organic surge — BSTS counterfactual).")
    print("  4. Run the Operator agent → sync_evolving_lookalike_seeds")
    print("     to detect Trap C (Logistics over-index in last 30 days).")
    print()
    print("═" * _WIDTH)
    print()


# ══════════════════════════════════════════════════════════════════════════════
# ── Main
# ══════════════════════════════════════════════════════════════════════════════

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate synthetic sandbox data for paid-media-agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.strip(),
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Days of history to generate backward from today (default: 90)",
    )
    parser.add_argument(
        "--wipe-clean",
        action="store_true",
        help="DELETE all rows from target tables before generating (idempotent re-run)",
    )
    args = parser.parse_args()

    import time
    t_start = time.perf_counter()

    rng   = random.Random(RANDOM_SEED)
    today = date.today()
    now   = datetime.now(timezone.utc)
    now_ts = _ts(now)

    print("\n  paid-media-agent Sandbox Generator")
    print(f"  Generating {args.days} days of data ending {today.isoformat()} …\n")

    # ── 0. Wipe ───────────────────────────────────────────────────────────────
    wipe_results = None
    if args.wipe_clean:
        print("  Wiping target tables …")
        wipe_results = wipe_tables()
        for tbl, status in wipe_results.items():
            print(f"    {tbl:<38}  {status}")
        print()

    # ── 1. Build in-memory fixtures ───────────────────────────────────────────
    print("  Building entity fixtures …")
    companies = _build_companies(rng)
    print(f"    {len(companies)} companies materialised\n")

    # ── 2. Generate all data ──────────────────────────────────────────────────
    print("  Generating table data …")

    campaign_rows  = gen_platform_campaigns(now_ts)
    print(f"    platform_campaigns          {len(campaign_rows):>8,} rows")

    spend_rows     = gen_platform_daily_spend(args.days, today, rng, now_ts)
    print(f"    platform_daily_spend        {len(spend_rows):>8,} rows")

    reddit_rows    = gen_reddit_daily_spend(args.days, today, rng)
    print(f"    reddit_daily_spend          {len(reddit_rows):>8,} rows")

    session_rows, ga4_to_domain = gen_sessions(args.days, today, rng, companies)
    print(f"    sessions                    {len(session_rows):>8,} rows")

    lead_rows      = gen_crm_leads(args.days, today, rng, companies, ga4_to_domain, session_rows)
    print(f"    crm_leads_staging           {len(lead_rows):>8,} rows  (Trap A: {TRAP_A_COUNT} injected)")

    opp_rows       = gen_crm_opportunities(args.days, today, rng, companies)
    print(f"    crm_opportunities_staging   {len(opp_rows):>8,} rows")

    profile_rows   = gen_company_profiles(companies, now_ts)
    print(f"    company_profiles            {len(profile_rows):>8,} rows")

    engagement_rows = gen_company_engagement(companies, today, rng, session_rows, ga4_to_domain)
    print(f"    company_engagement          {len(engagement_rows):>8,} rows")

    activity_rows  = gen_target_account_activity(companies, today, rng)
    print(f"    target_account_activity     {len(activity_rows):>8,} rows")

    mmm_run, mmm_contribs = gen_mmm_data(args.days, today, rng)
    print(f"    mmm_runs                    {1:>8,} rows")
    print(f"    mmm_channel_contributions   {len(mmm_contribs):>8,} rows")

    # ── 3. Stream to BigQuery ─────────────────────────────────────────────────
    print("\n  Streaming to BigQuery …")

    counts: dict[str, int] = {}

    def _stream(logical: str, rows: list[dict]) -> None:
        n = _batch_insert(logical, rows)
        counts[logical] = n
        print(f"    ✓  {logical:<38}  {n:>8,} rows")

    _stream("platform_campaigns",        campaign_rows)
    _stream("platform_daily_spend",      spend_rows)
    _stream("reddit_daily_spend",        reddit_rows)
    _stream("sessions",                  session_rows)
    _stream("crm_leads_staging",         lead_rows)
    _stream("crm_opportunities_staging", opp_rows)
    _stream("company_profiles",          profile_rows)
    _stream("company_engagement",        engagement_rows)
    _stream("target_account_activity",   activity_rows)
    _stream("mmm_runs",                  [mmm_run])
    _stream("mmm_channel_contributions", mmm_contribs)

    elapsed = time.perf_counter() - t_start

    # ── 4. Terminal dashboard ─────────────────────────────────────────────────
    trap_b_start = today - timedelta(days=TRAP_B_DAYS_AGO)
    trap_b_end   = trap_b_start + timedelta(days=TRAP_B_DURATION - 1)

    logistics_count = sum(1 for co in companies if "Logistics" in co["industry"])

    trap_notes = {
        "trap_a": (
            f"{TRAP_A_COUNT} leads with gclid/fbclid on session\n"
            "    but offline lead_source in CRM (5-day overwrite delay).\n"
            "    → Detectable via: audit_data_attribution_cleanliness"
        ),
        "trap_b": (
            f"5× organic session surge {trap_b_start.isoformat()} → {trap_b_end.isoformat()} (US geo).\n"
            "    Paid spend flat. Closed-Won opportunities in surge window.\n"
            "    → Detectable via: run_causal_impact_analysis"
        ),
        "trap_c": (
            f"Logistics & Supply Chain ({logistics_count} companies, ARR > $120K)\n"
            f"    represent ~{int(TRAP_C_SHARE*100)}% of closed revenue in last {TRAP_C_WINDOW} days.\n"
            "    → Detectable via: sync_evolving_lookalike_seeds"
        ),
    }

    print_dashboard(wipe_results, counts, trap_notes, elapsed, args.days, today)
    return 0


if __name__ == "__main__":
    sys.exit(main())
