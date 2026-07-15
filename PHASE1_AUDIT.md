# Phase 1 - Platform Consolidation Audit (BITE OS V2.0)

## Objective
Consolidate all existing Google Cloud ingestion pipelines into a unified platform-based architecture while preserving all existing business logic, BigQuery schemas, archive structures, and SQL downstream processes. 

> **CRITICAL RULE:** No production services are to be modified during Phase 1. This phase is for documentation and planning only.

---

## Phase 1 Deliverables Status

| File Name | Description | Status |
| :--- | :--- | :--- |
| `PHASE1_AUDIT.md` | Main roadmap, objective, and completion checklist | 🟡 In Progress |
| `ARCHITECTURE.md` | New platform, dispatcher, and registry design | ⚪ Pending |
| `PIPELINE_INVENTORY.md`| Complete catalog of every report pipeline | ⚪ Pending |
| `BUCKET_INVENTORY.md` | Mapping of raw GCS folders to new platform buckets | ⚪ Pending |
| `BIGQUERY_INVENTORY.md` | Catalog of datasets, tables, and ingestion rules | ⚪ Pending |
| `SERVICE_INVENTORY.md` | Inventory of current Cloud Run services and Eventarc triggers | ⚪ Pending |
| `MIGRATION_PLAN.md` | Step-by-step storage transfer and migration strategy | ⚪ Pending |
| `REPORT_CLASSIFICATION.md`| Tier 1 (Direct Load) vs. Tier 2 (Python cleaning) list | ⚪ Pending |

---

## Scope & Platforms Included
1. **Zomato** (`zomato-platform`)
2. **Swiggy** (`swiggy-platform`)
3. **District** (`district-platform`)
4. **Dineout** (`dineout-platform`)
5. **Eazydiner** (`eazydiner-platform`)
6. **Petpooja** (`petpooja-platform`)

---

## Directives for Gemini (What NOT to do)
To prevent operational disruption, the following actions are **strictly prohibited** in Phase 1:
- DO NOT rewrite working Python logic.
- DO NOT rename BigQuery tables or datasets.
- DO NOT rename existing Google Cloud Storage buckets.
- DO NOT change SQL transformations or table schemas.
- DO NOT deploy new services or delete old services.
- DO NOT modify active Eventarc triggers.
- DO NOT change existing service accounts.
