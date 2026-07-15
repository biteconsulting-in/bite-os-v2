# Architecture Design (Phase 1)

## Overview
This document outlines the target architecture for BITE OS V2.0 platform consolidation. 
**No production services will be modified in Phase 1.**

## New Platform Architecture Flow
The new design standardizes ingestion across all platforms into a single pipeline structure.

**Flow:**
`Platform` ➔ `Bucket` ➔ `One Eventarc Trigger` ➔ `One Cloud Run Service` ➔ `Dispatcher` ➔ `Pipeline Registry` ➔ `Individual Pipeline` ➔ `BigQuery Raw`

## Platform Rules
Each platform (e.g., `zomato-platform`, `swiggy-platform`) will contain the following standard structure:
* `main.py`
* `dispatcher.py`
* `registry.py`
* `shared/`
* `pipelines/`

## Dispatcher Rules
The Dispatcher has strict, limited responsibilities. **It must never contain transformation logic.**
1. Read object path
2. Find matching platform report
3. Execute report
4. Exit

## Report Independence
Every report must remain completely independent to prevent breaking changes.
* **Structure:** Reports live in the `pipelines/` folder (e.g., `ads.py`, `item_sales.py`).
* **Rule:** No report may directly import another report.

## Shared Framework
To reduce duplicated code, ONLY the following core modules will be shared across reports:
* `download()`
* `archive()`
* `delete()`
* `append_to_bigquery()`
* `logger()`
* `error_handler()`

## Deployment Rules & Service Accounts
* **Cloud Run:** One service per platform.
* **Eventarc:** One trigger per platform.
* **Reports:** Unlimited reports per platform.
* **Service Account:** All future deployments must strictly use:
  `o2-data-pipeline-loader@o2-data-s-z.iam.gserviceaccount.com`
  *(No additional ingestion service accounts are permitted).*
