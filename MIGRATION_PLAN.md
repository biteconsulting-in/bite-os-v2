# Data Migration Plan (Phase 1)

## Overview
This document outlines the strategy for migrating historical files from the current raw ingestion bucket to the new platform-specific buckets.

**Source Bucket:** `o2-data-raw-ingestion`

## Migration Requirements
To ensure absolute data integrity, the migration MUST adhere to the following strict rules:
- Historical data must remain intact.
- Folder hierarchy must remain intact.
- Archive folders must remain intact.
- No data loss and no duplicate files.
- Migration must be restartable.

## Tooling
Migration should preferably use one of the following tools rather than Python (unless a transformation is required):
- Storage Transfer Service
- `gsutil`
- `gcloud storage`

## Preservation Rules
The migration process must preserve:
- Timestamps (where possible)
- Filenames
- Directory hierarchy

## Migration Order Strategy
Never migrate all reports simultaneously. The migration must follow this exact sequence:
1. Audit (Current Phase)
2. Framework Setup
3. One Platform
4. One Report
5. Testing
6. Production
7. Retire Old Service
