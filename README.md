# BITE OS V2.0 - Platform Consolidation

## Project Purpose
To consolidate all existing Google Cloud ingestion pipelines into a unified, platform-based architecture. This repository serves as the single source of truth for the entire platform setup and migration.

## Architecture Overview
The new platform standardizes ingestion into a unified pipeline structure:
`Platform` ➔ `Bucket` ➔ `Eventarc Trigger` ➔ `Cloud Run Service` ➔ `Dispatcher` ➔ `Registry` ➔ `Pipeline` ➔ `BigQuery Raw`

## Platform Strategy
Data pipelines will be consolidated into independent platform groupings:
- Zomato (`zomato-platform`)
- Swiggy (`swiggy-platform`)
- District (`district-platform`)
- Dineout (`dineout-platform`)
- Eazydiner (`eazydiner-platform`)
- Petpooja (`petpooja-platform`)

## Migration Phases
**Phase 1 (Current):** Documentation, Inventory, and Repository Setup. 
> **CRITICAL:** Do not modify any Python logic, deploy Cloud Run services, change BigQuery tables, or refactor code during Phase 1.

**Future Phases:** Claude will use this completed repository as the foundation to begin engineering work and implement the new platform architecture.

## Repository Structure
* `/legacy` - Complete, unmodified source code of all existing production services.
* `/framework` - Empty directory structure ready for future development.
* `/docs` - Architecture, contracts, and planning documentation.
* `/inventory` - Audit files mapping current cloud resources to future state.
