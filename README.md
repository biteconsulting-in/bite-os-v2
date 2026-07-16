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

## Frozen Architecture

The Bite OS V2.0 architecture is locked and documented in:
- **[ARCHITECTURE_FROZEN.md](ARCHITECTURE_FROZEN.md)** — Current frozen state
- **framework/** directory — 7 contract files
- **shared/** directory — 6 contract files

Extension points (reports override exactly 3 methods):
1. `validate()` — optional custom validation
2. `prepare()` — optional pre-processing
3. `transform()` — required business logic

Before modifying framework or shared:
→ Ask: "Can this fit within existing contracts?"
→ If NO → stop and explain why

Full details: See [ARCHITECTURE_FROZEN.md](ARCHITECTURE_FROZEN.md)

## Frozen Architecture

The Bite OS V2.0 architecture is locked and documented in:
- **[ARCHITECTURE_FROZEN.md](ARCHITECTURE_FROZEN.md)** — Current frozen state
- **framework/** directory — 7 contract files
- **shared/** directory — 6 contract files

Extension points (reports override exactly 3 methods):
1. `validate()` — optional custom validation
2. `prepare()` — optional pre-processing
3. `transform()` — required business logic

Before modifying framework or shared:
→ Ask: "Can this fit within existing contracts?"
→ If NO → stop and explain why

Full details: See [ARCHITECTURE_FROZEN.md](ARCHITECTURE_FROZEN.md)

## Frozen Architecture

The Bite OS V2.0 architecture is locked and documented in:
- **[ARCHITECTURE_FROZEN.md](ARCHITECTURE_FROZEN.md)** — Current frozen state
- **framework/** directory — 7 contract files
- **shared/** directory — 6 contract files

Extension points (reports override exactly 3 methods):
1. `validate()` — optional custom validation
2. `prepare()` — optional pre-processing
3. `transform()` — required business logic

Before modifying framework or shared:
→ Ask: "Can this fit within existing contracts?"
→ If NO → stop and explain why

Full details: See [ARCHITECTURE_FROZEN.md](ARCHITECTURE_FROZEN.md)
