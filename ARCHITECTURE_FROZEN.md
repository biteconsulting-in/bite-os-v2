# Architecture Contract Specification (FROZEN)

## 1. Status: LOCKED
This architecture is frozen. No modifications to `framework/` or `shared/` contracts are permitted without an explicit review of responsibility boundaries.

## 2. Location of Framework Contracts
The 13 architecture contracts are distributed across:
- `framework/`: Core engine lifecycle and execution contexts (7 files)
- `shared/`: Utility abstractions for storage, warehouse, archiving, and logging (6 files)

## 3. Extension Points
Pipelines (reports) extending the core framework must implement exactly three methods:
1. `validate()` — Optional custom schema or record validation.
2. `prepare()` — Optional pre-processing of raw structures.
3. `transform()` — Required core transformation business logic.

## 4. Responsibility Boundaries
- **Framework Layer:** Manages routing, event decoding, lifecycle execution, state monitoring, and generic result packaging.
- **Shared Layer:** Provides robust, standardized clients for GCS (Storage), BigQuery (Warehouse), Archiving (Hive partitioning vs. flat structures), Logging, and Common validations.
- **Platform Layer:** Specific platform entry points that inherit from `BasePipeline` and override only the 3 extension points.

## 5. Standard 11-Step Pipeline Lifecycle
Every pipeline executed by the platform follows this strict execution sequence:
1. Decode Event payload
2. Initialize Context
3. Start Logging Session
4. Read Source (Storage)
5. Custom Validation (`validate()`)
6. Pre-process Payload (`prepare()`)
7. Execute Core Logic (`transform()`)
8. Write Destination (Warehouse)
9. Archive File (Archive Strategy)
10. Delete Landing Artifact (Cleanup)
11. Package Result

## 6. Future Change Process
Before proposing a change to `framework/` or `shared/`:
→ **Ask:** "Can this requirement fit within existing contracts?"
→ **Rule:** If the answer is YES, the change must be rejected.
→ **Rule:** If the answer is NO, you must document precisely why the current abstraction cannot fulfill the requirement before changing a single file.
