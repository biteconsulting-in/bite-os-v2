# Cloud Storage (GCS) Bucket Inventory & Mapping

## Overview
This document maps every existing folder in the current raw ingestion bucket to its new platform-specific destination bucket. 

**Current Master Bucket:** `o2-data-raw-ingestion`

## Bucket Mapping Strategy
*(This table will be filled out as the audit progresses)*

| Current Folder (`o2-data-raw-ingestion/`) | Owning Platform | Destination Bucket | Migration Required |
| :--- | :--- | :--- | :--- |
| `zomato_o2/` | Zomato | `zomato-raw-ingestion` | Yes |
| `district/` | District | `district-raw-ingestion` | Yes |
| `swiggy/` | Swiggy | `swiggy-raw-ingestion` | Yes |
| *[Add next folder]* | *[...]* | *[...]* | Yes/No |

> **Note:** The migration must preserve timestamps, filenames, and directory hierarchy. Archive folders must remain intact.
