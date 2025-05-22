# HubSpot Employee Sync

A Python automation tool for syncing employee records from Smartsheet (from BambooHR) into HubSpot CRM. The system detects newly added, updated, or removed employees and reflects those changes in HubSpot, while also logging all activity in a centralized Smartsheet control sheet.

## Project Structure

This project is structured around a central sync class (`HubspotEmployeeSync`) that coordinates interactions between:
- Smartsheet (via `grid.py`)
- HubSpot (via `hub_cli.py`)
- Encrypted config and logging modules

The project is organized into:
- `main.py` – entry point for running sync
- `clients/` – API wrappers
- `configs/` – config file, secrets handling, logging
- `dataclasses.py` – shared employee object

## Classes & Features 

### `HubspotEmployeeSync` (`main.py`)
- Coordinates full sync process
- Compares BambooHR data (via Smartsheet) to HubSpot
- Tracks created, updated, deleted, and unchanged records
- Writes change logs to a Smartsheet sheet

#### How it Works
1. `get_bamboo_data()` – Loads current employee records from Smartsheet
2. `get_employees()` – Pulls current contacts from HubSpot
3. `compare_employee_lists()` – 
   - Compares both data sources
   - Returns lists of contacts to `create`, `update`, `delete`, and `unchanged`
4. `batch_create_employees()`, `batch_update()`, `batch_delete()` – Applies changes to HubSpot in 100-record batches
5. `sync_to_sheet()` – 
   - Logs all changes to a Smartsheet control grid
   - Only posts "unchanged" employees if they’re not already logged

---

### `HubspotClient` (`hub_cli.py`)
- Handles all HubSpot API interactions
- Uses batch API calls
- Maps custom fields like `Region`, `Marketing Classification`, etc.

### `grid` (`grid.py`)
- Wrapper for Smartsheet SDK
- Handles reading/writing to Smartsheet control sheets
- Includes row-by-key updates and safe batching

---

## Notes

- Uses email address as the primary identifier for comparing records
- HubSpot `hub_id` is used for delete operations
- Updates are performed using HubSpot's email-based upsert method
- Region mapping logic is customizable via `configs/config.json`

### Assumptions

- Assumes that the BambooHR export sheet includes `firstName`, `lastName`, `emailAsText`, `location`, and `division`
- Assumes Smartsheet contains a control sheet with columns:  
  `Email`, `First Name`, `Last Name`, `Removed`, `Latest Update`, `Comments`
- Assumes region mapping in `config.json` matches division/location structure

### Setup Instructions

1. Clone this repo  
2. Install requirements:

```bash
pip install -r requirements.txt
