# Integrations

This directory contains all backend integrations for the Evolucionario project.

## Available Integrations

| Integration | Description | Schedule |
|---|---|---|
| [meta_ads_sync](./meta_ads_sync/) | Syncs Meta Ads campaign data (FPG account) directly to Supabase | Every 3 hours |

## Architecture

All integrations follow a **direct API-to-database** pattern:

```
Meta Ads API → Python Script → Supabase (meta_ads table)
```

This approach eliminates intermediate tools (e.g., Google Sheets, Zapier) for a more robust and maintainable pipeline.

## Setup

Each integration has its own directory with:
- `sync_*.py` — Main sync script
- `requirements.txt` — Python dependencies
- `.env.example` — Environment variable template
- `README.md` — Documentation

## Environment Variables

Each integration requires a `.env` file (never committed to git). Copy the `.env.example` file and fill in the credentials:

```bash
cp .env.example .env
# Edit .env with your credentials
```

## Scheduling

Scripts are scheduled to run automatically via Manus Scheduler or system cron.
See each integration's README for specific scheduling details.
