# Meta Ads to Supabase Sync

Automated synchronization of Meta Ads campaign data directly to Supabase database.

## Overview

This solution pulls campaign performance data from Meta Ads API and syncs it to the Supabase `meta_ads` table. The sync runs every 3 hours and retrieves the last 48 hours of data.

**Key Features:**
- Direct API-to-database integration (no intermediate tools)
- Automatic conflict resolution using UPSERT on unique constraint `(client_id, day, ad_id)`
- Comprehensive error handling and logging
- Supports all campaign metrics (spend, reach, impressions, leads, purchases, video views, etc.)

## Configuration

### Prerequisites

- Python 3.11+
- Meta Ads API access token with appropriate permissions
- Supabase project with `meta_ads` table created

### Setup

1. **Install dependencies:**
   ```bash
   pip install facebook-business supabase python-dotenv
   ```

2. **Create `.env` file:**
   ```bash
   cp .env.example .env
   ```

3. **Add credentials to `.env`:**
   ```
   META_ACCESS_TOKEN=your_token_here
   SUPABASE_URL=https://ctdjgjsibgwrqkdnztsx.supabase.co
   SUPABASE_KEY=your_key_here
   ```

## Usage

### Manual Execution

```bash
python3.11 sync_meta_ads.py
```

### Scheduled Execution (Every 3 hours)

The script can be scheduled using:
- **Linux Cron:** `0 */3 * * * cd /home/ubuntu/meta_ads_sync && python3.11 sync_meta_ads.py`
- **Manus Scheduler:** See scheduling configuration below

## Data Mapping

| Meta Ads Field | Supabase Column | Type | Notes |
|---|---|---|---|
| campaign_id | campaign_id | text | Primary identifier |
| campaign_name | campaign_name | text | Campaign name |
| adset_name | adset_name | text | Ad set name |
| ad_name | ad_name | text | Ad creative name |
| ad_id | ad_id | text | Ad identifier |
| spend | amount_spent | numeric | In account currency |
| reach | reach | integer | Unique users reached |
| impressions | impressions | integer | Total impressions |
| frequency | frequency | numeric | Avg impressions per user |
| clicks | link_clicks | integer | Link clicks |
| cpm | cpm | numeric | Cost per 1,000 impressions |
| cpc | cpc | numeric | Cost per click |
| ctr | ctr | numeric | Click-through rate |
| leads | leads | integer | Lead conversions |
| cost_per_lead | cost_per_lead | numeric | Derived from cost_per_action_type |
| purchases | purchases | numeric | Purchase conversions |
| cost_per_purchase | cost_per_purchase | numeric | Derived from cost_per_action_type |
| video_3_second_views | video_views_3s | integer | 3-second video views |
| video_p25_watched_actions | video_views_25 | integer | 25% video watched |
| video_p75_watched_actions | video_views_75 | integer | 75% video watched |
| date_start | day | date | Report date |

## Configuration Details

**Account Information:**
- Meta Account ID: `1821954868331644`
- Supabase Client ID: `0d2c1f21-4cba-403f-ac16-7952325fa65c`

**Sync Parameters:**
- Lookback Period: 48 hours
- Frequency: Every 3 hours
- Conflict Resolution: UPSERT on `(client_id, day, ad_id)`

## Logging

The script outputs detailed logs to console. Log levels:
- `INFO`: Normal operation messages
- `WARNING`: Non-critical issues
- `ERROR`: Sync failures

Example output:
```
2024-01-15 10:30:45 - __main__ - INFO - Starting Meta Ads sync...
2024-01-15 10:30:45 - __main__ - INFO - Date range: 2024-01-13 to 2024-01-15
2024-01-15 10:30:47 - __main__ - INFO - Total insights fetched: 245
2024-01-15 10:30:47 - __main__ - INFO - Transformed 245 records
2024-01-15 10:30:48 - __main__ - INFO - Successfully upserted 245 records
2024-01-15 10:30:48 - __main__ - INFO - Sync completed successfully
```

## Troubleshooting

### Authentication Errors

**Error:** `Invalid access token`
- Verify `META_ACCESS_TOKEN` is valid and has required permissions
- Token must have access to the ad account `1821954868331644`

**Error:** `Supabase authentication failed`
- Verify `SUPABASE_URL` and `SUPABASE_KEY` are correct
- Check that the Supabase project is active

### Data Issues

**No data returned:**
- Check if campaigns have data in the 48-hour window
- Verify campaigns are active and have spend

**Duplicate records:**
- The UPSERT strategy handles duplicates automatically
- Records are identified by `(client_id, day, ad_id)`

## Performance Considerations

- **API Rate Limits:** Meta Ads API has rate limits. For large accounts with many ads, consider increasing the sync interval
- **Data Freshness:** Data is typically available 24-48 hours after the campaign runs
- **Supabase Limits:** Check your Supabase plan for row limits and API rate limits

## Maintenance

### Monitoring

Monitor sync success by checking:
1. Supabase table for recent `synced_at` timestamps
2. Script logs for errors
3. Supabase API usage metrics

### Updates

When updating the script:
1. Test changes in a development environment
2. Verify data integrity before deploying to production
3. Monitor first few sync cycles after deployment

## Support

For issues or questions:
1. Check logs for specific error messages
2. Verify all credentials are correct
3. Ensure Supabase schema matches expected structure
