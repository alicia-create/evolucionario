#!/usr/bin/env python3.11
"""
Meta Ads API to Supabase Sync Script
Pulls campaign data from Meta Ads API and syncs to Supabase meta_ads table

Configuration:
- Account ID: 1821954868331644
- Client ID: 0d2c1f21-4cba-403f-ac16-7952325fa65c
- Lookback: 48 hours
- Frequency: Every 3 hours
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import requests
from supabase import create_client
from dotenv import load_dotenv

# Load environment variables from .env file
# override=True ensures .env values take precedence over system env vars
load_dotenv(override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
META_ACCESS_TOKEN = os.getenv('META_ACCESS_TOKEN')
META_ACCOUNT_ID = '1821954868331644'
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
CLIENT_ID = '0d2c1f21-4cba-403f-ac16-7952325fa65c'
LOOKBACK_HOURS = 48

# Meta Graph API
META_API_URL = 'https://graph.facebook.com/v19.0'

# Validated Insights Fields
INSIGHTS_FIELDS = [
    'campaign_id',
    'campaign_name',
    'adset_id',
    'adset_name',
    'ad_id',
    'ad_name',
    'spend',
    'reach',
    'impressions',
    'frequency',
    'clicks',
    'cpm',
    'cpc',
    'ctr',
    'actions',                        # contains leads, purchases, etc.
    'cost_per_action_type',           # cost per lead, cost per purchase
    'video_p25_watched_actions',      # 25% video watched
    'video_p75_watched_actions',      # 75% video watched
    'video_thruplay_watched_actions', # 3-second equivalent (ThruPlay)
    'date_start',
]


def get_action_value(action_list: List[Dict], action_type: str) -> Optional[int]:
    """Extract integer value for a specific action type from actions list."""
    if not action_list:
        return None
    for action in action_list:
        if action.get('action_type') == action_type:
            val = action.get('value')
            return int(float(val)) if val is not None else None
    return None


def get_cost_value(cost_list: List[Dict], action_type: str) -> Optional[float]:
    """Extract float cost value for a specific action type from cost_per_action_type list."""
    if not cost_list:
        return None
    for item in cost_list:
        if item.get('action_type') == action_type:
            val = item.get('value')
            return float(val) if val is not None else None
    return None


def get_video_action_value(action_list: List[Dict]) -> Optional[int]:
    """Extract total value from a video action list (sums all entries)."""
    if not action_list:
        return None
    total = sum(int(float(a.get('value', 0))) for a in action_list if a.get('value'))
    return total if total > 0 else None


class MetaAdsSync:
    """Handles syncing Meta Ads data to Supabase."""

    def __init__(self):
        if not META_ACCESS_TOKEN:
            raise ValueError("META_ACCESS_TOKEN environment variable not set")
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables not set")

        self.access_token = META_ACCESS_TOKEN
        self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

        logger.info(f"Initialized — Account: {META_ACCOUNT_ID} | Client: {CLIENT_ID}")

    def get_date_range(self) -> tuple[str, str]:
        """Return ISO date range covering the last 48 hours."""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(hours=LOOKBACK_HOURS)
        return start_date.isoformat(), end_date.isoformat()

    def fetch_insights(self) -> List[Dict[str, Any]]:
        """Fetch all ad-level insights from Meta Ads API with pagination."""
        start_date, end_date = self.get_date_range()
        logger.info(f"Fetching insights | Date range: {start_date} → {end_date}")

        url = f"{META_API_URL}/act_{META_ACCOUNT_ID}/insights"
        params = {
            'level': 'ad',
            'fields': ','.join(INSIGHTS_FIELDS),
            'time_range': json.dumps({'since': start_date, 'until': end_date}),
            'time_increment': 1,  # One row per day per ad
            'limit': 500,
            'access_token': self.access_token,
        }

        insights = []
        page = 1

        while True:
            logger.info(f"Fetching page {page}...")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if 'error' in data:
                raise Exception(f"Meta API Error: {data['error']['message']}")

            page_data = data.get('data', [])
            insights.extend(page_data)
            logger.info(f"  → {len(page_data)} records (total so far: {len(insights)})")

            # Pagination
            after = data.get('paging', {}).get('cursors', {}).get('after')
            if not after or not data.get('paging', {}).get('next'):
                break

            params['after'] = after
            page += 1

        logger.info(f"Total insights fetched: {len(insights)}")
        return insights

    def transform_insight(self, insight: Dict[str, Any]) -> Dict[str, Any]:
        """Map a Meta Ads insight row to the Supabase meta_ads schema."""
        actions = insight.get('actions', [])
        cost_per_action = insight.get('cost_per_action_type', [])
        video_p25 = insight.get('video_p25_watched_actions', [])
        video_p75 = insight.get('video_p75_watched_actions', [])
        video_thruplay = insight.get('video_thruplay_watched_actions', [])

        def safe_int(val):
            return int(float(val)) if val not in (None, '', '0') else None

        def safe_float(val):
            return float(val) if val not in (None, '', '0') else None

        return {
            'client_id': CLIENT_ID,
            'day': insight.get('date_start'),
            'account_id': META_ACCOUNT_ID,
            'campaign_status': insight.get('campaign_status'),
            'campaign_id': insight.get('campaign_id', ''),
            'campaign_name': insight.get('campaign_name', ''),
            'adset_name': insight.get('adset_name'),
            'ad_name': insight.get('ad_name'),
            'ad_id': insight.get('ad_id', ''),
            'amount_spent': safe_float(insight.get('spend')),
            'reach': safe_int(insight.get('reach')),
            'impressions': safe_int(insight.get('impressions')),
            'frequency': safe_float(insight.get('frequency')),
            'link_clicks': safe_int(insight.get('clicks')),
            'cpm': safe_float(insight.get('cpm')),
            'cpc': safe_float(insight.get('cpc')),
            'ctr': safe_float(insight.get('ctr')),
            'leads': get_action_value(actions, 'lead'),
            'cost_per_lead': get_cost_value(cost_per_action, 'lead'),
            'purchases': get_action_value(actions, 'purchase'),
            'cost_per_purchase': get_cost_value(cost_per_action, 'purchase'),
            'video_views_3s': get_video_action_value(video_thruplay),
            'video_views_25': get_video_action_value(video_p25),
            'video_views_75': get_video_action_value(video_p75),
            'synced_at': datetime.utcnow().isoformat() + 'Z',
        }

    def upsert_data(self, records: List[Dict[str, Any]]) -> None:
        """Upsert records to Supabase in batches to avoid payload limits."""
        if not records:
            logger.info("No records to upsert.")
            return

        BATCH_SIZE = 200
        total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE

        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            logger.info(f"Upserting batch {batch_num}/{total_batches} ({len(batch)} records)...")

            self.supabase.table('meta_ads').upsert(
                batch,
                on_conflict='client_id,day,ad_id'
            ).execute()

        logger.info(f"Successfully upserted {len(records)} records total.")

    def sync(self) -> Dict[str, Any]:
        """Run the full sync pipeline."""
        logger.info("=" * 60)
        logger.info("Starting Meta Ads → Supabase sync")
        logger.info("=" * 60)

        try:
            insights = self.fetch_insights()

            if not insights:
                logger.warning("No insights data returned for the date range.")
                return {
                    'status': 'success',
                    'records_synced': 0,
                    'timestamp': datetime.utcnow().isoformat(),
                }

            records = []
            errors = 0
            for insight in insights:
                try:
                    records.append(self.transform_insight(insight))
                except Exception as e:
                    logger.warning(f"Skipping ad_id={insight.get('ad_id')}: {e}")
                    errors += 1

            logger.info(f"Transformed: {len(records)} records | Skipped: {errors}")

            self.upsert_data(records)

            result = {
                'status': 'success',
                'records_synced': len(records),
                'records_skipped': errors,
                'date_range': self.get_date_range(),
                'timestamp': datetime.utcnow().isoformat(),
            }
            logger.info(f"Sync complete: {result}")
            return result

        except Exception as e:
            logger.error(f"Sync failed: {e}", exc_info=True)
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat(),
            }


def main():
    try:
        syncer = MetaAdsSync()
        result = syncer.sync()
        print(json.dumps(result, indent=2))
        sys.exit(0 if result['status'] == 'success' else 1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(json.dumps({'status': 'error', 'error': str(e)}))
        sys.exit(1)


if __name__ == '__main__':
    main()
