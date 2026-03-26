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
load_dotenv()

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

# Meta Graph API Base URL
META_API_URL = 'https://graph.instagram.com/v19.0'

# Insights Fields to fetch
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
    'leads',
    'cost_per_lead',
    'purchase_roas',
    'purchases',
    'cost_per_action_type',
    'video_3_second_views',
    'video_p25_watched_actions',
    'video_p75_watched_actions',
    'date_start',
]


class MetaAdsSync:
    """Handles syncing Meta Ads data to Supabase"""

    def __init__(self):
        """Initialize Meta Ads API and Supabase client"""
        if not META_ACCESS_TOKEN:
            raise ValueError("META_ACCESS_TOKEN environment variable not set")
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables not set")

        self.access_token = META_ACCESS_TOKEN
        self.account_id = META_ACCOUNT_ID
        
        # Initialize Supabase
        self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        logger.info(f"Initialized Meta Ads API for account {META_ACCOUNT_ID}")
        logger.info(f"Initialized Supabase client")

    def get_date_range(self) -> tuple[str, str]:
        """Get date range for insights (last 48 hours)"""
        end_date = datetime.now().date()
        start_date = end_date - timedelta(hours=LOOKBACK_HOURS)
        
        return start_date.isoformat(), end_date.isoformat()

    def fetch_insights(self) -> List[Dict[str, Any]]:
        """Fetch insights data from Meta Ads API"""
        logger.info("Fetching insights from Meta Ads API...")
        
        start_date, end_date = self.get_date_range()
        logger.info(f"Date range: {start_date} to {end_date}")

        try:
            url = f"{META_API_URL}/act_{self.account_id}/insights"
            
            params = {
                'level': 'ad',
                'fields': ','.join(INSIGHTS_FIELDS),
                'time_range': json.dumps({
                    'since': start_date,
                    'until': end_date,
                }),
                'limit': 1000,
                'access_token': self.access_token,
            }

            insights = []
            after_cursor = None
            page_count = 0

            while True:
                if after_cursor:
                    params['after'] = after_cursor

                logger.info(f"Fetching page {page_count + 1}...")
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()

                data = response.json()
                
                if 'error' in data:
                    raise Exception(f"Meta API Error: {data['error']}")

                page_data = data.get('data', [])
                insights.extend(page_data)
                logger.info(f"Fetched {len(page_data)} records from page {page_count + 1}")

                page_count += 1

                # Check for next page
                paging = data.get('paging', {})
                after_cursor = paging.get('cursors', {}).get('after')
                
                if not after_cursor:
                    break

            logger.info(f"Total insights fetched: {len(insights)}")
            return insights

        except Exception as e:
            logger.error(f"Error fetching insights: {str(e)}")
            raise

    def transform_insight(self, insight: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Meta Ads insight to Supabase schema"""
        
        # Extract cost_per_lead from cost_per_action_type
        cost_per_lead = None
        if 'cost_per_action_type' in insight and insight['cost_per_action_type']:
            for action in insight['cost_per_action_type']:
                if action.get('action_type') == 'lead':
                    cost_per_lead = float(action.get('value', 0))
                    break

        # Extract cost_per_purchase from cost_per_action_type
        cost_per_purchase = None
        if 'cost_per_action_type' in insight and insight['cost_per_action_type']:
            for action in insight['cost_per_action_type']:
                if action.get('action_type') == 'purchase':
                    cost_per_purchase = float(action.get('value', 0))
                    break

        # Extract video views
        video_views_3s = None
        video_views_25 = None
        video_views_75 = None
        
        if 'video_3_second_views' in insight:
            video_views_3s = int(insight['video_3_second_views']) if insight['video_3_second_views'] else None
        if 'video_p25_watched_actions' in insight:
            video_views_25 = int(insight['video_p25_watched_actions']) if insight['video_p25_watched_actions'] else None
        if 'video_p75_watched_actions' in insight:
            video_views_75 = int(insight['video_p75_watched_actions']) if insight['video_p75_watched_actions'] else None

        # Parse date
        date_str = insight.get('date_start', datetime.now().date().isoformat())

        record = {
            'client_id': CLIENT_ID,
            'day': date_str,
            'account_id': META_ACCOUNT_ID,
            'campaign_status': insight.get('campaign_status'),
            'campaign_id': insight.get('campaign_id', ''),
            'campaign_name': insight.get('campaign_name', ''),
            'adset_name': insight.get('adset_name'),
            'ad_name': insight.get('ad_name'),
            'ad_id': insight.get('ad_id', ''),
            'amount_spent': float(insight.get('spend', 0)) if insight.get('spend') else None,
            'reach': int(insight.get('reach', 0)) if insight.get('reach') else None,
            'impressions': int(insight.get('impressions', 0)) if insight.get('impressions') else None,
            'frequency': float(insight.get('frequency', 0)) if insight.get('frequency') else None,
            'link_clicks': int(insight.get('clicks', 0)) if insight.get('clicks') else None,
            'cpm': float(insight.get('cpm', 0)) if insight.get('cpm') else None,
            'cpc': float(insight.get('cpc', 0)) if insight.get('cpc') else None,
            'ctr': float(insight.get('ctr', 0)) if insight.get('ctr') else None,
            'leads': int(insight.get('leads', 0)) if insight.get('leads') else None,
            'cost_per_lead': cost_per_lead,
            'purchases': int(insight.get('purchases', 0)) if insight.get('purchases') else None,
            'cost_per_purchase': cost_per_purchase,
            'video_views_3s': video_views_3s,
            'video_views_25': video_views_25,
            'video_views_75': video_views_75,
            'synced_at': datetime.utcnow().isoformat() + 'Z',
        }

        return record

    def upsert_data(self, records: List[Dict[str, Any]]) -> None:
        """Upsert records to Supabase using UPSERT strategy"""
        if not records:
            logger.info("No records to upsert")
            return

        logger.info(f"Upserting {len(records)} records to Supabase...")

        try:
            # Supabase upsert with conflict resolution on unique constraint
            # (client_id, day, ad_id)
            response = self.supabase.table('meta_ads').upsert(
                records,
                ignore_duplicates=False  # Use upsert to update existing records
            ).execute()

            logger.info(f"Successfully upserted {len(records)} records")
            logger.debug(f"Response: {response}")

        except Exception as e:
            logger.error(f"Error upserting data: {str(e)}")
            raise

    def sync(self) -> Dict[str, Any]:
        """Execute full sync process"""
        logger.info("Starting Meta Ads sync...")
        
        try:
            # Fetch insights
            insights = self.fetch_insights()
            
            if not insights:
                logger.warning("No insights data fetched")
                return {
                    'status': 'success',
                    'records_synced': 0,
                    'timestamp': datetime.utcnow().isoformat(),
                }

            # Transform insights
            logger.info("Transforming insights...")
            records = []
            for insight in insights:
                try:
                    record = self.transform_insight(insight)
                    records.append(record)
                except Exception as e:
                    logger.warning(f"Error transforming insight {insight.get('ad_id')}: {str(e)}")
                    continue

            logger.info(f"Transformed {len(records)} records")

            # Upsert to Supabase
            self.upsert_data(records)

            logger.info("Sync completed successfully")
            return {
                'status': 'success',
                'records_synced': len(records),
                'timestamp': datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.error(f"Sync failed: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat(),
            }


def main():
    """Main entry point"""
    try:
        sync = MetaAdsSync()
        result = sync.sync()
        
        print(json.dumps(result, indent=2))
        
        if result['status'] == 'error':
            sys.exit(1)
        
        sys.exit(0)

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(json.dumps({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat(),
        }))
        sys.exit(1)


if __name__ == '__main__':
    main()
