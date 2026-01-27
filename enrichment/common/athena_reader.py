"""Read data from Athena/Iceberg tables"""
import os
from typing import List, Dict, Any
import boto3
from pyathena import connect
from pyathena.cursor import Cursor


class AthenaReader:
    """Read data from Athena/Iceberg tables for enrichment"""

    def __init__(self):
        self.region = os.getenv('AWS_REGION', 'ap-south-1')
        self.s3_output = os.getenv('ATHENA_S3_OUTPUT', 's3://peekit-athena-results-126730103313/output/')
        self.database = os.getenv('ATHENA_DATABASE', 'peekit_crawlers')
        self.workgroup = os.getenv('ATHENA_WORKGROUP', 'peekit-crawlers')

        self.cursor = self._get_cursor()

    def _get_cursor(self) -> Cursor:
        """Create Athena cursor connection"""
        return connect(
            s3_staging_dir=self.s3_output,
            region_name=self.region,
            work_group=self.workgroup
        ).cursor()

    def fetch_unenriched_tweets(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch tweets that haven't been enriched yet

        Args:
            limit: Maximum number of tweets to fetch

        Returns:
            List of tweet dictionaries
        """
        query = f"""
        SELECT
            tweet_id,
            tweet_text,
            author,
            author_handle,
            likes,
            retweets,
            replies,
            views,
            posted_at,
            keyword,
            region,
            scraped_at
        FROM {self.database}.x_tweets
        WHERE tweet_text IS NOT NULL
        ORDER BY scraped_at DESC
        LIMIT {limit}
        """

        print(f"Fetching up to {limit} tweets for enrichment...")
        self.cursor.execute(query)

        # Get column names
        columns = [desc[0] for desc in self.cursor.description] if self.cursor.description else []

        # Fetch rows
        rows = self.cursor.fetchall()

        # Convert to list of dicts
        results = []
        for row in rows:
            record = dict(zip(columns, row))
            results.append(record)

        print(f"Fetched {len(results)} tweets")
        return results

    def fetch_tweets_by_date_range(self, start_date: str, end_date: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Fetch tweets within a date range

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            limit: Maximum number of tweets to fetch

        Returns:
            List of tweet dictionaries
        """
        query = f"""
        SELECT
            tweet_id,
            tweet_text,
            author,
            author_handle,
            likes,
            retweets,
            replies,
            views,
            posted_at,
            keyword,
            region,
            scraped_at
        FROM {self.database}.x_tweets
        WHERE tweet_text IS NOT NULL
        AND date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        ORDER BY scraped_at DESC
        LIMIT {limit}
        """

        print(f"Fetching tweets from {start_date} to {end_date}...")
        self.cursor.execute(query)

        columns = [desc[0] for desc in self.cursor.description] if self.cursor.description else []
        rows = self.cursor.fetchall()

        results = []
        for row in rows:
            record = dict(zip(columns, row))
            results.append(record)

        print(f"Fetched {len(results)} tweets")
        return results
