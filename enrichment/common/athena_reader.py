"""Read data from Athena/Iceberg tables"""
import os
from typing import List, Dict, Any
import boto3
from pyathena import connect
from pyathena.cursor import Cursor

from enrichment.providers import ProviderConfig


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

    def fetch_unenriched(self, config: ProviderConfig, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch records from a provider table that haven't been enriched yet.
        Uses an anti-join against the enrichments table.

        Args:
            config: Provider configuration
            limit: Maximum number of records to fetch

        Returns:
            List of record dictionaries
        """
        # Build SELECT columns
        select_cols = [
            f"src.{config.id_column}",
            f"{config.text_column} AS source_text",
        ]

        if config.author_column:
            select_cols.append(f"src.{config.author_column}")
        if config.likes_column:
            select_cols.append(f"src.{config.likes_column}")
        if config.shares_column:
            select_cols.append(f"src.{config.shares_column}")
        if config.comments_column:
            select_cols.append(f"src.{config.comments_column}")
        if config.views_column:
            select_cols.append(f"src.{config.views_column}")
        if config.posted_at_column:
            select_cols.append(f"src.{config.posted_at_column}")
        if config.keyword_column:
            select_cols.append(f"src.{config.keyword_column}")
        if config.region_column:
            select_cols.append(f"src.{config.region_column}")
        if config.url_column:
            select_cols.append(f"src.{config.url_column}")

        select_cols.append("src.scraped_at")

        select_clause = ",\n            ".join(select_cols)

        query = f"""
        SELECT
            {select_clause}
        FROM {self.database}.{config.table} src
        LEFT JOIN {self.database}.enrichments enr
            ON enr.source_table = '{config.table}'
            AND enr.source_id = CAST(src.{config.id_column} AS VARCHAR)
        WHERE ({config.text_column}) IS NOT NULL
            AND enr.source_id IS NULL
        ORDER BY src.scraped_at DESC
        LIMIT {limit}
        """

        print(f"Fetching up to {limit} unenriched records from {config.table}...")
        self.cursor.execute(query)

        columns = [desc[0] for desc in self.cursor.description] if self.cursor.description else []
        rows = self.cursor.fetchall()

        results = []
        for row in rows:
            record = dict(zip(columns, row))
            results.append(record)

        print(f"Fetched {len(results)} records from {config.table}")
        return results

    def fetch_unenriched_tweets(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch tweets that haven't been enriched yet (legacy method).

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

        columns = [desc[0] for desc in self.cursor.description] if self.cursor.description else []
        rows = self.cursor.fetchall()

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
