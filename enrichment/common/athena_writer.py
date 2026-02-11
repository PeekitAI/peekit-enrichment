"""Write enriched data to Athena/Iceberg tables"""
import os
import time
from typing import List, Dict, Any
from datetime import datetime, date
import boto3


class AthenaWriter:
    """Write enriched data to Iceberg tables via Athena"""

    def __init__(self):
        self.region = os.getenv('AWS_REGION', 'ap-south-1')
        self.s3_output = os.getenv('ATHENA_S3_OUTPUT', 's3://peekit-athena-results-126730103313/output/')
        self.database = os.getenv('ATHENA_DATABASE', 'peekit_crawlers')
        self.workgroup = os.getenv('ATHENA_WORKGROUP', 'peekit-crawlers')

        self.athena_client = boto3.client('athena', region_name=self.region)

    def execute_query(self, query: str, wait: bool = True) -> str:
        """Execute Athena query and return execution ID"""
        print(f"Executing query: {query[:100]}...")

        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={'OutputLocation': self.s3_output},
            WorkGroup=self.workgroup
        )

        execution_id = response['QueryExecutionId']

        if wait:
            self._wait_for_query(execution_id)

        return execution_id

    def _wait_for_query(self, execution_id: str, max_wait: int = 300):
        """Wait for query to complete"""
        start_time = time.time()

        while True:
            if time.time() - start_time > max_wait:
                raise TimeoutError(f"Query {execution_id} exceeded max wait time")

            response = self.athena_client.get_query_execution(
                QueryExecutionId=execution_id
            )

            status = response['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                print(f"Query {execution_id} succeeded")
                return
            elif status in ['FAILED', 'CANCELLED']:
                reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                raise Exception(f"Query {execution_id} {status}: {reason}")

            time.sleep(2)

    # ── Unified enrichments table ────────────────────────────────────────

    def create_enrichments_table(self):
        """Create the unified enrichments Iceberg table if it doesn't exist"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.enrichments (
            source_table STRING,
            source_id STRING,
            source_text STRING,
            source_author STRING,
            source_url STRING,
            keyword STRING,
            region STRING,
            scraped_at TIMESTAMP,
            likes BIGINT,
            shares BIGINT,
            comments BIGINT,
            views BIGINT,
            posted_at TIMESTAMP,
            -- Sentiment fields
            sentiment_label STRING,
            sentiment_score DOUBLE,
            sentiment_emotions ARRAY<STRING>,
            sentiment_topics ARRAY<STRING>,
            -- Entity fields
            people ARRAY<STRING>,
            organizations ARRAY<STRING>,
            locations ARRAY<STRING>,
            products ARRAY<STRING>,
            hashtags ARRAY<STRING>,
            mentions ARRAY<STRING>,
            -- Topic classification fields
            primary_category STRING,
            sub_categories ARRAY<STRING>,
            industry STRING,
            keywords ARRAY<STRING>,
            is_commercial BOOLEAN,
            is_news BOOLEAN,
            -- Engagement scoring fields
            engagement_score DOUBLE,
            engagement_rate DOUBLE,
            virality_score DOUBLE,
            interaction_quality DOUBLE,
            time_adjusted_score DOUBLE,
            percentile_score DOUBLE,
            engagement_tier STRING,
            -- Content moderation fields
            is_safe BOOLEAN,
            risk_level STRING,
            flags ARRAY<STRING>,
            content_warnings ARRAY<STRING>,
            recommended_action STRING,
            confidence_score DOUBLE,
            -- Enrichment metadata
            enriched_at TIMESTAMP,
            enrichment_version STRING
        )
        PARTITIONED BY (date DATE)
        LOCATION 's3://peekit-iceberg-data-126730103313/warehouse/enrichments/'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet',
            'write_compression' = 'snappy'
        )
        """

        try:
            self.execute_query(query)
            print("Enrichments table created or already exists")
        except Exception as e:
            print(f"Error creating enrichments table: {e}")
            raise

    def merge_enriched_record(self, record: Dict[str, Any]):
        """
        Merge (upsert) a single enriched record using composite key
        (source_table + source_id).
        """
        columns = list(record.keys())
        values = self._format_values(record)

        set_clause = ', '.join([
            f'"{col}" = {self._format_single_value(record[col])}'
            for col in columns if col not in ('source_table', 'source_id')
        ])

        column_list = ', '.join([f'"{c}"' for c in columns])

        query = f"""
        MERGE INTO {self.database}.enrichments AS target
        USING (SELECT * FROM (VALUES ({values})) AS t ({column_list})) AS source
        ON target.source_table = source.source_table AND target.source_id = source.source_id
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({column_list})
            VALUES ({values})
        """

        try:
            self.execute_query(query, wait=False)
        except Exception as e:
            print(f"Error merging enriched record: {e}")
            raise

    def merge_enriched_records_batch(self, records: List[Dict[str, Any]], batch_size: int = 10):
        """
        Merge enriched records in batches.

        Args:
            records: List of enriched record dictionaries
            batch_size: Number of records to merge per batch
        """
        print(f"Merging {len(records)} enriched records in batches of {batch_size}")

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]

            for record in batch:
                try:
                    self.merge_enriched_record(record)
                except Exception as e:
                    print(f"Error merging record {record.get('source_table')}:{record.get('source_id')}: {e}")

            print(f"Merged batch {i // batch_size + 1} ({len(batch)} records)")
            time.sleep(1)

    # ── Legacy x_tweets_enriched table (kept for backward compat) ────────

    def create_enriched_tweets_table(self):
        """Create the enriched tweets table if it doesn't exist"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.database}.x_tweets_enriched (
            tweet_id STRING,
            tweet_text STRING,
            author STRING,
            author_handle STRING,
            likes BIGINT,
            retweets BIGINT,
            replies BIGINT,
            views BIGINT,
            posted_at TIMESTAMP,
            keyword STRING,
            region STRING,
            scraped_at TIMESTAMP,
            -- Sentiment fields
            sentiment_label STRING,
            sentiment_score DOUBLE,
            sentiment_emotions ARRAY<STRING>,
            sentiment_topics ARRAY<STRING>,
            -- Entity fields
            people ARRAY<STRING>,
            organizations ARRAY<STRING>,
            locations ARRAY<STRING>,
            products ARRAY<STRING>,
            hashtags ARRAY<STRING>,
            mentions ARRAY<STRING>,
            -- Topic classification fields
            primary_category STRING,
            sub_categories ARRAY<STRING>,
            industry STRING,
            keywords ARRAY<STRING>,
            is_commercial BOOLEAN,
            is_news BOOLEAN,
            -- Engagement scoring fields
            engagement_score DOUBLE,
            engagement_rate DOUBLE,
            virality_score DOUBLE,
            interaction_quality DOUBLE,
            time_adjusted_score DOUBLE,
            percentile_score DOUBLE,
            engagement_tier STRING,
            -- Content moderation fields
            is_safe BOOLEAN,
            risk_level STRING,
            flags ARRAY<STRING>,
            content_warnings ARRAY<STRING>,
            recommended_action STRING,
            confidence_score DOUBLE,
            -- Enrichment metadata
            enriched_at TIMESTAMP,
            enrichment_version STRING
        )
        PARTITIONED BY (date DATE)
        LOCATION 's3://peekit-iceberg-data-126730103313/warehouse/x_tweets_enriched/'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet',
            'write_compression' = 'snappy'
        )
        """

        try:
            self.execute_query(query)
            print("Enriched tweets table created or already exists")
        except Exception as e:
            print(f"Error creating enriched tweets table: {e}")
            raise

    def merge_enriched_tweet(self, tweet_data: Dict[str, Any]):
        """
        Merge (upsert) enriched tweet data

        Args:
            tweet_data: Dictionary with all tweet fields including sentiment
        """
        columns = list(tweet_data.keys())
        values = self._format_values(tweet_data)

        # Build SET clause for update (exclude tweet_id)
        set_clause = ', '.join([
            f'"{col}" = {self._format_single_value(tweet_data[col])}'
            for col in columns if col != 'tweet_id'
        ])

        column_list = ', '.join([f'"{c}"' for c in columns])

        query = f"""
        MERGE INTO {self.database}.x_tweets_enriched AS target
        USING (SELECT * FROM (VALUES ({values})) AS t ({column_list})) AS source
        ON target.tweet_id = source.tweet_id
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({column_list})
            VALUES ({values})
        """

        try:
            self.execute_query(query, wait=False)
        except Exception as e:
            print(f"Error merging enriched tweet: {e}")
            raise

    def merge_enriched_tweets_batch(self, tweets: List[Dict[str, Any]], batch_size: int = 10):
        """
        Merge enriched tweets in batches

        Args:
            tweets: List of enriched tweet dictionaries
            batch_size: Number of tweets to merge per batch
        """
        print(f"Merging {len(tweets)} enriched tweets in batches of {batch_size}")

        for i in range(0, len(tweets), batch_size):
            batch = tweets[i:i + batch_size]

            for tweet in batch:
                try:
                    self.merge_enriched_tweet(tweet)
                except Exception as e:
                    print(f"Error merging tweet {tweet.get('tweet_id')}: {e}")

            print(f"Merged batch {i // batch_size + 1} ({len(batch)} tweets)")
            time.sleep(1)  # Small delay between batches

    # ── Shared helpers ────────────────────────────────────────────────────

    def _format_values(self, record: Dict[str, Any]) -> str:
        """Format record values for SQL INSERT"""
        formatted = []

        for value in record.values():
            formatted.append(self._format_single_value(value))

        return ', '.join(formatted)

    def _format_single_value(self, value: Any) -> str:
        """Format a single value for SQL"""
        if value is None:
            return 'NULL'
        elif isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, datetime):
            return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(value, date):
            return f"DATE '{value.strftime('%Y-%m-%d')}'"
        elif isinstance(value, bool):
            return 'true' if value else 'false'
        elif isinstance(value, list):
            formatted_items = [self._format_single_value(item) for item in value]
            return f"ARRAY[{', '.join(formatted_items)}]"
        elif isinstance(value, dict):
            import json
            json_str = json.dumps(value).replace("'", "''")
            return f"'{json_str}'"
        else:
            return str(value)
