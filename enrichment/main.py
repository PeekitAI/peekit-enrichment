"""Main enrichment pipeline - orchestrates all enrichment modules across providers"""
import os
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any

from enrichment.common import AthenaReader, AthenaWriter
from enrichment.providers import PROVIDERS, ProviderConfig, normalize_record
from enrichment.modules import (
    SentimentAnalyzer,
    EntityExtractor,
    TopicClassifier,
    EngagementScorer,
    ContentModerator
)


class EnrichmentPipeline:
    """Main enrichment pipeline coordinator"""

    def __init__(self):
        self.reader = AthenaReader()
        self.writer = AthenaWriter()

        # Determine which providers to process
        providers_env = os.getenv('ENRICHMENT_PROVIDERS', '')
        if providers_env.strip():
            selected = [p.strip() for p in providers_env.split(',') if p.strip()]
            self.providers = {k: v for k, v in PROVIDERS.items() if k in selected}
            unknown = set(selected) - set(PROVIDERS.keys())
            if unknown:
                print(f"Warning: Unknown providers ignored: {unknown}")
        else:
            self.providers = dict(PROVIDERS)

        # Initialize modules based on config
        self.modules_enabled = {
            'sentiment': os.getenv('ENABLE_SENTIMENT', 'true').lower() == 'true',
            'entity': os.getenv('ENABLE_ENTITY', 'true').lower() == 'true',
            'topic': os.getenv('ENABLE_TOPIC', 'true').lower() == 'true',
            'engagement': os.getenv('ENABLE_ENGAGEMENT', 'true').lower() == 'true',
            'moderation': os.getenv('ENABLE_MODERATION', 'true').lower() == 'true',
        }

        self.sentiment_analyzer = None
        self.entity_extractor = None
        self.topic_classifier = None
        self.engagement_scorer = None
        self.content_moderator = None

        if self.modules_enabled['sentiment']:
            try:
                self.sentiment_analyzer = SentimentAnalyzer()
                print("  Sentiment analyzer initialized")
            except Exception as e:
                print(f"  Failed to initialize sentiment analyzer: {e}")
                self.modules_enabled['sentiment'] = False

        if self.modules_enabled['entity']:
            try:
                self.entity_extractor = EntityExtractor()
                print("  Entity extractor initialized")
            except Exception as e:
                print(f"  Failed to initialize entity extractor: {e}")
                self.modules_enabled['entity'] = False

        if self.modules_enabled['topic']:
            try:
                self.topic_classifier = TopicClassifier()
                print("  Topic classifier initialized")
            except Exception as e:
                print(f"  Failed to initialize topic classifier: {e}")
                self.modules_enabled['topic'] = False

        if self.modules_enabled['engagement']:
            try:
                self.engagement_scorer = EngagementScorer()
                print("  Engagement scorer initialized")
            except Exception as e:
                print(f"  Failed to initialize engagement scorer: {e}")
                self.modules_enabled['engagement'] = False

        if self.modules_enabled['moderation']:
            try:
                self.content_moderator = ContentModerator()
                print("  Content moderator initialized")
            except Exception as e:
                print(f"  Failed to initialize content moderator: {e}")
                self.modules_enabled['moderation'] = False

    def run(self, limit: int = 100, batch_size: int = 10):
        """
        Run the enrichment pipeline across all configured providers.

        Args:
            limit: Maximum number of records to process per provider
            batch_size: Number of records to write per batch
        """
        print("=" * 80)
        print("Peekit Data Enrichment Pipeline")
        print("=" * 80)
        print(f"Providers: {list(self.providers.keys())}")
        print(f"Enabled modules: {[k for k, v in self.modules_enabled.items() if v]}")
        print()

        # Step 1: Ensure enrichments table exists
        print("Step 1: Setting up enrichments table...")
        try:
            self.writer.create_enrichments_table()
            print("  Enrichments table ready")
        except Exception as e:
            print(f"  Failed to create enrichments table: {e}")
            sys.exit(1)

        # Step 2: Process each provider
        total_enriched = 0

        for provider_name, config in self.providers.items():
            print(f"\n{'─' * 60}")
            print(f"Processing provider: {provider_name}")
            print(f"{'─' * 60}")

            # Fetch unenriched records
            try:
                raw_records = self.reader.fetch_unenriched(config, limit=limit)
                if not raw_records:
                    print(f"  No unenriched records for {provider_name}. Skipping.")
                    continue
            except Exception as e:
                print(f"  Failed to fetch from {provider_name}: {e}")
                continue

            # Normalize records
            normalized = [normalize_record(r, config) for r in raw_records]

            # Enrich each record
            enriched_records = []
            for i, record in enumerate(normalized, 1):
                print(f"  Enriching {i}/{len(normalized)}: {record['source_id']}")

                enriched = self._enrich_record(record)
                enriched_records.append(enriched)

                if i % 10 == 0:
                    print(f"    Processed {i}/{len(normalized)} records...")

            print(f"  Enriched {len(enriched_records)} records from {provider_name}")

            # Write enriched records
            try:
                self.writer.merge_enriched_records_batch(enriched_records, batch_size=batch_size)
                total_enriched += len(enriched_records)
                print(f"  Wrote {len(enriched_records)} records for {provider_name}")
            except Exception as e:
                print(f"  Failed to write enriched records for {provider_name}: {e}")

        print(f"\n{'=' * 80}")
        print(f"Enrichment pipeline completed. Total records enriched: {total_enriched}")
        print("=" * 80)

    def _enrich_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single normalized record with all enabled modules.

        The record dict uses the normalized keys: text, author, likes,
        retweets, replies, views, posted_at, etc.

        After enrichment, renames keys to match the enrichments table schema:
        text -> source_text, author -> source_author, retweets -> shares,
        replies -> comments.
        """
        enriched = record.copy()

        text = record.get('text', '')
        author = record.get('author', '')

        # Enrichment metadata
        enriched['enriched_at'] = datetime.now(timezone.utc)
        enriched['enrichment_version'] = '1.0.0'
        enriched['date'] = datetime.now(timezone.utc).date()

        # Run sentiment analysis
        if self.modules_enabled['sentiment'] and self.sentiment_analyzer:
            try:
                sentiment = self.sentiment_analyzer.analyze(text)
                enriched.update(sentiment)
            except Exception as e:
                print(f"    Warning: Sentiment analysis failed: {e}")
                enriched.update({
                    'sentiment_label': 'unknown',
                    'sentiment_score': 0.0,
                    'sentiment_emotions': [],
                    'sentiment_topics': []
                })

        # Run entity extraction
        if self.modules_enabled['entity'] and self.entity_extractor:
            try:
                entities = self.entity_extractor.extract(text)
                enriched.update(entities)
            except Exception as e:
                print(f"    Warning: Entity extraction failed: {e}")
                enriched.update({
                    'people': [],
                    'organizations': [],
                    'locations': [],
                    'products': [],
                    'hashtags': [],
                    'mentions': []
                })

        # Run topic classification
        if self.modules_enabled['topic'] and self.topic_classifier:
            try:
                topics = self.topic_classifier.classify(text, author)
                enriched.update(topics)
            except Exception as e:
                print(f"    Warning: Topic classification failed: {e}")
                enriched.update({
                    'primary_category': 'Other',
                    'sub_categories': [],
                    'industry': 'Unknown',
                    'keywords': [],
                    'is_commercial': False,
                    'is_news': False
                })

        # Run engagement scoring - uses likes, retweets, replies, views keys
        if self.modules_enabled['engagement'] and self.engagement_scorer:
            try:
                engagement = self.engagement_scorer.score(record)
                enriched.update(engagement)
            except Exception as e:
                print(f"    Warning: Engagement scoring failed: {e}")
                enriched.update({
                    'engagement_score': 0.0,
                    'engagement_rate': 0.0,
                    'virality_score': 0.0,
                    'interaction_quality': 0.0,
                    'time_adjusted_score': 0.0,
                    'percentile_score': 0.0,
                    'engagement_tier': 'Low'
                })

        # Run content moderation
        if self.modules_enabled['moderation'] and self.content_moderator:
            try:
                moderation = self.content_moderator.moderate(text)
                enriched.update(moderation)
            except Exception as e:
                print(f"    Warning: Content moderation failed: {e}")
                enriched.update({
                    'is_safe': True,
                    'risk_level': 'safe',
                    'flags': [],
                    'content_warnings': [],
                    'recommended_action': 'none',
                    'confidence_score': 1.0
                })

        # Rename normalized keys to match enrichments table schema
        enriched['source_text'] = enriched.pop('text', '')
        enriched['source_author'] = enriched.pop('author', '')
        enriched['shares'] = enriched.pop('retweets', 0)
        enriched['comments'] = enriched.pop('replies', 0)

        return enriched


def main():
    """Main entry point"""
    limit = int(os.getenv('ENRICHMENT_LIMIT', '100'))
    batch_size = int(os.getenv('ENRICHMENT_BATCH_SIZE', '10'))

    pipeline = EnrichmentPipeline()
    pipeline.run(limit=limit, batch_size=batch_size)


if __name__ == '__main__':
    main()
