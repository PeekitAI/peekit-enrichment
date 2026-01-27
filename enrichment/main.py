"""Main enrichment pipeline - orchestrates all enrichment modules"""
import os
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any

from enrichment.common import AthenaReader, AthenaWriter
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
        # Initialize components
        self.reader = AthenaReader()
        self.writer = AthenaWriter()

        # Initialize modules based on config
        self.modules_enabled = {
            'sentiment': os.getenv('ENABLE_SENTIMENT', 'true').lower() == 'true',
            'entity': os.getenv('ENABLE_ENTITY', 'true').lower() == 'true',
            'topic': os.getenv('ENABLE_TOPIC', 'true').lower() == 'true',
            'engagement': os.getenv('ENABLE_ENGAGEMENT', 'true').lower() == 'true',
            'moderation': os.getenv('ENABLE_MODERATION', 'true').lower() == 'true',
        }

        # Initialize enabled modules
        self.sentiment_analyzer = None
        self.entity_extractor = None
        self.topic_classifier = None
        self.engagement_scorer = None
        self.content_moderator = None

        if self.modules_enabled['sentiment']:
            try:
                self.sentiment_analyzer = SentimentAnalyzer()
                print("✓ Sentiment analyzer initialized")
            except Exception as e:
                print(f"✗ Failed to initialize sentiment analyzer: {e}")
                self.modules_enabled['sentiment'] = False

        if self.modules_enabled['entity']:
            try:
                self.entity_extractor = EntityExtractor()
                print("✓ Entity extractor initialized")
            except Exception as e:
                print(f"✗ Failed to initialize entity extractor: {e}")
                self.modules_enabled['entity'] = False

        if self.modules_enabled['topic']:
            try:
                self.topic_classifier = TopicClassifier()
                print("✓ Topic classifier initialized")
            except Exception as e:
                print(f"✗ Failed to initialize topic classifier: {e}")
                self.modules_enabled['topic'] = False

        if self.modules_enabled['engagement']:
            try:
                self.engagement_scorer = EngagementScorer()
                print("✓ Engagement scorer initialized")
            except Exception as e:
                print(f"✗ Failed to initialize engagement scorer: {e}")
                self.modules_enabled['engagement'] = False

        if self.modules_enabled['moderation']:
            try:
                self.content_moderator = ContentModerator()
                print("✓ Content moderator initialized")
            except Exception as e:
                print(f"✗ Failed to initialize content moderator: {e}")
                self.modules_enabled['moderation'] = False

    def run(self, limit: int = 100, batch_size: int = 10):
        """
        Run the enrichment pipeline

        Args:
            limit: Maximum number of tweets to process
            batch_size: Number of tweets to write per batch
        """
        print("=" * 80)
        print("Peekit Data Enrichment Pipeline")
        print("=" * 80)
        print(f"Enabled modules: {[k for k, v in self.modules_enabled.items() if v]}")
        print()

        # Step 1: Ensure enriched table exists
        print("Step 1: Setting up enriched tweets table...")
        try:
            self.writer.create_enriched_tweets_table()
            print("✓ Enriched table ready")
        except Exception as e:
            print(f"✗ Failed to create enriched table: {e}")
            sys.exit(1)

        # Step 2: Fetch tweets to enrich
        print(f"\nStep 2: Fetching up to {limit} tweets...")
        try:
            tweets = self.reader.fetch_unenriched_tweets(limit=limit)
            if not tweets:
                print("No tweets to enrich. Exiting.")
                return
            print(f"✓ Fetched {len(tweets)} tweets")
        except Exception as e:
            print(f"✗ Failed to fetch tweets: {e}")
            sys.exit(1)

        # Step 3: Enrich tweets
        print(f"\nStep 3: Enriching {len(tweets)} tweets...")
        enriched_tweets = []

        for i, tweet in enumerate(tweets, 1):
            print(f"Processing tweet {i}/{len(tweets)}: {tweet.get('tweet_id')}")

            enriched = self._enrich_tweet(tweet)
            enriched_tweets.append(enriched)

            # Progress indicator
            if i % 10 == 0:
                print(f"  Processed {i}/{len(tweets)} tweets...")

        print(f"✓ Enriched {len(enriched_tweets)} tweets")

        # Step 4: Write enriched data
        print(f"\nStep 4: Writing enriched tweets to Iceberg...")
        try:
            self.writer.merge_enriched_tweets_batch(enriched_tweets, batch_size=batch_size)
            print(f"✓ Successfully wrote {len(enriched_tweets)} enriched tweets")
        except Exception as e:
            print(f"✗ Failed to write enriched tweets: {e}")
            sys.exit(1)

        print("\n" + "=" * 80)
        print("Enrichment pipeline completed successfully!")
        print("=" * 80)

    def _enrich_tweet(self, tweet: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single tweet with all enabled modules

        Args:
            tweet: Original tweet data

        Returns:
            Tweet with enrichment fields added
        """
        enriched = tweet.copy()

        # Add enrichment metadata
        enriched['enriched_at'] = datetime.now(timezone.utc)
        enriched['enrichment_version'] = '1.0.0'

        # Add partition date
        enriched['date'] = datetime.now(timezone.utc).date()

        # Run sentiment analysis if enabled
        if self.modules_enabled['sentiment'] and self.sentiment_analyzer:
            try:
                sentiment = self.sentiment_analyzer.analyze(tweet.get('tweet_text', ''))
                enriched.update(sentiment)
            except Exception as e:
                print(f"  Warning: Sentiment analysis failed: {e}")
                enriched.update({
                    'sentiment_label': 'unknown',
                    'sentiment_score': 0.0,
                    'sentiment_emotions': [],
                    'sentiment_topics': []
                })

        # Run entity extraction if enabled
        if self.modules_enabled['entity'] and self.entity_extractor:
            try:
                entities = self.entity_extractor.extract(tweet.get('tweet_text', ''))
                enriched.update(entities)
            except Exception as e:
                print(f"  Warning: Entity extraction failed: {e}")
                enriched.update({
                    'people': [],
                    'organizations': [],
                    'locations': [],
                    'products': [],
                    'hashtags': [],
                    'mentions': []
                })

        # Run topic classification if enabled
        if self.modules_enabled['topic'] and self.topic_classifier:
            try:
                topics = self.topic_classifier.classify(
                    tweet.get('tweet_text', ''),
                    tweet.get('author', '')
                )
                enriched.update(topics)
            except Exception as e:
                print(f"  Warning: Topic classification failed: {e}")
                enriched.update({
                    'primary_category': 'Other',
                    'sub_categories': [],
                    'industry': 'Unknown',
                    'keywords': [],
                    'is_commercial': False,
                    'is_news': False
                })

        # Run engagement scoring if enabled
        if self.modules_enabled['engagement'] and self.engagement_scorer:
            try:
                engagement = self.engagement_scorer.score(tweet)
                enriched.update(engagement)
            except Exception as e:
                print(f"  Warning: Engagement scoring failed: {e}")
                enriched.update({
                    'engagement_score': 0.0,
                    'engagement_rate': 0.0,
                    'virality_score': 0.0,
                    'interaction_quality': 0.0,
                    'time_adjusted_score': 0.0,
                    'percentile_score': 0.0,
                    'engagement_tier': 'Low'
                })

        # Run content moderation if enabled
        if self.modules_enabled['moderation'] and self.content_moderator:
            try:
                moderation = self.content_moderator.moderate(tweet.get('tweet_text', ''))
                enriched.update(moderation)
            except Exception as e:
                print(f"  Warning: Content moderation failed: {e}")
                enriched.update({
                    'is_safe': True,
                    'risk_level': 'safe',
                    'flags': [],
                    'content_warnings': [],
                    'recommended_action': 'none',
                    'confidence_score': 1.0
                })

        return enriched


def main():
    """Main entry point"""
    # Configuration from environment variables
    limit = int(os.getenv('ENRICHMENT_LIMIT', '100'))
    batch_size = int(os.getenv('ENRICHMENT_BATCH_SIZE', '10'))

    # Create and run pipeline
    pipeline = EnrichmentPipeline()
    pipeline.run(limit=limit, batch_size=batch_size)


if __name__ == '__main__':
    main()
