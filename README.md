# Peekit Data Enrichment

Data enrichment pipelines for social media crawler data.

## Overview

This repository contains enrichment jobs that process raw crawler data from Iceberg tables and add:
- Sentiment analysis
- Entity extraction
- Topic classification
- Trend detection
- Engagement scoring
- Content moderation flags

## Architecture

### Input
- Raw data from Iceberg tables (x_tweets, youtube_videos, instagram_posts, etc.)
- Reads from AWS Athena/Iceberg

### Processing
- Enrichment jobs run as Kubernetes Jobs/CronJobs
- Uses AWS services (Comprehend, Rekognition, etc.) or custom ML models
- Batch processing with configurable intervals

### Output
- Enriched data written back to Iceberg tables
- Category: Data Enrichment

## Project Structure

```
enrichment/
├── sentiment-analyzer/     # Sentiment analysis for text content
├── entity-extractor/       # Extract entities (people, places, brands)
├── topic-classifier/       # Classify content by topic/category
├── engagement-scorer/      # Calculate engagement scores
├── content-moderator/      # Flag inappropriate content
└── common/                 # Shared utilities
```

## Getting Started

Coming soon - enrichment pipelines will be added here.

## License

Proprietary - Peekit.AI
