# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Peekit Data Enrichment — a Python pipeline that reads content from multiple provider tables in AWS Athena (`peekit_crawlers`), enriches them through 5 modules, and writes results to a unified Iceberg table (`peekit_crawlers.enrichments`) via MERGE/upsert on `(source_table, source_id)`.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run pipeline (default 100 records per provider, all providers)
python -m enrichment.main

# Run with custom limit
ENRICHMENT_LIMIT=500 python -m enrichment.main

# Run specific providers only
ENRICHMENT_PROVIDERS=x_tweets,instagram_posts python -m enrichment.main
```

No tests or linting are configured yet.

## Architecture

**Pipeline flow:** `ProviderConfig → AthenaReader → normalize_record() → [Module Chain] → AthenaWriter`

The orchestrator is `EnrichmentPipeline` in `enrichment/main.py`. For each enabled provider, it fetches unenriched records (anti-join against `enrichments`), normalizes them via `providers.py`, runs each through enabled modules, then writes to the unified `enrichments` table via Iceberg MERGE.

### Provider Registry (`enrichment/providers.py`)

`ProviderConfig` dataclass maps source-specific column names to a normalized interface. `normalize_record()` transforms raw source records so all modules receive consistent keys (`text`, `author`, `likes`, `retweets`, `replies`, `views`, `posted_at`).

Supported providers: `x_tweets`, `apify_x_tweets`, `apify_tiktok_posts`, `instagram_posts`, `reddit_posts`, `tiktok_videos`, `youtube_videos`. (`google_cpc` is skipped — keyword reference data with no text.)

### Modules (`enrichment/modules/`)

Four modules call **AWS Bedrock** (Converse API with `toolUse` for structured JSON output) using a shared `BedrockClient`:
- `SentimentAnalyzer` — sentiment label, score, confidence, emotions
- `EntityExtractor` — named entities with types and relevance
- `TopicClassifier` — primary/secondary topics with confidence
- `ContentModerator` — safety flags, toxicity score, categories

One module is **purely algorithmic** (no API):
- `EngagementScorer` — weighted engagement score, virality, interaction quality, time-adjusted metrics

Each Bedrock module follows the pattern: `_build_prompt()` → `client.invoke_structured()` → parsed dict, with `_empty_result()` as fallback on error.

### Common utilities (`enrichment/common/`)

- `BedrockClient` — shared wrapper around `boto3 bedrock-runtime` Converse API; `invoke_structured()` forces tool use to get structured JSON
- `AthenaReader` — `fetch_unenriched(config, limit)` queries any provider table with anti-join against `enrichments`
- `AthenaWriter` — `create_enrichments_table()` + `merge_enriched_records_batch()` for the unified output table

Legacy methods (`fetch_unenriched_tweets`, `create_enriched_tweets_table`, `merge_enriched_tweets_batch`) still exist for backward compatibility.

## Configuration

All config is via environment variables (see `.env.example`). Key ones:
- `BEDROCK_MODEL_ID` — Bedrock model (default: `us.anthropic.claude-3-5-haiku-20241022-v1:0`)
- `BEDROCK_REGION` — Bedrock region (defaults to `AWS_REGION` / `ap-south-1`)
- `AWS_REGION`, `ATHENA_S3_OUTPUT`, `ATHENA_DATABASE`, `ATHENA_WORKGROUP` — AWS/Athena config
- `ENRICHMENT_PROVIDERS` — comma-separated list of providers to process (defaults to all)
- `ENABLE_SENTIMENT`, `ENABLE_ENTITY`, `ENABLE_TOPIC`, `ENABLE_ENGAGEMENT`, `ENABLE_MODERATION` — toggle individual modules
- `ENRICHMENT_BATCH_SIZE` — controls write batch size (default 10)

## Key Patterns

- Modules are independently toggleable; initialization failures disable individual modules without crashing
- Per-record enrichment failures are logged but don't halt the pipeline
- Provider failures are logged and skipped; the pipeline continues to the next provider
- Output table is Iceberg format, partitioned by date, using Parquet/Snappy
- Composite MERGE key: `(source_table, source_id)` ensures records from different providers coexist
