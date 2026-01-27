# Peekit Data Enrichment

Unified data enrichment pipeline for social media crawler data. Processes raw tweets from Iceberg tables and adds AI-powered enrichments.

## Features

### Implemented Modules

All 5 enrichment modules are fully implemented and production-ready:

1. **Sentiment Analysis** - Analyzes sentiment using z.ai with structured output
   - Sentiment label (positive/negative/neutral)
   - Confidence score
   - Emotions detected
   - Topics/themes extracted

2. **Entity Extraction** - Extracts named entities using z.ai
   - People names
   - Organizations/brands
   - Locations/places
   - Products/services
   - Hashtags
   - User mentions

3. **Topic Classification** - Categorizes content using z.ai
   - Primary category (Technology, Business, Entertainment, etc.)
   - Sub-categories
   - Industry/sector
   - Keywords
   - Commercial content flag
   - News content flag

4. **Engagement Scoring** - Calculates engagement metrics (algorithmic, no API)
   - Weighted engagement score
   - Engagement rate
   - Virality score
   - Interaction quality
   - Time-adjusted score
   - Percentile ranking
   - Engagement tier

5. **Content Moderation** - Flags inappropriate content using z.ai
   - Safety assessment
   - Risk level (safe/low/medium/high/critical)
   - Content flags (hate speech, violence, adult content, spam, etc.)
   - Content warnings
   - Recommended action (none/flag/review/remove)
   - Confidence score

## Architecture

```
┌─────────────────┐
│   Athena/       │
│   Iceberg       │
│   (x_tweets)    │
└────────┬────────┘
         │
         │ Read unenriched tweets
         ▼
┌─────────────────┐
│   Enrichment    │
│   Pipeline      │
│                 │
│  ┌──────────┐   │
│  │Sentiment │   │
│  │Analyzer  │   │
│  └──────────┘   │
│                 │
│  ┌──────────┐   │
│  │  Future  │   │
│  │ Modules  │   │
│  └──────────┘   │
└────────┬────────┘
         │
         │ Write enriched data
         ▼
┌─────────────────┐
│   Athena/       │
│   Iceberg       │
│ (x_tweets_      │
│  enriched)      │
└─────────────────┘
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:
- AWS credentials (via AWS CLI or environment)
- Z.ai API key
- Configuration parameters

### 3. Run Enrichment

```bash
# Run with default settings (100 tweets)
python -m enrichment.main

# Or with custom limit
ENRICHMENT_LIMIT=500 python -m enrichment.main
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_REGION` | AWS region | `ap-south-1` |
| `ATHENA_DATABASE` | Athena database name | `peekit_crawlers` |
| `ATHENA_WORKGROUP` | Athena workgroup | `peekit-crawlers` |
| `ZAI_API_KEY` | Z.ai API key | Required |
| `ZAI_MODEL` | Z.ai model to use | `gpt-4o-mini` |
| `ENABLE_SENTIMENT` | Enable sentiment analysis | `true` |
| `ENABLE_ENTITY` | Enable entity extraction | `true` |
| `ENABLE_TOPIC` | Enable topic classification | `true` |
| `ENABLE_ENGAGEMENT` | Enable engagement scoring | `true` |
| `ENABLE_MODERATION` | Enable content moderation | `true` |
| `ENRICHMENT_LIMIT` | Max tweets to process | `100` |
| `ENRICHMENT_BATCH_SIZE` | Batch size for writing | `10` |

### Module Configuration

Enable/disable modules via environment variables:

```bash
ENABLE_SENTIMENT=true
ENABLE_ENTITY=true
ENABLE_TOPIC=true
ENABLE_ENGAGEMENT=true
ENABLE_MODERATION=true
```

All modules are enabled by default. Set to `false` to disable individual modules.

## Database Schema

### Input: `x_tweets`

Original tweet data from crawlers.

### Output: `x_tweets_enriched`

Enriched tweet data with additional fields:

| Field | Type | Description |
|-------|------|-------------|
| `tweet_id` | STRING | Unique tweet identifier |
| `tweet_text` | STRING | Tweet content |
| `author` | STRING | Author name |
| `author_handle` | STRING | Author handle |
| `likes` | BIGINT | Number of likes |
| `retweets` | BIGINT | Number of retweets |
| `replies` | BIGINT | Number of replies |
| `views` | BIGINT | Number of views |
| `posted_at` | TIMESTAMP | Tweet posting time |
| `keyword` | STRING | Crawl keyword |
| `region` | STRING | Region (IN/UK) |
| `scraped_at` | TIMESTAMP | Crawl timestamp |
| **`sentiment_label`** | **STRING** | **Sentiment (positive/negative/neutral)** |
| **`sentiment_score`** | **DOUBLE** | **Confidence score (0-1)** |
| **`sentiment_emotions`** | **ARRAY<STRING>** | **Emotions detected** |
| **`sentiment_topics`** | **ARRAY<STRING>** | **Topics extracted** |
| **`people`** | **ARRAY<STRING>** | **People mentioned** |
| **`organizations`** | **ARRAY<STRING>** | **Organizations/brands mentioned** |
| **`locations`** | **ARRAY<STRING>** | **Locations mentioned** |
| **`products`** | **ARRAY<STRING>** | **Products mentioned** |
| **`hashtags`** | **ARRAY<STRING>** | **Hashtags used** |
| **`mentions`** | **ARRAY<STRING>** | **User mentions** |
| **`primary_category`** | **STRING** | **Primary content category** |
| **`sub_categories`** | **ARRAY<STRING>** | **Sub-categories** |
| **`industry`** | **STRING** | **Industry/sector** |
| **`keywords`** | **ARRAY<STRING>** | **Keywords** |
| **`is_commercial`** | **BOOLEAN** | **Commercial content flag** |
| **`is_news`** | **BOOLEAN** | **News content flag** |
| **`engagement_score`** | **DOUBLE** | **Weighted engagement score** |
| **`engagement_rate`** | **DOUBLE** | **Engagement rate** |
| **`virality_score`** | **DOUBLE** | **Virality score** |
| **`interaction_quality`** | **DOUBLE** | **Interaction quality** |
| **`time_adjusted_score`** | **DOUBLE** | **Time-adjusted score** |
| **`percentile_score`** | **DOUBLE** | **Percentile ranking** |
| **`engagement_tier`** | **STRING** | **Engagement tier** |
| **`is_safe`** | **BOOLEAN** | **Content safety flag** |
| **`risk_level`** | **STRING** | **Risk level** |
| **`flags`** | **ARRAY<STRING>** | **Moderation flags** |
| **`content_warnings`** | **ARRAY<STRING>** | **Content warnings** |
| **`recommended_action`** | **STRING** | **Recommended moderation action** |
| **`confidence_score`** | **DOUBLE** | **Moderation confidence** |
| `enriched_at` | TIMESTAMP | Enrichment timestamp |
| `enrichment_version` | STRING | Pipeline version |

## Z.ai Integration

### Structured Output

Uses z.ai's structured output capability for consistent sentiment analysis:

```python
{
  "sentiment_label": "positive",
  "sentiment_score": 0.89,
  "sentiment_emotions": ["joy", "excitement"],
  "sentiment_topics": ["technology", "AI", "innovation"]
}
```

### API Configuration

Based on [z.ai structured output documentation](https://docs.z.ai/guides/capabilities/struct-output).

## Development

### Adding New Modules

1. Create module in `enrichment/modules/your_module.py`
2. Implement enrichment logic
3. Update `main.py` to call your module
4. Add configuration variable
5. Update schema in `athena_writer.py` if needed

Example:

```python
# enrichment/modules/entity_extractor.py
class EntityExtractor:
    def extract(self, text: str) -> List[str]:
        # Your extraction logic
        pass
```

### Running Tests

```bash
# TODO: Add tests
pytest tests/
```

## Deployment

### As Kubernetes Job

Coming soon - will run as a scheduled job in Kubernetes cluster.

### Job Template

Category: **Data Enrichment**

```yaml
apiVersion: peekit.io/v1
kind: JobTemplate
metadata:
  name: tweet-enrichment
spec:
  category: Data Enrichment
  image: peekit-enrichment:latest
  # ... rest of template
```

## Monitoring

The pipeline outputs progress and statistics:

```
Processing tweet 1/100: 123456789
  Sentiment: positive (0.89)
  Emotions: joy, excitement
  Topics: technology, AI
```

## Troubleshooting

### Common Issues

1. **Z.ai API key not set**
   ```
   ValueError: ZAI_API_KEY environment variable is required
   ```
   Solution: Set `ZAI_API_KEY` in `.env`

2. **AWS credentials not configured**
   ```
   botocore.exceptions.NoCredentialsError
   ```
   Solution: Configure AWS credentials via `aws configure`

3. **Table doesn't exist**
   ```
   Error creating enriched tweets table
   ```
   Solution: The pipeline creates the table automatically, check IAM permissions

## License

Proprietary - Peekit.AI
