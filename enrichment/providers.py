"""Provider registry - maps source tables to a normalized interface"""
from dataclasses import dataclass, field
from typing import Dict, Optional, Any


@dataclass
class ProviderConfig:
    """Configuration for a single content provider/source table"""
    table: str
    id_column: str
    text_column: str  # SQL expression for text extraction
    author_column: Optional[str] = None
    likes_column: Optional[str] = None
    shares_column: Optional[str] = None
    comments_column: Optional[str] = None
    views_column: Optional[str] = None
    posted_at_column: Optional[str] = None
    keyword_column: Optional[str] = None
    region_column: Optional[str] = None
    url_column: Optional[str] = None


PROVIDERS: Dict[str, ProviderConfig] = {
    'x_tweets': ProviderConfig(
        table='x_tweets',
        id_column='tweet_id',
        text_column='tweet_text',
        author_column='author',
        likes_column='likes',
        shares_column='retweets',
        comments_column='replies',
        views_column='views',
        posted_at_column='posted_at',
        keyword_column='keyword',
        region_column='region',
        url_column=None,
    ),
    'apify_x_tweets': ProviderConfig(
        table='apify_x_tweets',
        id_column='id',
        text_column='text',
        author_column='author_username',
        likes_column='like_count',
        shares_column='retweet_count',
        comments_column='reply_count',
        views_column='view_count',
        posted_at_column='created_at',
        keyword_column='keyword',
        region_column='region',
        url_column=None,
    ),
    'apify_tiktok_posts': ProviderConfig(
        table='apify_tiktok_posts',
        id_column='id',
        text_column='text',
        author_column='author_username',
        likes_column='digg_count',
        shares_column='share_count',
        comments_column='comment_count',
        views_column='play_count',
        posted_at_column='created_at',
        keyword_column='keyword',
        region_column='region',
        url_column=None,
    ),
    'instagram_posts': ProviderConfig(
        table='instagram_posts',
        id_column='post_id',
        text_column='caption',
        author_column='username',
        likes_column='likes',
        shares_column=None,
        comments_column='comments',
        views_column='views',
        posted_at_column=None,
        keyword_column='keyword',
        region_column='region',
        url_column=None,
    ),
    'reddit_posts': ProviderConfig(
        table='reddit_posts',
        id_column='post_id',
        text_column="COALESCE(title || ' ' || caption, title, caption)",
        author_column='author',
        likes_column='upvotes',
        shares_column=None,
        comments_column='comments',
        views_column=None,
        posted_at_column='posted_at',
        keyword_column='keyword',
        region_column=None,
        url_column=None,
    ),
    'tiktok_videos': ProviderConfig(
        table='tiktok_videos',
        id_column='video_id',
        text_column='description',
        author_column='creator',
        likes_column='likes',
        shares_column='shares',
        comments_column='comments',
        views_column='views',
        posted_at_column=None,
        keyword_column='keyword',
        region_column=None,
        url_column=None,
    ),
    'youtube_videos': ProviderConfig(
        table='youtube_videos',
        id_column='video_id',
        text_column='title',
        author_column='channel',
        likes_column='likes',
        shares_column=None,
        comments_column='comments',
        views_column='views',
        posted_at_column='upload_date',
        keyword_column='keyword',
        region_column='region',
        url_column=None,
    ),
}


def normalize_record(raw: Dict[str, Any], config: ProviderConfig) -> Dict[str, Any]:
    """
    Transform a raw source record into the normalized format expected by
    enrichment modules and the unified enrichments table.

    Returns dict with keys: source_table, source_id, text, author, likes,
    retweets, replies, views, posted_at, keyword, region, scraped_at, source_url
    """
    def _get(col: Optional[str]) -> Any:
        """Get value from raw dict by column name, handling SQL expressions."""
        if col is None:
            return None
        # For SQL expressions (like COALESCE), the alias in the query will be 'text'
        # so we look up 'text' in the raw dict. For simple columns, use the column name.
        return raw.get(col)

    record = {
        'source_table': config.table,
        'source_id': str(raw.get(config.id_column, '')),
        # For SQL expressions in text_column, the query aliases it as 'source_text'
        'text': raw.get('source_text', '') or '',
        'author': _get(config.author_column) or '',
        'likes': _get(config.likes_column) or 0,
        'retweets': _get(config.shares_column) or 0,
        'replies': _get(config.comments_column) or 0,
        'views': _get(config.views_column) or 0,
        'posted_at': _get(config.posted_at_column),
        'keyword': _get(config.keyword_column) or '',
        'region': _get(config.region_column) or '',
        'scraped_at': raw.get('scraped_at'),
        'source_url': _get(config.url_column) or '',
    }

    return record
