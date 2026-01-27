"""Engagement scoring module - calculates engagement metrics"""
from typing import Dict, Any
from datetime import datetime, timezone
import math


class EngagementScorer:
    """
    Calculate engagement scores and metrics

    Scores:
    - Total engagement score (weighted)
    - Engagement rate
    - Virality score
    - Interaction quality
    - Time-adjusted engagement
    """

    def __init__(self):
        # Weights for different engagement types
        self.weights = {
            'likes': 1.0,
            'retweets': 3.0,      # Retweets are more valuable
            'replies': 2.0,       # Replies show active engagement
            'views': 0.01         # Views are numerous but less valuable
        }

    def score(self, tweet_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate engagement scores for a tweet

        Args:
            tweet_data: Tweet dictionary with engagement metrics

        Returns:
            Dictionary with scoring data
        """
        likes = tweet_data.get('likes', 0) or 0
        retweets = tweet_data.get('retweets', 0) or 0
        replies = tweet_data.get('replies', 0) or 0
        views = tweet_data.get('views', 0) or 0
        posted_at = tweet_data.get('posted_at')

        # Calculate total engagement score (weighted sum)
        engagement_score = (
            likes * self.weights['likes'] +
            retweets * self.weights['retweets'] +
            replies * self.weights['replies'] +
            views * self.weights['views']
        )

        # Calculate engagement rate (total interactions / views)
        total_interactions = likes + retweets + replies
        engagement_rate = (total_interactions / views) if views > 0 else 0.0

        # Calculate virality score (retweets / total interactions)
        virality_score = (retweets / total_interactions) if total_interactions > 0 else 0.0

        # Calculate interaction quality (replies + retweets / likes)
        # Higher score means more active engagement beyond passive likes
        meaningful_interactions = replies + retweets
        interaction_quality = (meaningful_interactions / likes) if likes > 0 else 0.0

        # Calculate time-adjusted score (score per hour)
        time_adjusted_score = self._calculate_time_adjusted_score(
            engagement_score, posted_at
        )

        # Calculate percentile tier (0-100 scale, logarithmic)
        percentile_score = self._calculate_percentile_score(engagement_score)

        return {
            'engagement_score': round(engagement_score, 2),
            'engagement_rate': round(engagement_rate, 4),
            'virality_score': round(virality_score, 4),
            'interaction_quality': round(interaction_quality, 4),
            'time_adjusted_score': round(time_adjusted_score, 2),
            'percentile_score': round(percentile_score, 2),
            'engagement_tier': self._get_engagement_tier(percentile_score)
        }

    def _calculate_time_adjusted_score(self, score: float, posted_at) -> float:
        """Calculate engagement score adjusted for time elapsed"""
        if not posted_at:
            return 0.0

        try:
            # Convert to datetime if string
            if isinstance(posted_at, str):
                posted_at = datetime.fromisoformat(posted_at.replace('Z', '+00:00'))

            # Calculate hours elapsed
            now = datetime.now(timezone.utc)
            if posted_at.tzinfo is None:
                posted_at = posted_at.replace(tzinfo=timezone.utc)

            elapsed = (now - posted_at).total_seconds() / 3600  # hours

            if elapsed <= 0:
                return 0.0

            # Score per hour
            return score / elapsed

        except Exception as e:
            print(f"Error calculating time-adjusted score: {e}")
            return 0.0

    def _calculate_percentile_score(self, score: float) -> float:
        """
        Calculate percentile score (0-100) using logarithmic scale

        Roughly:
        - 0-10: Low engagement (< 100 points)
        - 10-25: Below average (100-500 points)
        - 25-50: Average (500-2000 points)
        - 50-75: Above average (2000-10000 points)
        - 75-90: High engagement (10000-50000 points)
        - 90-100: Viral (> 50000 points)
        """
        if score <= 0:
            return 0.0

        # Logarithmic scaling
        # log base 10, normalized to 0-100 scale
        log_score = math.log10(score + 1)

        # Map log scale to percentile (adjust thresholds as needed)
        # 0 -> 0, 10 -> 10, 100 -> 20, 1000 -> 30, 10000 -> 40, etc.
        percentile = min(100.0, log_score * 25)

        return percentile

    def _get_engagement_tier(self, percentile: float) -> str:
        """Get engagement tier based on percentile"""
        if percentile >= 90:
            return 'Viral'
        elif percentile >= 75:
            return 'High'
        elif percentile >= 50:
            return 'Above Average'
        elif percentile >= 25:
            return 'Average'
        elif percentile >= 10:
            return 'Below Average'
        else:
            return 'Low'

    def batch_score(self, tweets: list) -> list:
        """Score multiple tweets"""
        return [self.score(tweet) for tweet in tweets]
