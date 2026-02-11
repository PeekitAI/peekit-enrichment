"""Topic classification module using AWS Bedrock with structured output"""
from typing import Dict, Any, List
from pydantic import BaseModel, Field
from enrichment.common import BedrockClient


class TopicOutput(BaseModel):
    """Structured output schema for topic classification"""
    primary_category: str = Field(description="Main category (Technology, Business, Entertainment, Sports, Politics, Health, Science, Lifestyle, Education, Other)")
    sub_categories: List[str] = Field(description="List of sub-categories or specific topics")
    industry: str = Field(description="Relevant industry or sector")
    keywords: List[str] = Field(description="Key terms that define the topic")
    is_commercial: bool = Field(description="Whether content is commercial/promotional")
    is_news: bool = Field(description="Whether content is news-related")


class TopicClassifier:
    """
    Classify content by topic/category using AWS Bedrock

    Classifies into:
    - Primary category
    - Sub-categories
    - Industry/sector
    - Keywords
    - Commercial flag
    - News flag
    """

    def __init__(self):
        self.client = BedrockClient()

    def classify(self, text: str, author: str = None) -> Dict[str, Any]:
        """
        Classify topic of given text

        Args:
            text: Tweet text to classify
            author: Optional author name for context

        Returns:
            Dictionary with classification data
        """
        if not text or not text.strip():
            return self._empty_result()

        try:
            prompt = self._build_prompt(text, author)
            result = self.client.invoke_structured(
                system_prompt='You are an expert content classifier. Accurately categorize social media content into topics and industries.',
                user_prompt=prompt,
                schema=TopicOutput,
                tool_name='topic_classification',
                temperature=0.2,
            )
            return result
        except Exception as e:
            print(f"Error classifying topic: {e}")
            return self._empty_result()

    def _build_prompt(self, text: str, author: str = None) -> str:
        """Build the classification prompt"""
        author_context = f"\nAuthor: {author}" if author else ""

        return f"""Classify the following tweet into topics and categories:

Tweet: "{text}"{author_context}

Analyze and determine:
1. Primary Category - Choose ONE from: Technology, Business, Entertainment, Sports, Politics, Health, Science, Lifestyle, Education, Other
2. Sub-categories - Specific topics within the primary category
3. Industry - Relevant industry or business sector
4. Keywords - Key terms that define the topic
5. Is Commercial - Whether this is promotional/advertising content
6. Is News - Whether this is news or current events

Be specific and accurate in classification."""

    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure"""
        return {
            'primary_category': 'Other',
            'sub_categories': [],
            'industry': 'Unknown',
            'keywords': [],
            'is_commercial': False,
            'is_news': False
        }
