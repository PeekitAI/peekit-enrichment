"""Topic classification module using z.ai with structured output"""
import os
import requests
from typing import Dict, Any, List
from pydantic import BaseModel, Field


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
    Classify content by topic/category using z.ai

    Classifies into:
    - Primary category
    - Sub-categories
    - Industry/sector
    - Keywords
    - Commercial flag
    - News flag
    """

    def __init__(self):
        self.api_key = os.getenv('ZAI_API_KEY')
        if not self.api_key:
            raise ValueError("ZAI_API_KEY environment variable is required")

        self.api_url = os.getenv('ZAI_API_URL', 'https://api.z.ai/v1/chat/completions')
        self.model = os.getenv('ZAI_MODEL', 'gpt-4o-mini')

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
            result = self._call_zai_api(prompt)
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

    def _call_zai_api(self, prompt: str) -> Dict[str, Any]:
        """Call z.ai API with structured output"""
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

        schema = TopicOutput.model_json_schema()

        payload = {
            'model': self.model,
            'messages': [
                {
                    'role': 'system',
                    'content': 'You are an expert content classifier. Accurately categorize social media content into topics and industries.'
                },
                {
                    'role': 'user',
                    'content': prompt
                }
            ],
            'response_format': {
                'type': 'json_schema',
                'json_schema': {
                    'name': 'topic_classification',
                    'schema': schema,
                    'strict': True
                }
            },
            'temperature': 0.2,
        }

        response = requests.post(
            self.api_url,
            headers=headers,
            json=payload,
            timeout=30
        )

        if response.status_code != 200:
            raise Exception(f"Z.ai API error: {response.status_code} - {response.text}")

        result = response.json()
        content = result['choices'][0]['message']['content']

        import json
        topic_data = json.loads(content)

        return topic_data

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
