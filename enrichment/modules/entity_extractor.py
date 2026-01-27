"""Entity extraction module using z.ai with structured output"""
import os
import requests
from typing import Dict, Any, List
from pydantic import BaseModel, Field


class EntityOutput(BaseModel):
    """Structured output schema for entity extraction"""
    people: List[str] = Field(description="List of people names mentioned")
    organizations: List[str] = Field(description="List of organizations/brands mentioned")
    locations: List[str] = Field(description="List of locations/places mentioned")
    products: List[str] = Field(description="List of products/services mentioned")
    hashtags: List[str] = Field(description="List of hashtags used")
    mentions: List[str] = Field(description="List of user mentions (without @)")


class EntityExtractor:
    """
    Extract entities from text using z.ai's structured output

    Extracts:
    - People names
    - Organizations/brands
    - Locations/places
    - Products/services
    - Hashtags
    - User mentions
    """

    def __init__(self):
        self.api_key = os.getenv('ZAI_API_KEY')
        if not self.api_key:
            raise ValueError("ZAI_API_KEY environment variable is required")

        self.api_url = os.getenv('ZAI_API_URL', 'https://api.z.ai/v1/chat/completions')
        self.model = os.getenv('ZAI_MODEL', 'gpt-4o-mini')

    def extract(self, text: str) -> Dict[str, Any]:
        """
        Extract entities from given text

        Args:
            text: Tweet text to analyze

        Returns:
            Dictionary with entity lists
        """
        if not text or not text.strip():
            return self._empty_result()

        try:
            prompt = self._build_prompt(text)
            result = self._call_zai_api(prompt)
            return result
        except Exception as e:
            print(f"Error extracting entities: {e}")
            return self._empty_result()

    def _build_prompt(self, text: str) -> str:
        """Build the extraction prompt"""
        return f"""Extract all entities from the following tweet:

Tweet: "{text}"

Identify and extract:
1. People - Names of individuals mentioned
2. Organizations - Companies, brands, institutions
3. Locations - Cities, countries, places
4. Products - Products or services mentioned
5. Hashtags - All hashtags used
6. Mentions - User mentions (without the @ symbol)

Be thorough and accurate. Only extract entities that are clearly present."""

    def _call_zai_api(self, prompt: str) -> Dict[str, Any]:
        """Call z.ai API with structured output"""
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

        schema = EntityOutput.model_json_schema()

        payload = {
            'model': self.model,
            'messages': [
                {
                    'role': 'system',
                    'content': 'You are an expert at entity extraction. Extract all relevant entities from social media content accurately.'
                },
                {
                    'role': 'user',
                    'content': prompt
                }
            ],
            'response_format': {
                'type': 'json_schema',
                'json_schema': {
                    'name': 'entity_extraction',
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
        entity_data = json.loads(content)

        return entity_data

    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure"""
        return {
            'people': [],
            'organizations': [],
            'locations': [],
            'products': [],
            'hashtags': [],
            'mentions': []
        }
