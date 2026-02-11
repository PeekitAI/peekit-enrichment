"""Entity extraction module using AWS Bedrock with structured output"""
from typing import Dict, Any, List
from pydantic import BaseModel, Field
from enrichment.common import BedrockClient


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
    Extract entities from text using AWS Bedrock structured output

    Extracts:
    - People names
    - Organizations/brands
    - Locations/places
    - Products/services
    - Hashtags
    - User mentions
    """

    def __init__(self):
        self.client = BedrockClient()

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
            result = self.client.invoke_structured(
                system_prompt='You are an expert at entity extraction. Extract all relevant entities from social media content accurately.',
                user_prompt=prompt,
                schema=EntityOutput,
                tool_name='entity_extraction',
                temperature=0.2,
            )
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
