"""Content moderation module using z.ai with structured output"""
import os
import requests
from typing import Dict, Any, List
from pydantic import BaseModel, Field


class ModerationOutput(BaseModel):
    """Structured output schema for content moderation"""
    is_safe: bool = Field(description="Whether content is safe for general audiences")
    risk_level: str = Field(description="Risk level: safe, low, medium, high, critical")
    flags: List[str] = Field(description="List of content flags (hate_speech, violence, adult_content, spam, misinformation, profanity, harassment)")
    content_warnings: List[str] = Field(description="Specific warnings for unsafe content")
    recommended_action: str = Field(description="Recommended action: none, flag, review, remove")
    confidence_score: float = Field(description="Confidence in moderation decision (0-1)")


class ContentModerator:
    """
    Flag inappropriate or unsafe content using z.ai

    Checks for:
    - Hate speech
    - Violence/graphic content
    - Adult/NSFW content
    - Spam
    - Misinformation
    - Profanity
    - Harassment
    """

    def __init__(self):
        self.api_key = os.getenv('ZAI_API_KEY')
        if not self.api_key:
            raise ValueError("ZAI_API_KEY environment variable is required")

        self.api_url = os.getenv('ZAI_API_URL', 'https://api.z.ai/v1/chat/completions')
        self.model = os.getenv('ZAI_MODEL', 'gpt-4o-mini')

    def moderate(self, text: str) -> Dict[str, Any]:
        """
        Moderate content and flag issues

        Args:
            text: Tweet text to moderate

        Returns:
            Dictionary with moderation results
        """
        if not text or not text.strip():
            return self._safe_result()

        try:
            prompt = self._build_prompt(text)
            result = self._call_zai_api(prompt)
            return result
        except Exception as e:
            print(f"Error moderating content: {e}")
            return self._safe_result()

    def _build_prompt(self, text: str) -> str:
        """Build the moderation prompt"""
        return f"""Analyze the following tweet for content safety and moderation:

Tweet: "{text}"

Evaluate for:
1. Hate Speech - Targeting protected groups based on race, religion, gender, etc.
2. Violence - Graphic violence, threats, or incitement
3. Adult Content - NSFW, explicit sexual content
4. Spam - Repetitive, irrelevant, or promotional spam
5. Misinformation - False or misleading information
6. Profanity - Excessive profanity or vulgar language
7. Harassment - Bullying, harassment, or personal attacks

Determine:
- Is Safe: Whether content is safe for general audiences
- Risk Level: safe, low, medium, high, or critical
- Flags: List of specific issues found
- Content Warnings: Specific warnings for moderators
- Recommended Action: none, flag (for review), review (needs manual check), remove (immediate removal)
- Confidence Score: How confident you are in this assessment (0-1)

Be objective and consistent in moderation decisions."""

    def _call_zai_api(self, prompt: str) -> Dict[str, Any]:
        """Call z.ai API with structured output"""
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

        schema = ModerationOutput.model_json_schema()

        payload = {
            'model': self.model,
            'messages': [
                {
                    'role': 'system',
                    'content': 'You are an expert content moderator. Evaluate content objectively for safety and policy violations.'
                },
                {
                    'role': 'user',
                    'content': prompt
                }
            ],
            'response_format': {
                'type': 'json_schema',
                'json_schema': {
                    'name': 'content_moderation',
                    'schema': schema,
                    'strict': True
                }
            },
            'temperature': 0.1,  # Very low for consistent moderation
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
        moderation_data = json.loads(content)

        return moderation_data

    def _safe_result(self) -> Dict[str, Any]:
        """Return safe/clean result"""
        return {
            'is_safe': True,
            'risk_level': 'safe',
            'flags': [],
            'content_warnings': [],
            'recommended_action': 'none',
            'confidence_score': 1.0
        }
