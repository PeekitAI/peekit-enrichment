"""Content moderation module using AWS Bedrock with structured output"""
from typing import Dict, Any, List
from pydantic import BaseModel, Field
from enrichment.common import BedrockClient


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
    Flag inappropriate or unsafe content using AWS Bedrock

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
        self.client = BedrockClient()

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
            result = self.client.invoke_structured(
                system_prompt='You are an expert content moderator. Evaluate content objectively for safety and policy violations.',
                user_prompt=prompt,
                schema=ModerationOutput,
                tool_name='content_moderation',
                temperature=0.1,
            )
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
