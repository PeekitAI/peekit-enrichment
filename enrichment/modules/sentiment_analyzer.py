"""Sentiment analysis module using AWS Bedrock with structured output"""
from typing import Dict, Any, List
from pydantic import BaseModel, Field
from enrichment.common import BedrockClient


class SentimentOutput(BaseModel):
    """Structured output schema for sentiment analysis"""
    sentiment_label: str = Field(description="Overall sentiment: positive, negative, or neutral")
    sentiment_score: float = Field(description="Confidence score between 0 and 1")
    sentiment_emotions: List[str] = Field(description="List of emotions detected (e.g., joy, anger, sadness)")
    sentiment_topics: List[str] = Field(description="List of main topics or themes in the text")


class SentimentAnalyzer:
    """
    Analyze sentiment of text using AWS Bedrock structured output

    Uses Bedrock Converse API with toolUse to extract:
    - Sentiment label (positive/negative/neutral)
    - Confidence score
    - Emotions detected
    - Topics/themes
    """

    def __init__(self):
        self.client = BedrockClient()

    def analyze(self, text: str) -> Dict[str, Any]:
        """
        Analyze sentiment of given text

        Args:
            text: Tweet text to analyze

        Returns:
            Dictionary with sentiment_label, sentiment_score, sentiment_emotions, sentiment_topics
        """
        if not text or not text.strip():
            return self._empty_result()

        try:
            prompt = self._build_prompt(text)
            result = self.client.invoke_structured(
                system_prompt='You are a sentiment analysis expert. Analyze the sentiment of social media content accurately and extract emotions and topics.',
                user_prompt=prompt,
                schema=SentimentOutput,
                tool_name='sentiment_analysis',
                temperature=0.3,
            )
            return result
        except Exception as e:
            print(f"Error analyzing sentiment: {e}")
            return self._empty_result()

    def _build_prompt(self, text: str) -> str:
        """Build the analysis prompt"""
        return f"""Analyze the sentiment of the following tweet and extract:
1. Overall sentiment (positive, negative, or neutral)
2. Confidence score (0-1)
3. Emotions detected (e.g., joy, anger, sadness, fear, surprise, disgust)
4. Main topics or themes

Tweet: "{text}"

Provide a comprehensive sentiment analysis."""

    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure"""
        return {
            'sentiment_label': 'neutral',
            'sentiment_score': 0.0,
            'sentiment_emotions': [],
            'sentiment_topics': []
        }

    def analyze_batch(self, texts: List[str], max_workers: int = 5) -> List[Dict[str, Any]]:
        """
        Analyze sentiment for multiple texts in parallel

        Args:
            texts: List of tweet texts
            max_workers: Maximum number of concurrent API calls

        Returns:
            List of sentiment analysis results
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_text = {executor.submit(self.analyze, text): i for i, text in enumerate(texts)}

            # Collect results as they complete
            for future in as_completed(future_to_text):
                idx = future_to_text[future]
                try:
                    result = future.result()
                    results.append((idx, result))
                except Exception as e:
                    print(f"Error analyzing text {idx}: {e}")
                    results.append((idx, self._empty_result()))

        # Sort by original index
        results.sort(key=lambda x: x[0])

        # Return just the results without indices
        return [r[1] for r in results]
