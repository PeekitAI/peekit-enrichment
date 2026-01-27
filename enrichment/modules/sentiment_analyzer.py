"""Sentiment analysis module using z.ai with structured output"""
import os
import requests
from typing import Dict, Any, List
from pydantic import BaseModel, Field


class SentimentOutput(BaseModel):
    """Structured output schema for sentiment analysis"""
    sentiment_label: str = Field(description="Overall sentiment: positive, negative, or neutral")
    sentiment_score: float = Field(description="Confidence score between 0 and 1")
    sentiment_emotions: List[str] = Field(description="List of emotions detected (e.g., joy, anger, sadness)")
    sentiment_topics: List[str] = Field(description="List of main topics or themes in the text")


class SentimentAnalyzer:
    """
    Analyze sentiment of text using z.ai's structured output capability

    Uses z.ai API with structured output to extract:
    - Sentiment label (positive/negative/neutral)
    - Confidence score
    - Emotions detected
    - Topics/themes
    """

    def __init__(self):
        self.api_key = os.getenv('ZAI_API_KEY')
        if not self.api_key:
            raise ValueError("ZAI_API_KEY environment variable is required")

        # Z.ai API endpoint (adjust based on actual z.ai docs)
        self.api_url = os.getenv('ZAI_API_URL', 'https://api.z.ai/v1/chat/completions')
        self.model = os.getenv('ZAI_MODEL', 'gpt-4o-mini')  # Default model, adjust as needed

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
            # Build the prompt for sentiment analysis
            prompt = self._build_prompt(text)

            # Call z.ai API with structured output
            result = self._call_zai_api(prompt)

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

    def _call_zai_api(self, prompt: str) -> Dict[str, Any]:
        """
        Call z.ai API with structured output

        Based on z.ai docs at https://docs.z.ai/guides/capabilities/struct-output
        """
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

        # Convert Pydantic model to JSON schema for structured output
        schema = SentimentOutput.model_json_schema()

        payload = {
            'model': self.model,
            'messages': [
                {
                    'role': 'system',
                    'content': 'You are a sentiment analysis expert. Analyze the sentiment of social media content accurately and extract emotions and topics.'
                },
                {
                    'role': 'user',
                    'content': prompt
                }
            ],
            'response_format': {
                'type': 'json_schema',
                'json_schema': {
                    'name': 'sentiment_analysis',
                    'schema': schema,
                    'strict': True
                }
            },
            'temperature': 0.3,  # Lower temperature for more consistent results
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

        # Extract the structured output from response
        # Adjust based on actual z.ai response format
        content = result['choices'][0]['message']['content']

        # Parse JSON response
        import json
        sentiment_data = json.loads(content)

        return sentiment_data

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
