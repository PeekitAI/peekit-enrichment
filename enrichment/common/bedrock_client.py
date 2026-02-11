"""Shared AWS Bedrock client for structured LLM output via Converse API"""
import os
import json
import boto3
from typing import Any, Dict, Type
from pydantic import BaseModel


class BedrockClient:
    """
    Wraps boto3 bedrock-runtime Converse API to get structured JSON output
    using toolUse (forced tool choice).
    """

    def __init__(self):
        region = os.getenv('BEDROCK_REGION', os.getenv('AWS_REGION', 'ap-south-1'))
        self.model_id = os.getenv(
            'BEDROCK_MODEL_ID',
            'us.anthropic.claude-3-5-haiku-20241022-v1:0',
        )
        self.client = boto3.client('bedrock-runtime', region_name=region)

    def invoke_structured(
        self,
        system_prompt: str,
        user_prompt: str,
        schema: Type[BaseModel],
        tool_name: str,
        temperature: float = 0.3,
    ) -> Dict[str, Any]:
        """
        Call Bedrock Converse with a forced tool to obtain structured JSON.

        Args:
            system_prompt: System-level instruction.
            user_prompt: User message / analysis request.
            schema: Pydantic model class whose JSON schema defines the tool input.
            tool_name: Name used for the Bedrock tool definition.
            temperature: Sampling temperature.

        Returns:
            Parsed dict matching the Pydantic schema fields.
        """
        json_schema = schema.model_json_schema()
        json_schema.pop('title', None)

        response = self.client.converse(
            modelId=self.model_id,
            system=[{'text': system_prompt}],
            messages=[
                {'role': 'user', 'content': [{'text': user_prompt}]},
            ],
            toolConfig={
                'tools': [
                    {
                        'toolSpec': {
                            'name': tool_name,
                            'description': f'Return structured {tool_name} results',
                            'inputSchema': {'json': json_schema},
                        }
                    }
                ],
                'toolChoice': {'tool': {'name': tool_name}},
            },
            inferenceConfig={'temperature': temperature},
        )

        # Extract the tool-use block from the response
        for block in response['output']['message']['content']:
            if 'toolUse' in block:
                return block['toolUse']['input']

        raise RuntimeError('No toolUse block found in Bedrock response')
