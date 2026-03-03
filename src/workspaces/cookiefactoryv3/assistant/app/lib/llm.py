# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 2023
# SPDX-License-Identifier: Apache-2.0
# Updated 2024: Migrated to Claude 3 / Messages API (langchain-aws ChatBedrock)

import boto3

from langchain_aws import ChatBedrock
from langchain_aws import BedrockEmbeddings

from botocore.config import Config

from .env import get_bedrock_region

available_models = [
    "anthropic.claude-3-5-haiku-20241022-v1:0",
    "anthropic.claude-3-5-sonnet-20241022-v2:0",
    "amazon.titan-text-premier-v1:0",
]

# the current model used for text generation
text_model_id = "anthropic.claude-3-5-haiku-20241022-v1:0"
text_v2_model_id = "anthropic.claude-3-5-haiku-20241022-v1:0"
embedding_model_id = "amazon.titan-embed-text-v1"

model_kwargs = {
    "anthropic.claude-3-5-haiku-20241022-v1:0": {
        "max_tokens": 2048,
        "temperature": 0.1,
        "top_p": 0.9,
    },
}

bedrock = boto3.client('bedrock', get_bedrock_region(), config=Config(
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    }
))
bedrock_runtime = boto3.client('bedrock-runtime', get_bedrock_region(), config=Config(
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    }
))


def get_bedrock_text():
    llm = ChatBedrock(
        model_id=text_model_id,
        client=bedrock_runtime,
        model_kwargs=model_kwargs.get(text_model_id, {}),
    )
    return llm


def get_bedrock_text_v2():
    llm = ChatBedrock(
        model_id=text_v2_model_id,
        client=bedrock_runtime,
        model_kwargs=model_kwargs.get(text_v2_model_id, {}),
    )
    return llm


def get_bedrock_embedding():
    embeddings = BedrockEmbeddings(
        model_id=embedding_model_id,
        client=bedrock_runtime
    )
    return embeddings


def get_processed_prompt_template(template):
    """Claude 3 uses Messages API -- return the template as-is (no Human/Assistant wrapping needed)."""
    return template


def get_prefix_prompt_template(template):
    """No-op for Claude 3 Messages API compatibility."""
    return template


def get_postfix_prompt_template(template):
    """No-op for Claude 3 Messages API compatibility."""
    return template
