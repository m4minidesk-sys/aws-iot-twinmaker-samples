# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 2023
# SPDX-License-Identifier: Apache-2.0
# Updated 2024: Migrated to Claude 3 / Messages API (langchain-aws ChatBedrock)
#
# NOTE: The prompt template helper functions (get_processed_prompt_template,
# get_prefix_prompt_template, get_postfix_prompt_template) are intentionally
# kept as pass-through functions to maintain API compatibility with all callers.
# Claude 3 Messages API handles system/human message formatting internally via
# ChatBedrock, so no H:/A: wrapping is required. The templates are passed as-is.

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
# text_v2_model_id: originally claude-v2, now unified to claude-3-5-haiku.
# Intentionally kept as a separate variable for future differentiation (e.g., Sonnet 3.5).
text_v2_model_id = "anthropic.claude-3-5-haiku-20241022-v1:0"
embedding_model_id = "amazon.titan-embed-text-v1"

model_kwargs = {
    "anthropic.claude-3-5-haiku-20241022-v1:0": {
        # max_tokens replaces deprecated max_tokens_to_sample (Claude 3 Messages API)
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
    """Return the template unchanged.

    Claude 3 uses the Messages API, which does not require Human:/Assistant:
    wrapping. Templates are passed directly to ChatBedrock via LangChain's
    PromptTemplate mechanism. This function exists solely for API compatibility
    with callers (router.py, qa.py, general.py, view.py, initial_diagnosis.py,
    partiql_generator.py, domain_mapper.py).
    """
    return template


def get_prefix_prompt_template(template):
    """Return the prefix template unchanged (Claude 3 Messages API compatibility).

    Previously added 'Human:' prefix for Claude 2 text completion API.
    Not required for Claude 3 Messages API / ChatBedrock.
    """
    return template


def get_postfix_prompt_template(template):
    """Return the postfix template unchanged (Claude 3 Messages API compatibility).

    Previously added 'Assistant:' suffix for Claude 2 text completion API.
    Not required for Claude 3 Messages API / ChatBedrock.
    """
    return template
