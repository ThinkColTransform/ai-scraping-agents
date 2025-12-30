"""Azure OpenAI client for autonomous agents"""

import os
import json
import logging
from typing import Dict, Any, Optional
from openai import AzureOpenAI

logger = logging.getLogger(__name__)


class AzureOpenAIClient:
    """
    Wrapper for Azure OpenAI API to spawn autonomous agents

    Agents can:
    - Analyze websites
    - Propose extraction strategies
    - Fix data quality issues
    - Make decisions about tools (Playwright vs requests)
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        api_key: Optional[str] = None,
        deployment_name: Optional[str] = None,
        api_version: str = "2024-08-01-preview"
    ):
        """
        Initialize Azure OpenAI client

        Args:
            endpoint: Azure OpenAI endpoint URL
            api_key: Azure OpenAI API key
            deployment_name: Deployment name (e.g., "gpt-4")
            api_version: API version
        """
        self.endpoint = endpoint or os.getenv("AZURE_OPENAI_ENDPOINT")
        self.api_key = api_key or os.getenv("AZURE_OPENAI_API_KEY")
        self.deployment_name = deployment_name or os.getenv("AOAI_DEPLOYMENT_NAME")
        self.api_call_count = 0  # Track number of API calls

        if not all([self.endpoint, self.api_key, self.deployment_name]):
            logger.warning(
                "Azure OpenAI credentials not fully configured. "
                "Running in MOCK mode. Set AZURE_OPENAI_ENDPOINT, "
                "AZURE_OPENAI_API_KEY, and AOAI_DEPLOYMENT_NAME in .env"
            )
            self.mock_mode = True
            self.client = None
        else:
            self.mock_mode = False
            self.client = AzureOpenAI(
                azure_endpoint=self.endpoint,
                api_key=self.api_key,
                api_version=api_version
            )
            logger.info(f"Azure OpenAI client initialized with deployment: {self.deployment_name}")

    def create_agent(
        self,
        system_prompt: str,
        user_prompt: str,
        response_format: Optional[str] = "json_object",
        temperature: float = 0.7,
        max_tokens: int = 4096
    ) -> Dict[str, Any]:
        """
        Spawn an autonomous agent with a specific task

        Args:
            system_prompt: System instructions for the agent
            user_prompt: User task/question
            response_format: "json_object" or "text"
            temperature: Creativity (0.0-1.0)
            max_tokens: Max response length

        Returns:
            Dict containing agent's response
        """
        if self.mock_mode:
            logger.warning("Running in MOCK mode - returning simulated response")
            return self._mock_response(user_prompt)

        try:
            self.api_call_count += 1  # Increment call counter

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]

            # Call Azure OpenAI
            logger.info(f"API Call #{self.api_call_count} to Azure OpenAI ({self.deployment_name})...")

            # GPT-5.x models use max_completion_tokens instead of max_tokens
            token_param = {}
            if 'gpt-5' in self.deployment_name.lower():
                token_param = {"max_completion_tokens": max_tokens}
            else:
                token_param = {"max_tokens": max_tokens}

            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=messages,
                temperature=temperature,
                **token_param,
                response_format={"type": response_format} if response_format == "json_object" else None
            )

            content = response.choices[0].message.content

            # Parse JSON if requested
            if response_format == "json_object":
                try:
                    return json.loads(content)
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse JSON response: {content}")
                    return {"error": "Invalid JSON response", "raw_content": content}

            return {"response": content}

        except Exception as e:
            logger.error(f"Azure OpenAI API error: {e}")
            return {"error": str(e)}

    def analyze_website_structure(self, url: str, html_snippet: str) -> Dict[str, Any]:
        """
        Agent analyzes website structure and recommends extraction strategy

        Args:
            url: Target URL
            html_snippet: Sample HTML from the page

        Returns:
            {
                "page_type": "api|html|spa|playwright_required",
                "recommended_strategy": "...",
                "confidence": 0.0-1.0,
                "reasoning": "..."
            }
        """
        from .prompts import RECON_PROMPT

        user_prompt = f"""
Analyze this website and recommend an extraction strategy:

URL: {url}

HTML Snippet (first 2000 chars):
{html_snippet[:2000]}

Determine:
1. Is data available via API? (check for fetch/ajax calls)
2. Is it static HTML or JavaScript-rendered?
3. Will we need Playwright or is requests sufficient?

Respond in JSON format.
"""

        return self.create_agent(
            system_prompt=RECON_PROMPT,
            user_prompt=user_prompt,
            response_format="json_object"
        )

    def fix_data_quality_issue(
        self,
        issue_description: str,
        sample_records: list,
        website_context: str
    ) -> Dict[str, Any]:
        """
        Agent investigates data quality issue and proposes fix

        Args:
            issue_description: What's wrong (e.g., "40% missing floor data")
            sample_records: Sample records with the issue
            website_context: Information about the website

        Returns:
            {
                "solution_type": "regex|api_field|detail_pages|...",
                "implementation": {...},
                "confidence": 0.0-1.0,
                "requires_playwright": bool
            }
        """
        from .prompts import REPAIR_PROMPT

        user_prompt = f"""
Data quality issue detected:

ISSUE: {issue_description}

SAMPLE RECORDS WITH ISSUE:
{json.dumps(sample_records[:5], indent=2, ensure_ascii=False)}

WEBSITE CONTEXT:
{website_context}

Your task: Investigate and propose a solution to fix this issue.

Respond in JSON format with:
- solution_type
- implementation details
- confidence score
- whether Playwright is required
"""

        return self.create_agent(
            system_prompt=REPAIR_PROMPT,
            user_prompt=user_prompt,
            response_format="json_object"
        )

    def generate_extraction_config(
        self,
        url: str,
        html_features: Dict[str, Any],
        site_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        AI agent generates extraction CONFIG (not code) for a specific mall

        Args:
            url: Target URL
            html_features: Extracted features from HTML (not raw HTML!)
            site_analysis: Previous AI analysis of the site

        Returns:
            {
                "extraction_type": "api|html|json_embedded",
                "api_endpoint": "...",
                "list_selector": "...",
                "field_mappings": {...},
                "confidence": 0.0-1.0
            }
        """
        from .prompts import EXTRACTION_CONFIG_PROMPT

        user_prompt = f"""
URL: {url}

SITE ANALYSIS:
{json.dumps(site_analysis, indent=2, ensure_ascii=False)}

HTML FEATURES:
{json.dumps(html_features, indent=2, ensure_ascii=False)}

Return extraction config JSON (max 50 lines).
"""

        return self.create_agent(
            system_prompt=EXTRACTION_CONFIG_PROMPT,
            user_prompt=user_prompt,
            response_format="json_object",
            temperature=0.2,  # Very low for deterministic config
            max_tokens=1024  # Config is small
        )

    def _mock_response(self, prompt: str) -> Dict[str, Any]:
        """Simulated response when running in mock mode"""

        # Detect what kind of request this is
        if "analyze this website" in prompt.lower() or "extraction strategy" in prompt.lower():
            return {
                "page_type": "api",
                "recommended_strategy": "Use requests to call REST API endpoint",
                "confidence": 0.85,
                "reasoning": "Found fetch() calls in HTML, likely using REST API",
                "api_endpoint_pattern": "/get/{section}?mall_id={id}"
            }

        elif "missing floor" in prompt.lower() or "floor data" in prompt.lower():
            return {
                "solution_type": "api_field_mapping",
                "reasoning": "Floor data likely in malllevel_id field, not location text",
                "implementation": {
                    "field_mapping": {
                        "floor_source_field": "malllevel_id",
                        "conversion_logic": "Map malllevel_id to canonical floor format"
                    }
                },
                "confidence": 0.90,
                "requires_playwright": False,
                "estimated_improvement": "+25% floor coverage"
            }

        else:
            return {
                "analysis": "Mock response - configure Azure OpenAI for real agents",
                "recommendation": "Add credentials to .env file",
                "confidence": 0.5
            }
