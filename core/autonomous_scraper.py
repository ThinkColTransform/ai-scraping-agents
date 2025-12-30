"""Autonomous mall scraper - main orchestrator"""

import logging
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import json
import re

from .models import (
    RawRecord,
    NormalizedRecord,
    SiteConfig,
    EvaluationReport,
    PageClassification
)
from ai.azure_openai_client import AzureOpenAIClient
from utils.normalization import normalize_floor, extract_shop_number, extract_floor_and_shop_from_location
from utils.export import export_to_csv, export_to_json, create_summary_report

logger = logging.getLogger(__name__)


class AutonomousMallScraper:
    """
    5-Step Autonomous Mall Scraper

    Steps:
    1. Recon: Discover site structure and classify pages
    2. Extract: Scrape data using optimal strategy
    3. Normalize: Convert to canonical schema
    4. Evaluate: Assess data quality
    5. Repair: Fix issues and iterate until threshold met
    """

    def __init__(
        self,
        root_url: str,
        output_dir: str = "./output",
        use_ai_agents: bool = True
    ):
        """
        Initialize autonomous scraper

        Args:
            root_url: Root URL of the mall website
            output_dir: Directory for output files
            use_ai_agents: Whether to use AI agents for repair (requires Azure OpenAI)
        """
        self.root_url = root_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Extract mall name from URL for file naming
        from urllib.parse import urlparse
        domain = urlparse(root_url).netloc
        # Remove www. and .com.hk/.com/.hk etc.
        self.mall_name = domain.replace('www.', '').split('.')[0]

        # Initialize AI client
        self.ai_client = AzureOpenAIClient() if use_ai_agents else None

        # State
        self.config = SiteConfig()
        self.raw_records: List[RawRecord] = []
        self.normalized_records: List[NormalizedRecord] = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def run(
        self,
        coverage_threshold: float = 0.90,
        max_iterations: int = 5
    ) -> Dict:
        """
        Run autonomous scraping workflow

        Args:
            coverage_threshold: Minimum coverage required (default: 90%)
            max_iterations: Maximum repair iterations (default: 5)

        Returns:
            Dict with results summary
        """
        logger.info("="*60)
        logger.info("AUTONOMOUS MALL SCRAPER - STARTING")
        logger.info("="*60)
        logger.info(f"Target: {self.root_url}")
        logger.info(f"Coverage threshold: {coverage_threshold:.0%}")
        logger.info(f"Max iterations: {max_iterations}")

        # STEP 1: RECON
        logger.info("\n" + "="*60)
        logger.info("STEP 1: RECON / STRUCTURE DISCOVERY")
        logger.info("="*60)
        self._discover_structure()

        # MAIN LOOP: Steps 2-5
        for iteration in range(1, max_iterations + 1):
            logger.info("\n" + "#"*60)
            logger.info(f"# ITERATION {iteration}/{max_iterations}")
            logger.info("#"*60)

            # STEP 2: EXTRACTION
            logger.info("\n" + "="*60)
            logger.info("STEP 2: EXTRACTION")
            logger.info("="*60)
            self._extract_data()

            # STEP 3: NORMALIZATION
            logger.info("\n" + "="*60)
            logger.info("STEP 3: NORMALIZATION")
            logger.info("="*60)
            self._normalize_data()

            # STEP 3.5: FLOOR MAPPING (Always run to maximize coverage)
            if iteration == 1 and self.ai_client:
                logger.info("\n" + "="*60)
                logger.info("STEP 3.5: AI FLOOR MAPPING DISCOVERY")
                logger.info("="*60)
                logger.info("🤖 Running AI floor mapper to maximize coverage...")
                floor_mapping_improved = self._repair_floors()
                if floor_mapping_improved:
                    # Re-normalize with discovered floor mapping
                    logger.info("Re-normalizing with discovered floor mapping...")
                    self._normalize_data()

            # STEP 4: EVALUATION
            logger.info("\n" + "="*60)
            logger.info("STEP 4: EVALUATION")
            logger.info("="*60)
            evaluation = self._evaluate_quality()

            # Check if threshold met
            if evaluation.passes_threshold(coverage_threshold):
                logger.info(f"\n✅ Coverage threshold met: {evaluation.overall_coverage:.1%} >= {coverage_threshold:.0%}")
                break

            # STEP 5: REPAIR
            logger.info("\n" + "="*60)
            logger.info("STEP 5: REPAIR LOOP")
            logger.info("="*60)
            repair_applied = self._attempt_repair(evaluation, iteration)

            if not repair_applied:
                logger.warning("No repairs could be applied - stopping")
                break

        # Save final results
        self._save_results(evaluation, iteration)

        # Get API call count
        api_calls = self.ai_client.api_call_count if self.ai_client else 0

        return {
            "success": evaluation.passes_threshold(coverage_threshold),
            "iterations": iteration,
            "coverage": evaluation.overall_coverage,
            "total_records": len(self.normalized_records),
            "api_calls": api_calls,
            "output_dir": str(self.output_dir.absolute())
        }

    def _discover_structure(self):
        """Step 1: Discover site structure using AI agent"""
        logger.info(f"Analyzing {self.root_url}...")

        # Try AI agent first for any site
        if self.ai_client and not self.ai_client.mock_mode:
            logger.info("🤖 Using AI agent to discover site structure...")
            try:
                resp = self.session.get(self.root_url, timeout=15)
                analysis = self.ai_client.analyze_website_structure(
                    self.root_url,
                    resp.text
                )
                logger.info(f"✅ AI Analysis complete:")
                logger.info(f"   Page type: {analysis.get('page_type', 'unknown')}")
                logger.info(f"   Strategy: {analysis.get('recommended_strategy', 'N/A')[:80]}")
                logger.info(f"   Confidence: {analysis.get('confidence', 0):.0%}")

                # Store AI analysis in config
                self.config.ai_analysis = analysis

                # Try to extract API endpoints from analysis
                if 'api_endpoint_pattern' in analysis:
                    self.config.api_endpoints = {
                        "discovered": analysis['api_endpoint_pattern']
                    }
            except Exception as e:
                logger.warning(f"AI analysis failed: {e}")
                logger.info("Falling back to pattern-based detection...")

        # If still no config and no AI, warn user
        if not self.config.sections and not self.config.api_endpoints:
            logger.warning("⚠️  Could not discover site structure automatically")
            logger.warning("   Options:")
            logger.warning("   1. Configure Azure OpenAI for AI-powered discovery")
            logger.warning("   2. Manually configure site structure in code")
            logger.warning("   3. The scraper will attempt generic extraction")

    def _extract_data(self):
        """Step 2: Extract raw data using site-specific or AI-generated code"""
        self.raw_records = []

        # Check if we have generated extraction config from previous run
        if hasattr(self.config, 'extraction_config') and self.config.extraction_config:
            logger.info("📝 Using previously generated extraction config...")
            # Fetch HTML again
            resp = self.session.get(self.root_url, timeout=15)
            self._execute_config_based_extraction(self.config.extraction_config, resp.text)
            self._deduplicate_records()
            return

        # Use AI to generate extraction code for any site
        if self.ai_client and not self.ai_client.mock_mode:
            logger.info("\n🤖 AI generating extraction code for unknown site...")
            self._generate_and_execute_extraction()
            self._deduplicate_records()
        else:
            logger.warning("⚠️  Unknown site and no AI agent available")
            logger.warning("   Cannot extract data automatically")

    def _extract_html_features(self, html: str) -> Dict:
        """Extract structured features from HTML - optimized for shop detection"""
        from bs4 import BeautifulSoup
        import re

        soup = BeautifulSoup(html, 'html.parser')

        features = {
            # Repeated elements (likely shop cards)
            "repeated_structures": [],
            "sample_html_nodes": [],

            # Semantic tags
            "has_article_tags": len(soup.find_all('article')) > 0,
            "has_data_attributes": False,

            # Script tags (for embedded JSON)
            "has_json_script": False,
            "script_patterns": [],
            "json_sample": None,

            # Interactive elements
            "filter_options": [],
            "pagination_present": False,

            # Sample shop data
            "likely_shop_names": [],
            "likely_shop_info": []
        }

        # Find repeated structures (shop cards)
        class_counter = {}
        id_pattern_counter = {}

        for tag in soup.find_all(class_=True):
            classes = ' '.join(sorted(tag.get('class', [])))
            if classes:
                class_counter[classes] = class_counter.get(classes, 0) + 1

            # Check for data attributes
            if any(attr.startswith('data-') for attr in tag.attrs):
                features["has_data_attributes"] = True

        # Get top repeated classes (minimum 5 occurrences = likely a list)
        repeated = [(cls, count) for cls, count in class_counter.items() if count >= 5]
        repeated.sort(key=lambda x: x[1], reverse=True)
        features["repeated_structures"] = repeated[:5]

        # If we found repeated structures, extract sample HTML
        if repeated:
            top_class = repeated[0][0]
            sample_nodes = soup.find_all(class_=top_class.split())[:2]
            for node in sample_nodes:
                # Minify: remove extra whitespace
                node_html = str(node)[:500]  # First 500 chars
                features["sample_html_nodes"].append(node_html)

        # Check for embedded JSON and API patterns in scripts
        api_patterns_found = []
        for script in soup.find_all('script'):
            text = script.string or ''

            # Look for API endpoints in fetch/ajax calls
            api_patterns = [
                r'fetch\([\'"]([^\'\"]+)[\'"]',  # fetch('url')
                r'\.get\([\'"]([^\'\"]+)[\'"]',  # $.get('url') or axios.get('url')
                r'\.post\([\'"]([^\'\"]+)[\'"]',  # $.post('url') or axios.post('url')
                r'ajax\(\s*\{[^}]*url:\s*[\'"]([^\'\"]+)[\'"]',  # $.ajax({url: 'url'})
                r'apiUrl\s*=\s*[\'"]([^\'\"]+)[\'"]',  # apiUrl = 'url'
                r'endpoint\s*=\s*[\'"]([^\'\"]+)[\'"]',  # endpoint = 'url'
            ]
            for pattern in api_patterns:
                matches = re.findall(pattern, text)
                for match in matches:
                    if any(keyword in match.lower() for keyword in ['shop', 'store', 'tenant', 'mall', 'dining', 'api', '/get/', '/data/']):
                        api_patterns_found.append(match)

            # Look for JSON assignments
            if any(pattern in text for pattern in ['JSON.parse', '__INITIAL', 'window.', 'var shops', 'const stores']):
                features["has_json_script"] = True

                # Try to extract actual JSON
                json_patterns = [
                    r'(\{[\s\S]{50,2000}?\})',  # JSON objects
                    r'(\[[\s\S]{50,2000}?\])',  # JSON arrays
                ]
                for pattern in json_patterns:
                    matches = re.findall(pattern, text)
                    if matches:
                        features["json_sample"] = matches[0][:1000]
                        break

                # Extract variable names
                var_patterns = re.findall(r'(?:window\.|var |const |let )(\w+)\s*=', text[:1000])
                features["script_patterns"].extend(var_patterns[:5])

        # Add discovered API patterns to features
        if api_patterns_found:
            features["api_patterns_in_js"] = list(set(api_patterns_found))[:10]  # Dedupe and limit

        # Find likely shop names (text that repeats in pattern)
        text_elements = {}
        for tag in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'a', 'span', 'div']):
            text = tag.get_text(strip=True)
            if 5 < len(text) < 80:  # Reasonable shop name length
                parent_class = ' '.join(tag.parent.get('class', []))
                if parent_class:
                    key = (parent_class, tag.name)
                    if key not in text_elements:
                        text_elements[key] = []
                    text_elements[key].append(text)

        # Find patterns with multiple instances
        for (parent_class, tag_name), texts in text_elements.items():
            if len(texts) >= 3:  # At least 3 shops
                features["likely_shop_names"].extend(texts[:5])
                break

        # Check for pagination
        pagination_keywords = ['next', 'prev', 'page', '下一頁', '上一頁', 'load more', '更多']
        for keyword in pagination_keywords:
            if soup.find(string=re.compile(keyword, re.I)):
                features["pagination_present"] = True
                break

        return features

    def _generate_and_execute_extraction(self):
        """Use AI to generate extraction CONFIG and execute with engine"""
        import time

        max_retries = 2
        retry_delay = 5  # seconds

        for attempt in range(max_retries):
            try:
                # Fetch sample HTML
                logger.info(f"Fetching {self.root_url} for analysis...")
                resp = self.session.get(self.root_url, timeout=15)
                html = resp.text

                # Extract features (NOT raw HTML!)
                logger.info("Extracting HTML features...")
                html_features = self._extract_html_features(html)
                logger.info(f"   Found {len(html_features.get('links', []))} links")
                logger.info(f"   Found {len(html_features.get('top_classes', []))} repeated classes")
                logger.info(f"   JSON in scripts: {html_features.get('has_json_script', False)}")
                if 'api_patterns_in_js' in html_features:
                    logger.info(f"   API patterns found in JS: {html_features['api_patterns_in_js']}")

                # Get site analysis (from discovery step)
                site_analysis = getattr(self.config, 'ai_analysis', {})

                # Generate extraction CONFIG (not code!)
                if attempt > 0:
                    logger.info(f"🔄 Retry {attempt}/{max_retries-1}: Asking AI for extraction config...")
                else:
                    logger.info("🤖 Asking AI for extraction config...")

                result = self.ai_client.generate_extraction_config(
                    self.root_url,
                    html_features,
                    site_analysis
                )

                # Check for error response
                if 'error' in result:
                    error_msg = result['error']
                    logger.warning(f"⚠️  AI API error: {error_msg}")

                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    else:
                        logger.error("❌ Max retries exceeded. Config generation failed.")
                        return

                if 'extraction_type' in result:
                    config = result
                    extraction_type = config.get('extraction_type')
                    confidence = config.get('confidence', 0)

                    logger.info(f"✅ AI generated extraction config:")
                    logger.info(f"   Type: {extraction_type}")
                    logger.info(f"   Confidence: {confidence:.0%}")
                    logger.info(f"   Config size: {len(str(config))} chars")
                    logger.info(f"   Config: {json.dumps(config, ensure_ascii=False)}")

                    # Save config for reuse
                    self.config.extraction_config = config

                    # Execute using config-based engine
                    self._execute_config_based_extraction(config, html)
                    return  # Success!
                else:
                    logger.error(f"AI response missing 'extraction_type': {result}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue

            except Exception as e:
                logger.error(f"Error generating extraction code (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    import traceback
                    traceback.print_exc()
                    logger.error("❌ Code generation failed after all retries")

    def _execute_config_based_extraction(self, config: Dict, html: str):
        """Execute extraction with automatic fallback if primary method fails"""
        try:
            # DEFAULT: Use Playwright for all malls (slower but more accurate)
            # Playwright can click into shop detail pages to extract accurate floor codes
            logger.info("🎭 Using Playwright extraction (default for all malls - clicks shop details for accurate data)...")
            self._extract_via_playwright(self.root_url, click_details=True)
            return

            # Legacy code below (not used - keeping for reference)
            extraction_type = config.get('extraction_type')
            logger.info(f"⚡ Executing {extraction_type} extraction...")

            if extraction_type == 'api':
                self._extract_via_api(config)
            elif extraction_type == 'html':
                self._extract_via_html(config, html)
            elif extraction_type == 'json_embedded':
                self._extract_via_embedded_json(config, html)
            else:
                logger.error(f"Unknown extraction type: {extraction_type}")

            # Automatic fallback if extraction got 0 records
            if len(self.raw_records) == 0:
                logger.warning(f"⚠️  {extraction_type} extraction got 0 records - trying fallback...")

                if extraction_type == 'api':
                    # Try embedded JSON first
                    logger.info("Fallback 1: Trying embedded JSON extraction...")
                    self._extract_via_embedded_json_auto(html)

                if len(self.raw_records) == 0:
                    # Try generic HTML extraction
                    logger.info("Fallback 2: Trying generic HTML extraction...")
                    self._extract_via_html_auto(html)

                if len(self.raw_records) == 0:
                    # Last resort: Playwright to render JavaScript
                    logger.info("Fallback 3: Trying Playwright (JavaScript rendering)...")
                    self._extract_via_playwright(self.root_url)

        except Exception as e:
            logger.error(f"Error executing config-based extraction: {e}")
            import traceback
            traceback.print_exc()

    def _extract_via_api(self, config: Dict):
        """Extract data via API endpoint with automatic variation attempts"""
        api_endpoint = config.get('api_endpoint', '')
        method = config.get('method', 'GET').upper()
        field_mappings = config.get('field_mappings', {})

        logger.info(f"Trying API: {api_endpoint}")

        # Build full URL
        if api_endpoint.startswith('/'):
            base_url = self.root_url.rstrip('/')
            full_url = base_url + api_endpoint
        else:
            full_url = api_endpoint

        # Try different variations (mall_id, id, etc.)
        for mall_id in range(1, 20):
            url = full_url.replace('{mall_id}', str(mall_id)).replace('{id}', str(mall_id))

            try:
                resp = self.session.request(method, url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()

                    # Navigate to data path
                    data_path = config.get('data_path', '.')
                    if data_path != '.':
                        for key in data_path.split('.'):
                            data = data.get(key, data)

                    # Convert to RawRecord
                    if isinstance(data, list):
                        for item in data:
                            record = RawRecord(
                                source_url=url,
                                source_section="api",
                                scraped_at=datetime.now().isoformat(),
                                raw_data=item,
                                extraction_method="ai_config_api"
                            )
                            self.raw_records.append(record)
                        logger.info(f"  API {mall_id}: {len(data)} records")
            except:
                pass

        # If initial endpoint failed, try common variations
        if len(self.raw_records) == 0:
            logger.info(f"⚠️  Initial API endpoint got 0 records, trying common variations...")

            # Parse base URL
            from urllib.parse import urlparse
            parsed = urlparse(self.root_url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"

            # Common API path patterns for mall/shop data
            common_patterns = [
                '/get/shopping?mall_id={mall_id}',
                '/get/dining?mall_id={mall_id}',
                '/api/shops?mall_id={mall_id}',
                '/api/stores?mall_id={mall_id}',
                '/api/tenants?mall_id={mall_id}',
                '/data/shops?mall_id={mall_id}',
            ]

            for pattern in common_patterns:
                logger.info(f"   Trying pattern: {pattern}")
                for mall_id in range(1, 20):
                    url = base_url + pattern.replace('{mall_id}', str(mall_id))

                    try:
                        resp = self.session.request(method, url, timeout=10)
                        if resp.status_code == 200:
                            data = resp.json()

                            # Try to find list in common locations
                            if isinstance(data, list):
                                items = data
                            elif isinstance(data, dict):
                                # Try common data paths
                                items = (data.get('data') or data.get('shops') or
                                        data.get('stores') or data.get('items') or
                                        data.get('tenants') or [])
                            else:
                                items = []

                            if isinstance(items, list) and len(items) > 0:
                                for item in items:
                                    record = RawRecord(
                                        source_url=url,
                                        source_section="api",
                                        scraped_at=datetime.now().isoformat(),
                                        raw_data=item,
                                        extraction_method="ai_config_api_variation"
                                    )
                                    self.raw_records.append(record)
                                logger.info(f"  ✅ Found data with pattern {pattern} (mall_id={mall_id}): {len(items)} records")
                    except:
                        pass

                # If we found data with this pattern, stop trying others
                if len(self.raw_records) > 0:
                    logger.info(f"✅ Successfully found data using variation: {pattern}")
                    break

        logger.info(f"✅ Extracted {len(self.raw_records)} records via API")

    def _extract_via_html(self, config: Dict, html: str):
        """Extract data via HTML selectors"""
        from bs4 import BeautifulSoup

        list_selector = config.get('list_selector', '')
        field_selectors = config.get('field_selectors', {})

        soup = BeautifulSoup(html, 'html.parser')
        items = soup.select(list_selector)

        logger.info(f"Found {len(items)} items with selector: {list_selector}")

        for item in items:
            raw_data = {}
            for field, selector in field_selectors.items():
                elem = item.select_one(selector)
                if elem:
                    raw_data[field] = elem.get_text(strip=True)

            if raw_data:
                record = RawRecord(
                    source_url=self.root_url,
                    source_section="html",
                    scraped_at=datetime.now().isoformat(),
                    raw_data=raw_data,
                    extraction_method="ai_config_html"
                )
                self.raw_records.append(record)

        logger.info(f"✅ Extracted {len(self.raw_records)} records via HTML")

    def _extract_via_embedded_json(self, config: Dict, html: str):
        """Extract data from embedded JSON in script tags"""
        from bs4 import BeautifulSoup

        script_pattern = config.get('script_pattern', '')
        data_path = config.get('data_path', '')

        soup = BeautifulSoup(html, 'html.parser')

        for script in soup.find_all('script'):
            text = script.string or ''
            if script_pattern in text:
                # Try to extract JSON
                import re
                json_match = re.search(r'\{.*\}', text, re.DOTALL)
                if json_match:
                    try:
                        data = json.loads(json_match.group())

                        # Navigate data path
                        for key in data_path.split('.'):
                            data = data.get(key, data)

                        if isinstance(data, list):
                            for item in data:
                                record = RawRecord(
                                    source_url=self.root_url,
                                    source_section="json_embedded",
                                    scraped_at=datetime.now().isoformat(),
                                    raw_data=item,
                                    extraction_method="ai_config_json"
                                )
                                self.raw_records.append(record)
                            break
                    except:
                        continue

        logger.info(f"✅ Extracted {len(self.raw_records)} records via embedded JSON")

    def _extract_via_embedded_json_auto(self, html: str):
        """Automatic: Extract JSON from script tags without config"""
        from bs4 import BeautifulSoup
        import re

        soup = BeautifulSoup(html, 'html.parser')

        for script in soup.find_all('script'):
            text = script.string or ''

            # Look for array-like structures that might be shops
            array_patterns = [
                r'\bshops?\s*[:=]\s*(\[[\s\S]{100,5000}?\])',
                r'\bstores?\s*[:=]\s*(\[[\s\S]{100,5000}?\])',
                r'\bitems?\s*[:=]\s*(\[[\s\S]{100,5000}?\])',
                r'\bdata\s*[:=]\s*(\[[\s\S]{100,5000}?\])',
            ]

            for pattern in array_patterns:
                matches = re.findall(pattern, text, re.I)
                for match in matches:
                    try:
                        data = json.loads(match)
                        if isinstance(data, list) and len(data) > 2:
                            # Looks like a list of items
                            for item in data:
                                if isinstance(item, dict):
                                    record = RawRecord(
                                        source_url=self.root_url,
                                        source_section="json_auto",
                                        scraped_at=datetime.now().isoformat(),
                                        raw_data=item,
                                        extraction_method="auto_json"
                                    )
                                    self.raw_records.append(record)
                            logger.info(f"✅ Auto-extracted {len(data)} records from embedded JSON")
                            return
                    except:
                        continue

    def _extract_via_html_auto(self, html: str):
        """Automatic: Extract from HTML without config by finding repeated patterns"""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, 'html.parser')

        # Find repeated element structures that look like shop cards
        class_counter = {}
        for tag in soup.find_all(class_=True):
            # Filter out likely non-data elements
            text = tag.get_text(strip=True)

            # Skip if:
            # - Too short (< 10 chars) - likely UI elements like buttons
            # - Contains no link - shop cards usually link to detail pages
            # - Is a common UI element (button, dropdown, icon)
            if len(text) < 10:
                continue

            has_link = tag.name == 'a' or tag.find('a')
            if not has_link:
                continue

            # Skip common UI element class names
            classes_str = ' '.join(tag.get('class', []))
            if any(skip in classes_str.lower() for skip in ['button', 'btn', 'dropdown', 'icon', 'menu', 'nav']):
                continue

            classes = ' '.join(sorted(tag.get('class', [])))
            if classes:
                class_counter[classes] = class_counter.get(classes, 0) + 1

        # Get elements that repeat 10+ times (likely shop cards)
        repeated = [(cls, count) for cls, count in class_counter.items() if count >= 10]
        if not repeated:
            logger.warning("No repeated shop card patterns found (need 10+ occurrences with links and text > 10 chars)")
            logger.warning("This page may require Playwright for JavaScript rendering")
            return

        repeated.sort(key=lambda x: x[1], reverse=True)
        top_class = repeated[0][0]
        count = repeated[0][1]

        logger.info(f"Found repeated pattern: '{top_class[:50]}...' ({count} times)")

        # Extract all instances
        items = soup.find_all(class_=top_class.split())

        for item in items[:200]:  # Limit to first 200
            # Extract all text content
            raw_data = {
                "text": item.get_text(strip=True),
                "html": str(item)[:500]
            }

            # Extract shop name - try multiple strategies
            shop_name = None

            # Strategy 1: Look for headings (h1-h6)
            headings = item.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
            if headings:
                shop_name = headings[0].get_text(strip=True)

            # Strategy 2: Look for anchor text (links often contain shop names)
            if not shop_name:
                links = item.find_all('a', href=True)
                if links:
                    # Store all link URLs
                    raw_data["links"] = [a.get('href') for a in links[:3]]
                    # Use first substantial link text as name
                    for link in links:
                        link_text = link.get_text(strip=True)
                        if len(link_text) > 2 and len(link_text) < 100:
                            shop_name = link_text
                            break

            # Strategy 3: Look for elements with "name", "title", "shop", "store" in class
            if not shop_name:
                name_candidates = item.find_all(class_=lambda c: c and any(
                    keyword in ' '.join(c).lower()
                    for keyword in ['name', 'title', 'shop', 'store', 'brand']
                ))
                if name_candidates:
                    shop_name = name_candidates[0].get_text(strip=True)

            # Strategy 4: Look for largest text block (likely the name)
            if not shop_name:
                text_elements = item.find_all(['div', 'span', 'p'])
                text_blocks = [(elem.get_text(strip=True), len(elem.get_text(strip=True)))
                              for elem in text_elements]
                # Filter: reasonable length (3-100 chars), not too short, not entire card
                candidates = [text for text, length in text_blocks
                             if 3 < length < 100]
                if candidates:
                    shop_name = candidates[0]

            # Store the shop name we found
            if shop_name:
                raw_data["heading"] = shop_name

            # Try to extract data attributes
            data_attrs = {k: v for k, v in item.attrs.items() if k.startswith('data-')}
            if data_attrs:
                raw_data.update(data_attrs)

            if raw_data.get("text") or raw_data.get("heading"):
                record = RawRecord(
                    source_url=self.root_url,
                    source_section="html_auto",
                    scraped_at=datetime.now().isoformat(),
                    raw_data=raw_data,
                    extraction_method="auto_html"
                )
                self.raw_records.append(record)

        logger.info(f"✅ Auto-extracted {len(self.raw_records)} records from HTML patterns")

    def _extract_via_playwright(self, url: str, click_details: bool = None):
        """Automatic: Use Playwright to render JavaScript and extract from DOM

        Args:
            url: URL to extract from
            click_details: If True, click into each shop's detail page for accurate floor extraction
                          If None, auto-detect based on URL (e.g., APM uses detail extraction)
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("Playwright not installed. Run: pip install playwright && python -m playwright install chromium")
            return

        # Auto-detect if we should click details (for sites with Chinese floor names)
        if click_details is None:
            # APM has Chinese floor names, so use detail extraction
            click_details = 'hkapm.com' in url.lower()

        if click_details:
            logger.info("🎭 Launching Playwright browser with DEEP extraction (clicking shop details)...")
        else:
            logger.info("🎭 Launching Playwright browser to render JavaScript...")

        try:
            with sync_playwright() as p:
                # Launch headless browser
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()

                # Navigate to page
                logger.info(f"Loading: {url}")
                page.goto(url, wait_until='networkidle', timeout=60000)

                # Wait a bit for dynamic content to load
                page.wait_for_timeout(3000)

                # Get rendered HTML
                rendered_html = page.content()

                # Note: Don't close browser yet if we need to click details

                logger.info("✅ Page rendered successfully")

                # Now use auto HTML extraction on the rendered content
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(rendered_html, 'html.parser')

                # Find repeated element structures that look like shop cards
                class_counter = {}
                for tag in soup.find_all(class_=True):
                    text = tag.get_text(strip=True)

                    # Skip too short or too long text (shop cards usually 20-500 chars)
                    if len(text) < 20 or len(text) > 500:
                        continue

                    # Must have a link
                    has_link = tag.name == 'a' or tag.find('a')
                    if not has_link:
                        continue

                    # Skip UI elements
                    classes_str = ' '.join(tag.get('class', []))
                    if any(skip in classes_str.lower() for skip in ['button', 'btn', 'dropdown', 'icon', 'menu', 'nav', 'filter', 'header', 'footer']):
                        continue

                    # Skip elements that look like non-shop content
                    # - Opening hours: "星期" (day of week), time patterns
                    # - Too many numbers without letters (likely not a shop name)
                    if any(pattern in text for pattern in ['星期一', '星期二', '星期三', '星期四', '星期五', '星期六', '星期日', '公眾假期']):
                        continue

                    # Must have some alphabetic content (shop names have letters)
                    import re
                    if not re.search(r'[A-Za-z\u4e00-\u9fff]', text):
                        continue

                    classes = ' '.join(sorted(tag.get('class', [])))
                    if classes:
                        class_counter[classes] = class_counter.get(classes, 0) + 1

                # Get elements that repeat 10+ times (likely shop cards)
                repeated = [(cls, count) for cls, count in class_counter.items() if count >= 10]

                if not repeated:
                    logger.warning("No repeated shop card patterns found even after JavaScript rendering")
                    return

                repeated.sort(key=lambda x: x[1], reverse=True)
                top_class = repeated[0][0]
                count = repeated[0][1]

                logger.info(f"Found repeated pattern in rendered page: '{top_class[:50]}...' ({count} times)")

                # Extract all instances
                items = soup.find_all(class_=top_class.split())

                for item in items[:200]:
                    raw_data = {
                        "text": item.get_text(strip=True),
                        "html": str(item)[:500]
                    }

                    # Extract shop name - try multiple strategies
                    shop_name = None

                    # Strategy 1: Look for headings (h1-h6)
                    headings = item.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                    if headings:
                        shop_name = headings[0].get_text(strip=True)

                    # Strategy 2: Look for anchor text (links often contain shop names)
                    if not shop_name:
                        links = item.find_all('a', href=True)
                        if links:
                            # Store all link URLs
                            raw_data["links"] = [a.get('href') for a in links[:3]]
                            # Use first substantial link text as name
                            for link in links:
                                link_text = link.get_text(strip=True)
                                if len(link_text) > 2 and len(link_text) < 100:
                                    shop_name = link_text
                                    break

                    # Strategy 3: Look for elements with "name", "title", "shop", "store" in class
                    if not shop_name:
                        name_candidates = item.find_all(class_=lambda c: c and any(
                            keyword in ' '.join(c).lower()
                            for keyword in ['name', 'title', 'shop', 'store', 'brand']
                        ))
                        if name_candidates:
                            shop_name = name_candidates[0].get_text(strip=True)

                    # Strategy 4: Look for largest text block (likely the name)
                    if not shop_name:
                        text_elements = item.find_all(['div', 'span', 'p'])
                        text_blocks = [(elem.get_text(strip=True), len(elem.get_text(strip=True)))
                                      for elem in text_elements]
                        # Filter: reasonable length (3-100 chars), not too short, not entire card
                        candidates = [text for text, length in text_blocks
                                     if 3 < length < 100]
                        if candidates:
                            shop_name = candidates[0]

                    # Clean up shop name
                    if shop_name:
                        import re
                        # Remove common noise patterns like floor/unit info from name
                        # e.g., "Shop Name一期, L3, 357舖" -> "Shop Name"
                        shop_name = re.split(r'[,，]', shop_name)[0].strip()
                        # Remove trailing location indicators
                        shop_name = re.sub(r'\s*(一期|二期|三期|LB|UB|L\d+|G/F|B\d+|舖).*$', '', shop_name).strip()

                    # Store the shop name we found
                    if shop_name and len(shop_name) > 1:
                        raw_data["heading"] = shop_name

                    # Extract data attributes
                    data_attrs = {k: v for k, v in item.attrs.items() if k.startswith('data-')}
                    if data_attrs:
                        raw_data.update(data_attrs)

                    # DEEP EXTRACTION: Click into shop detail page if requested
                    if click_details and raw_data.get("links"):
                        detail_link = raw_data["links"][0]

                        # Make link absolute if relative
                        from urllib.parse import urljoin
                        detail_url = urljoin(url, detail_link)

                        try:
                            logger.debug(f"Clicking into detail page: {detail_url}")
                            page.goto(detail_url, wait_until='networkidle', timeout=30000)
                            page.wait_for_timeout(2000)

                            # Extract floor and shop number from detail page
                            detail_content = page.content()
                            detail_soup = BeautifulSoup(detail_content, 'html.parser')

                            # Look for shop code / floor information in common locations
                            # Strategy 1: Look for text with patterns like "Shop LB-06", "Unit G-123", etc.
                            detail_text = detail_soup.get_text()

                            # Strategy 2: Look for labeled fields (Shop No, Floor, Unit, etc.)
                            import re
                            shop_code_patterns = [
                                r'Shop\s*(?:No|Number|Code)?[:\s]+([A-Z0-9\-]+)',
                                r'Unit\s*(?:No|Number|Code)?[:\s]+([A-Z0-9\-]+)',
                                r'店鋪編號[:\s]+([A-Z0-9\-]+)',
                                r'舖位編號[:\s]+([A-Z0-9\-]+)',
                            ]

                            floor_code_patterns = [
                                r'Floor[:\s]+([A-Z0-9\-]+)',
                                r'樓層[:\s]+([A-Z0-9\-]+)',
                            ]

                            extracted_shop_code = None
                            extracted_floor = None

                            # Try to extract shop code
                            for pattern in shop_code_patterns:
                                match = re.search(pattern, detail_text, re.I)
                                if match:
                                    extracted_shop_code = match.group(1).strip()
                                    logger.debug(f"Extracted shop code from detail: {extracted_shop_code}")
                                    break

                            # Try to extract floor
                            for pattern in floor_code_patterns:
                                match = re.search(pattern, detail_text, re.I)
                                if match:
                                    extracted_floor = match.group(1).strip()
                                    logger.debug(f"Extracted floor from detail: {extracted_floor}")
                                    break

                            # Store extracted info if found
                            if extracted_shop_code:
                                raw_data["shop_code_detail"] = extracted_shop_code
                            if extracted_floor:
                                raw_data["floor_detail"] = extracted_floor

                            # Go back to listing page for next iteration
                            page.go_back(wait_until='networkidle', timeout=30000)
                            page.wait_for_timeout(1000)

                        except Exception as e:
                            logger.warning(f"Failed to extract from detail page {detail_url}: {e}")
                            # Try to go back to listing page
                            try:
                                page.go_back(timeout=10000)
                            except:
                                # If go_back fails, reload the listing page
                                page.goto(url, wait_until='networkidle', timeout=30000)
                                page.wait_for_timeout(2000)

                    if raw_data.get("text") or raw_data.get("heading"):
                        record = RawRecord(
                            source_url=url,
                            source_section="playwright_auto",
                            scraped_at=datetime.now().isoformat(),
                            raw_data=raw_data,
                            extraction_method="playwright"
                        )
                        self.raw_records.append(record)

                # Close browser
                browser.close()

                logger.info(f"✅ Auto-extracted {len(self.raw_records)} records via Playwright")

        except Exception as e:
            logger.error(f"Playwright extraction failed: {e}", exc_info=True)

    def _deduplicate_records(self):
        """Remove duplicate raw records based on content hash"""
        if not self.raw_records:
            return

        original_count = len(self.raw_records)
        seen_hashes = set()
        unique_records = []

        for record in self.raw_records:
            # Create hash from key identifying fields
            data = record.raw_data

            # Extract key fields for hash (name + location/text)
            name = (data.get('heading') or data.get('name') or
                   data.get('name_tc') or data.get('name_en') or '')
            text = data.get('text', '')
            location = data.get('location', '')

            # Create composite key
            key = f"{name}|{text}|{location}".lower().strip()

            # Hash the key
            import hashlib
            record_hash = hashlib.md5(key.encode('utf-8')).hexdigest()

            if record_hash not in seen_hashes:
                seen_hashes.add(record_hash)
                unique_records.append(record)

        duplicates_removed = original_count - len(unique_records)
        if duplicates_removed > 0:
            logger.info(f"🔍 Removed {duplicates_removed} duplicate records ({len(unique_records)} unique)")
            self.raw_records = unique_records

    def _infer_category_from_url(self, url: str) -> Optional[str]:
        """
        Infer category from URL path segments

        Common patterns:
        - /shopping/ or /時尚購物/ → Shopping
        - /dining/ or /餐飲/ → Dining
        - /entertainment/ or /娛樂/ → Entertainment
        """
        import re
        from urllib.parse import unquote

        # Decode URL to handle Chinese characters
        url = unquote(url)

        # Common category mappings (Chinese & English)
        category_patterns = {
            # Shopping
            r'(?:shopping|shop|時尚|購物|fashion|retail|boutique)': 'Shopping',
            # Dining
            r'(?:dining|food|restaurant|餐飲|美食|cafe|coffee)': 'Dining',
            # Entertainment
            r'(?:entertainment|娛樂|消閒|leisure|recreation)': 'Entertainment',
            # Lifestyle
            r'(?:lifestyle|生活|品味|wellness|beauty|健康)': 'Lifestyle',
            # Services
            r'(?:service|服務|medical|clinic|bank|atm)': 'Services',
        }

        # Search for patterns in URL path
        for pattern, category in category_patterns.items():
            if re.search(pattern, url, re.I):
                return category

        return None

    def _normalize_data(self):
        """Step 3: Normalize to canonical schema"""
        self.normalized_records = []

        for raw in self.raw_records:
            data = raw.raw_data

            # Handle auto-extracted HTML records (from _extract_via_html_auto or _extract_via_playwright)
            if raw.extraction_method in ("auto_html", "playwright"):
                # heading likely contains shop name
                name = data.get('heading') or data.get('name_tc') or data.get('name_en') or data.get('name')
                # text field might contain location info
                location = data.get('text', '')

                # Fallback: Extract name from comma-separated text format
                # Format: "Shop Name一期, L3, 301舖" → name = "Shop Name"
                if not name and location:
                    import re
                    # Split by comma and take first segment, remove phase markers (一期, 二期, etc.)
                    first_segment = re.split(r'[,，]', location)[0].strip()
                    # Remove phase/building markers: 一期, 二期, 三期, Phase 1, etc.
                    name = re.sub(r'\s*(一期|二期|三期|四期|Phase\s*[0-9]+)\s*$', '', first_segment, flags=re.I).strip()
                    if name:
                        logger.debug(f"Extracted name '{name}' from text field: {location[:50]}")
            else:
                # Standard extraction (API or AI-config based)
                name = data.get('name') or data.get('name_tc') or data.get('name_en')
                # location can be in 'location' or 'floor' field
                location = data.get('location') or data.get('floor', '')

            category = data.get('category') or data.get('type')

            # Infer category from URL if missing (common for category landing pages)
            if not category:
                category = self._infer_category_from_url(raw.source_url)

            website = data.get('url') or data.get('website')

            # Extract floor and shop number from location text
            floor_from_location, shop_from_location = extract_floor_and_shop_from_location(location)

            # Parse detail page shop code (format: "LB-06", "G-123", "L3-456", etc.)
            floor_from_detail = data.get('floor_detail')
            shop_from_detail = data.get('shop_code_detail')

            # If shop_code_detail contains floor info (e.g., "LB-06"), parse it
            if shop_from_detail and not floor_from_detail:
                import re
                # Try to split shop code into floor + shop number
                match = re.match(r'^([A-Z]+[0-9]*)[:\-\s]+([0-9A-Z]+)$', str(shop_from_detail), re.I)
                if match:
                    floor_from_detail = match.group(1).upper()
                    shop_from_detail = match.group(2).upper()
                    logger.debug(f"Parsed shop code '{shop_from_detail}' into floor='{floor_from_detail}', shop='{shop_from_detail}'")

            # Extract floor
            # Priority 0: Floor from detail page (most accurate - extracted via Playwright)
            floor_canonical = floor_from_detail

            # Priority 1: Direct floor field from AI extraction
            if not floor_canonical:
                floor_canonical = data.get('floor')

            # Priority 2: Use AI-discovered malllevel_id mapping (if available)
            if not floor_canonical and hasattr(self.config, 'floor_mapping') and self.config.floor_mapping:
                level_id = data.get('malllevel_id')
                if level_id in self.config.floor_mapping:
                    floor_canonical = self.config.floor_mapping[level_id]

            # Priority 3: Extract from location text
            if not floor_canonical:
                floor_canonical = floor_from_location

            # Store raw floor for debugging
            floor_raw = location or data.get('floor', '')

            # Extract shop number
            # Priority 0: Shop code from detail page (most accurate - extracted via Playwright)
            shop_no = shop_from_detail

            # Priority 1: Direct shop_number field from AI extraction
            if not shop_no:
                shop_no = data.get('shop_number')

            # Priority 2: Other common field names
            if not shop_no:
                shop_no = data.get('display_unit') or data.get('unit_no') or shop_from_location

            # Smart floor inference: If floor is still missing, try to infer from shop number
            if not floor_canonical and shop_no:
                # Extract floor prefix from shop number (e.g., "G123" → G, "L3-456" → L3, "B1-02" → B1)
                import re
                shop_str = str(shop_no)

                # Try specific floor patterns first (most specific to least specific)
                floor_patterns = [
                    (r'^(LB|UB|LG|UG|UC|LC)(?:[0-9])', r'\1'),  # LB06 → LB, UB04 → UB
                    (r'^(L[0-9]+|B[0-9]+|P[0-9]+|M[0-9]*)', r'\1'),  # L3-456 → L3, B1-02 → B1
                    (r'^(G|C)(?:[^A-Z]|$)', r'\1'),  # G123 → G, C-05 → C (but not GA, CA)
                ]

                for pattern, replacement in floor_patterns:
                    match = re.match(pattern, shop_str, re.I)
                    if match:
                        potential_floor = match.expand(replacement).upper()
                        # Validate it looks like a floor code
                        valid_floor_pattern = r'^(G|B[0-9]+|L[0-9]+|LB|UB|LG|UG|C|UC|LC|M[0-9]*|P[0-9]+)$'
                        if re.match(valid_floor_pattern, potential_floor):
                            floor_canonical = potential_floor
                            logger.debug(f"Inferred floor {floor_canonical} from shop_number {shop_no}")
                            break

            # Default missing floors to Ground for API-based extractions (common pattern)
            if not floor_canonical and raw.extraction_method in ("ai_config_api", "ai_config_api_variation"):
                # Only default to G if we have some location context suggesting it's a physical shop
                if shop_no or location:
                    floor_canonical = "G"
                    logger.debug(f"Defaulted floor to G for shop {name} (shop_no: {shop_no})")

            # Create normalized record
            normalized = NormalizedRecord(
                name=name,
                floor=floor_canonical,
                shop_number=shop_no,
                category=category,
                website=website,
                name_en=data.get('name_en'),
                name_tc=data.get('name_tc'),
                name_sc=data.get('name_sc'),
                raw_floor=floor_raw,
                source_url=raw.source_url,
                source_section=raw.source_section,
                extraction_method=raw.extraction_method
            )

            self.normalized_records.append(normalized)

        logger.info(f"Normalized {len(self.normalized_records)} records")

    def _evaluate_quality(self) -> EvaluationReport:
        """Step 4: Evaluate data quality"""
        if not self.normalized_records:
            return EvaluationReport(
                total_records=0,
                field_coverage={},
                overall_coverage=0.0,
                missing_fields={},
                unknown_floors=[],
                selector_failures=[],
                top_failures=[]
            )

        total = len(self.normalized_records)
        required_fields = ['name', 'floor', 'shop_number', 'category']

        # Calculate coverage
        field_coverage = {}
        missing_fields = {}

        for field in required_fields:
            populated = sum(
                1 for r in self.normalized_records
                if getattr(r, field, None) is not None
            )
            field_coverage[field] = populated / total
            missing_fields[field] = total - populated

        overall_coverage = sum(field_coverage.values()) / len(field_coverage)

        # Find unknown floors
        unknown_floors = set()
        # Valid canonical formats: G, B1-B9, L1-L99, LB, UB, LG, UG, C, UC, LC, M, P1-P9
        valid_floor_pattern = r'^(G|B[0-9]+|L[0-9]+|LB|UB|LG|UG|C|UC|LC|M[0-9]*|P[0-9]+)$'
        for r in self.normalized_records:
            if r.floor and not re.match(valid_floor_pattern, r.floor):
                unknown_floors.add(r.floor)

        # Identify top failures
        failures = []
        for field, count in missing_fields.items():
            if count > 0:
                failures.append({
                    "issue": f"Missing {field}",
                    "count": count,
                    "pct": count / total
                })

        failures.sort(key=lambda x: x['count'], reverse=True)

        # Collect sample records with issues (for debugging)
        sample_issues = {}
        max_samples = 5  # Show up to 5 examples per issue

        # Sample records missing each field
        for field in required_fields:
            missing = [
                {
                    "name": r.name or "(missing)",
                    "floor": r.floor or "(missing)",
                    "shop_number": r.shop_number or "(missing)",
                    "category": r.category or "(missing)",
                    "raw_floor": r.raw_floor or "",
                    "source_url": r.source_url or "",
                    "extraction_method": r.extraction_method or ""
                }
                for r in self.normalized_records
                if getattr(r, field, None) is None
            ]
            if missing:
                sample_issues[f"missing_{field}"] = missing[:max_samples]

        # Sample records with unknown floors
        unknown_floor_records = [
            {
                "name": r.name or "(missing)",
                "floor": r.floor or "(missing)",
                "shop_number": r.shop_number or "(missing)",
                "raw_floor": r.raw_floor or "",
                "source_url": r.source_url or ""
            }
            for r in self.normalized_records
            if r.floor and not re.match(valid_floor_pattern, r.floor)
        ]
        if unknown_floor_records:
            sample_issues["unknown_floors"] = unknown_floor_records[:max_samples]

        # Log results
        logger.info(f"Total records: {total}")
        logger.info(f"Overall coverage: {overall_coverage:.1%}")
        logger.info("\nField coverage:")
        for field, coverage in field_coverage.items():
            status = "✅" if coverage >= 0.90 else "⚠️" if coverage >= 0.70 else "❌"
            logger.info(f"  {status} {field}: {coverage:.1%}")

        if failures:
            logger.info("\nTop issues:")
            for i, failure in enumerate(failures[:3], 1):
                logger.info(f"  {i}. {failure['issue']}: {failure['count']} records ({failure['pct']:.1%})")

        return EvaluationReport(
            total_records=total,
            field_coverage=field_coverage,
            overall_coverage=overall_coverage,
            missing_fields=missing_fields,
            unknown_floors=list(unknown_floors),
            selector_failures=[],
            top_failures=failures,
            sample_issues=sample_issues
        )

    def _attempt_repair(self, evaluation: EvaluationReport, iteration: int) -> bool:
        """Step 5: Attempt to repair data quality issues"""
        if not evaluation.top_failures:
            logger.info("No issues to repair")
            return False

        top_issue = evaluation.top_failures[0]
        logger.info(f"Analyzing top issue: {top_issue['issue']}")
        logger.info(f"  Affected: {top_issue['count']} records ({top_issue['pct']:.1%})")

        # Special handling for floor issues
        if 'floor' in top_issue['issue'].lower():
            logger.info("\n🔧 Detected floor extraction issue - using AI floor mapper...")
            return self._repair_floors()

        # Use AI agent for other repair types
        if self.ai_client:
            # Get sample records with the issue
            field_name = top_issue['issue'].replace('Missing ', '')
            sample_records = [
                r.to_dict() for r in self.normalized_records
                if getattr(r, field_name, None) is None
            ][:5]

            logger.info("\n🤖 Consulting AI agent for solution...")
            solution = self.ai_client.fix_data_quality_issue(
                issue_description=top_issue['issue'],
                sample_records=sample_records,
                website_context=f"URL: {self.root_url}, Sections: {list(self.config.sections.keys())}"
            )

            logger.info(f"AI Solution: {json.dumps(solution, indent=2, ensure_ascii=False)}")

            # Apply solution (implementation depends on solution type)
            # For now, log and continue
            return True
        else:
            logger.warning("No AI agent available for repair")
            return False

    def _repair_floors(self) -> bool:
        """Use AI to discover malllevel_id → floor mapping and apply it"""
        from ai.floor_mapper import FloorMapper

        # Check if we already have a mapping
        if hasattr(self.config, 'floor_mapping') and self.config.floor_mapping:
            logger.info("Floor mapping already exists - skipping discovery")
            return False

        mapper = FloorMapper()

        # Discover mapping from raw records
        mapping = mapper.discover_mapping(self.raw_records, self.ai_client)

        if not mapping:
            logger.warning("Could not discover floor mapping")
            return False

        logger.info(f"✅ Discovered floor mapping for {len(mapping)} levels:")
        for level_id, floor in sorted(mapping.items())[:10]:
            logger.info(f"   malllevel_id {level_id} → {floor}")

        # SAVE mapping to config so it persists across iterations
        if not hasattr(self.config, 'floor_mapping'):
            self.config.floor_mapping = {}
        self.config.floor_mapping.update(mapping)

        logger.info(f"✅ Saved floor mapping to config - will be used in next extraction")
        return True  # Trigger re-normalization

    def _save_results(self, evaluation: EvaluationReport, iterations: int):
        """Save all results with mall-specific filenames"""
        # Save normalized data as JSON
        json_file = self.output_dir / f"{self.mall_name}_normalized_data.json"
        export_to_json(self.normalized_records, json_file)

        # Save as CSV
        csv_file = self.output_dir / f"{self.mall_name}_normalized_data.csv"
        export_to_csv(self.normalized_records, csv_file)

        # Save evaluation report
        report_file = self.output_dir / f"{self.mall_name}_quality_report.json"
        create_summary_report(
            evaluation,
            report_file,
            metadata={
                "mall_name": self.mall_name,
                "root_url": self.root_url,
                "iterations": iterations,
                "api_calls": self.ai_client.api_call_count if self.ai_client else 0,
                "timestamp": datetime.now().isoformat()
            }
        )

        # Save config
        config_file = self.output_dir / f"{self.mall_name}_site_config.json"
        self.config.save(config_file)

        logger.info(f"\n✅ Results saved to {self.output_dir.absolute()}")
        logger.info(f"   - {json_file.name}")
        logger.info(f"   - {csv_file.name}")
        logger.info(f"   - {report_file.name}")
        logger.info(f"   - {config_file.name}")
