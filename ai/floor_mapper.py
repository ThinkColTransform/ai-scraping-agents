"""AI-powered floor mapping discovery"""

import json
import logging
from typing import Dict, List, Any
from collections import defaultdict

logger = logging.getLogger(__name__)


class FloorMapper:
    """
    Uses AI agents to discover malllevel_id → floor mappings

    Problem: malllevel_id is a database ID, not a floor number
    Solution: Analyze shops that have floor info in location text,
              discover the pattern, apply to all shops
    """

    def discover_mapping(
        self,
        raw_records: List[Any],
        ai_client: Any
    ) -> Dict[int, str]:
        """
        Discover malllevel_id → canonical floor mapping using AI

        Uses multiple signals:
        1. Floor info in location text (e.g., "1/F", "G/F")
        2. Shop number patterns (e.g., "Shop 201" → Level 2)
        3. Mall-specific context

        Args:
            raw_records: Raw shop records with malllevel_id and location
            ai_client: AzureOpenAIClient instance

        Returns:
            Dict mapping malllevel_id → canonical floor ("G", "B1", "L1", etc.)
        """
        # Group records by malllevel_id
        by_level = defaultdict(list)
        for record in raw_records:
            level_id = record.raw_data.get('malllevel_id')
            location = record.raw_data.get('location', '')
            shop_no = record.raw_data.get('display_unit', '')

            if level_id is not None:
                by_level[level_id].append({
                    'location': location,
                    'shop_no': shop_no,
                    'name': record.raw_data.get('name_en') or record.raw_data.get('name'),
                })

        # Extract training samples with ANY useful floor signals
        training_samples = []
        for level_id, shops in by_level.items():
            for shop in shops[:5]:  # Take up to 5 samples per level
                location = shop['location']
                shop_no = shop['shop_no']

                # Collect samples with floor info in location
                has_floor_in_location = any(
                    pattern in location
                    for pattern in ['G/F', '/F', '樓', '楼', 'Floor', 'Level', '地下', '地庫']
                )

                # Also collect samples with shop numbers (for pattern inference)
                if has_floor_in_location or shop_no:
                    training_samples.append({
                        'malllevel_id': level_id,
                        'location': location,
                        'shop_no': shop_no,
                        'shop': shop['name']
                    })

        logger.info(f"Found {len(training_samples)} training samples (location + shop numbers)")

        if not training_samples:
            logger.warning("No training samples found - cannot discover mapping")
            return {}

        # Ask AI agent to discover the pattern
        if ai_client and not ai_client.mock_mode:
            logger.info("🤖 Asking AI agent to discover malllevel_id → floor mapping...")

            prompt = f"""
Analyze these shop records and discover the pattern between malllevel_id and floor number.

IMPORTANT: Each mall may have different floor structures. Use ALL available clues:
1. Floor info in location text (e.g., "1/F", "G/F", "2樓")
2. Shop number patterns (e.g., "Shop 201-202" → likely Level 2, "Shop G38" → likely Ground)
3. Mall-specific patterns

TRAINING DATA:
{json.dumps(training_samples[:40], indent=2, ensure_ascii=False)}

INFERENCE RULES:
- Shop numbers 001-099 or G01-G99 → Ground floor (G)
- Shop numbers 101-199 → Level 1 (L1)
- Shop numbers 201-299 → Level 2 (L2)
- Shop numbers 301-399 → Level 3 (L3)
- Shop numbers B01-B99 → Basement (B1)
- But verify against location text when available!

TASK:
1. Group shops by malllevel_id
2. For each malllevel_id, look at:
   - Location text (if it has floor info like "1/F")
   - Shop numbers (e.g., all shops with malllevel_id=X have shop_no in 200s → Level 2)
3. Make your best inference for each malllevel_id
4. Assign confidence based on evidence quality

Canonical floor formats:
- Ground: "G"
- Basement: "B1", "B2", etc.
- Levels: "L1", "L2", "L3", etc.

Respond in JSON format:
{{
  "mapping": {{
    "7": "L2",
    "10": "G",
    "19": "L1",
    ...
  }},
  "confidence": 0.0-1.0,
  "reasoning": "Explain the pattern for each level, e.g.: 'malllevel_id 7: Shop numbers 201-210, inferred L2'"
}}
"""

            result = ai_client.create_agent(
                system_prompt="You are a floor mapping discovery agent. Analyze building floor patterns and create accurate mappings.",
                user_prompt=prompt,
                response_format="json_object"
            )

            if 'mapping' in result:
                # Convert string keys to int
                mapping = {int(k): v for k, v in result['mapping'].items()}
                logger.info(f"AI discovered {len(mapping)} floor mappings")
                logger.info(f"Confidence: {result.get('confidence', 0):.1%}")
                logger.info(f"Reasoning: {result.get('reasoning', 'N/A')}")
                return mapping

        # Fallback: Simple heuristic mapping
        logger.info("Using heuristic floor mapping (no AI available)")
        return self._heuristic_mapping(training_samples)

    def _heuristic_mapping(self, samples: List[Dict]) -> Dict[int, str]:
        """
        Fallback heuristic mapping when AI not available
        Uses shop number patterns and location text
        """
        import re
        from collections import Counter

        # Group samples by malllevel_id
        by_level = defaultdict(list)
        for sample in samples:
            by_level[sample['malllevel_id']].append(sample)

        mapping = {}

        for level_id, shops in by_level.items():
            floor_votes = []

            for shop in shops:
                location = shop['location']
                shop_no = shop.get('shop_no', '')

                # Priority 1: Extract from location text
                if 'G/F' in location or '地下' in location:
                    floor_votes.append('G')
                elif 'B1' in location or 'B/1' in location or '地庫' in location:
                    floor_votes.append('B1')
                elif '1/F' in location or '1樓' in location or '1楼' in location:
                    floor_votes.append('L1')
                elif '2/F' in location or '2樓' in location or '2楼' in location:
                    floor_votes.append('L2')
                elif '3/F' in location or '3樓' in location:
                    floor_votes.append('L3')

                # Priority 2: Infer from shop number patterns
                if shop_no and not floor_votes:
                    # Extract leading digits
                    match = re.match(r'([A-Z])?(\d+)', shop_no.upper())
                    if match:
                        prefix = match.group(1)
                        number = int(match.group(2))

                        if prefix == 'G' or (number < 100 and number > 0):
                            floor_votes.append('G')
                        elif prefix == 'B':
                            floor_votes.append('B1')
                        elif 100 <= number < 200:
                            floor_votes.append('L1')
                        elif 200 <= number < 300:
                            floor_votes.append('L2')
                        elif 300 <= number < 400:
                            floor_votes.append('L3')
                        elif 400 <= number < 500:
                            floor_votes.append('L4')

            # Take majority vote
            if floor_votes:
                counter = Counter(floor_votes)
                most_common = counter.most_common(1)[0][0]
                mapping[level_id] = most_common
                logger.info(f"  malllevel_id {level_id} → {most_common} (votes: {dict(counter)})")

        return mapping
