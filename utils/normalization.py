"""Data normalization utilities"""

import re
from typing import Optional, Tuple


# Floor normalization patterns (for exact matching - used by normalize_floor)
FLOOR_PATTERNS_EXACT = [
    # Chinese traditional and simplified (generic patterns only - mall-specific terms vary)
    (re.compile(r'^\s*([0-9]+)\s*[樓楼]\s*$'), lambda m: f"L{m.group(1)}"),  # 1樓 → L1
    (re.compile(r'地下|地鋪'), lambda m: "G"),  # 地下 → G (ground floor)
    (re.compile(r'地庫|地?[负負]([0-9]+)'), lambda m: f"B{m.group(1) if m.lastindex else '1'}"),  # 地庫 → B1

    # Special floor codes (common in HK/Asia malls)
    # LB/UB are single levels (Lower/Upper Basement), not numbered like B1/B2
    (re.compile(r'^\s*LB\s*$', re.I), lambda m: "LB"),  # LB → LB
    (re.compile(r'^\s*UB\s*$', re.I), lambda m: "UB"),  # UB → UB
    (re.compile(r'^\s*LG\s*([0-9]*)\s*$', re.I), lambda m: "LG"),  # LG → LG (Lower Ground)
    (re.compile(r'^\s*UG\s*([0-9]*)\s*$', re.I), lambda m: "UG"),  # UG → UG (Upper Ground)
    (re.compile(r'^\s*UC\s*([0-9]*)\s*$', re.I), lambda m: "UC"),  # UC → UC (Upper Concourse)
    (re.compile(r'^\s*LC\s*([0-9]*)\s*$', re.I), lambda m: "LC"),  # LC → LC (Lower Concourse)
    (re.compile(r'^\s*C\s*$', re.I), lambda m: "C"),  # C → C (Concourse)
    (re.compile(r'^\s*M\s*([0-9]*)\s*$', re.I), lambda m: f"M{m.group(1) if m.group(1) else ''}"),  # M → M (Mezzanine)
    (re.compile(r'^\s*P\s*([0-9]+)\s*$', re.I), lambda m: f"P{m.group(1)}"),  # P1 → P1 (Parking)

    # English formats
    (re.compile(r'^\s*([0-9]+)\s*/F\s*$', re.I), lambda m: f"L{m.group(1)}"),  # 1/F → L1
    (re.compile(r'^\s*G\s*/F\s*$', re.I), lambda m: "G"),  # G/F → G
    (re.compile(r'^\s*B\s*([0-9]+)\s*$', re.I), lambda m: f"B{m.group(1)}"),  # B1 → B1
    (re.compile(r'^\s*L\s*([0-9]+)\s*$', re.I), lambda m: f"L{m.group(1)}"),  # L1 → L1

    # Level/Floor prefix
    (re.compile(r'Level\s+([0-9]+)', re.I), lambda m: f"L{m.group(1)}"),  # Level 1 → L1
    (re.compile(r'Floor\s+([0-9]+)', re.I), lambda m: f"L{m.group(1)}"),  # Floor 1 → L1

    # Ground floor variations
    (re.compile(r'Ground', re.I), lambda m: "G"),
    (re.compile(r'^\s*G\s*$', re.I), lambda m: "G"),

    # Basement variations
    (re.compile(r'Basement\s+([0-9]+)', re.I), lambda m: f"B{m.group(1)}"),
    (re.compile(r'Basement', re.I), lambda m: "B1"),
]

# Floor search patterns (for searching within text - used by extract_floor_and_shop_from_location)
# These patterns don't use ^ and $ anchors so they can match within longer strings
FLOOR_PATTERNS_SEARCH = [
    # Special floor codes (common in HK/Asia malls) - MOST SPECIFIC FIRST
    # Note: LB/UB followed by digits in shop numbers (LB06, UB04) should extract just LB/UB as the floor
    (re.compile(r'\bLB\b', re.I), lambda m: "LB"),  # LB → LB (don't capture trailing numbers)
    (re.compile(r'\bUB\b', re.I), lambda m: "UB"),  # UB → UB (don't capture trailing numbers)
    (re.compile(r'\bLG\b', re.I), lambda m: "LG"),  # LG → LG (Lower Ground)
    (re.compile(r'\bUG\b', re.I), lambda m: "UG"),  # UG → UG (Upper Ground)
    (re.compile(r'\bUC\b', re.I), lambda m: "UC"),  # UC → UC (Upper Concourse)
    (re.compile(r'\bLC\b', re.I), lambda m: "LC"),  # LC → LC (Lower Concourse)
    (re.compile(r'\bM\s*([0-9]*)\b', re.I), lambda m: f"M{m.group(1) if m.group(1) else ''}"),  # M → M (Mezzanine)
    (re.compile(r'\bP\s*([0-9]+)\b', re.I), lambda m: f"P{m.group(1)}"),  # P1 → P1 (Parking)

    # English formats - MOST SPECIFIC FIRST
    (re.compile(r'\b([0-9]+)\s*/F\b', re.I), lambda m: f"L{m.group(1)}"),  # 1/F → L1
    (re.compile(r'\bG\s*/F\b', re.I), lambda m: "G"),  # G/F → G
    (re.compile(r'\bB\s*([0-9]+)\b', re.I), lambda m: f"B{m.group(1)}"),  # B1 → B1
    (re.compile(r'\bL\s*([0-9]+)\b', re.I), lambda m: f"L{m.group(1)}"),  # L1 → L1

    # Chinese traditional and simplified
    (re.compile(r'([0-9]+)\s*[樓楼]'), lambda m: f"L{m.group(1)}"),  # 1樓 → L1
    (re.compile(r'地下|地鋪'), lambda m: "G"),  # 地下 → G
    (re.compile(r'地庫|地?[负負]([0-9]+)'), lambda m: f"B{m.group(1) if m.lastindex else '1'}"),  # 地庫 → B1

    # Level/Floor prefix
    (re.compile(r'Level\s+([0-9]+)', re.I), lambda m: f"L{m.group(1)}"),  # Level 1 → L1
    (re.compile(r'Floor\s+([0-9]+)', re.I), lambda m: f"L{m.group(1)}"),  # Floor 1 → L1

    # Ground floor variations
    (re.compile(r'\bGround\b', re.I), lambda m: "G"),

    # Basement variations
    (re.compile(r'Basement\s+([0-9]+)', re.I), lambda m: f"B{m.group(1)}"),
    (re.compile(r'Basement', re.I), lambda m: "B1"),

    # Concourse (must be after UC/LC to avoid conflicts)
    (re.compile(r'\bC\b', re.I), lambda m: "C"),  # C → C (Concourse)
    (re.compile(r'\bG\b', re.I), lambda m: "G"),  # G → G (Ground)
]

# Legacy name for backwards compatibility
FLOOR_PATTERNS = FLOOR_PATTERNS_EXACT


def normalize_floor(raw_floor: str) -> Optional[str]:
    """
    Normalize floor to canonical format

    Canonical formats:
    - Ground: "G"
    - Basement: "B1", "B2", etc.
    - Levels: "L1", "L2", etc.

    Args:
        raw_floor: Raw floor string (e.g., "1樓", "1/F", "Ground", etc.)

    Returns:
        Canonical floor string or None if cannot normalize
    """
    if not raw_floor:
        return None

    raw_floor = str(raw_floor).strip()

    # Try each pattern
    for pattern, replacer in FLOOR_PATTERNS:
        match = pattern.search(raw_floor)
        if match:
            try:
                result = replacer(match)
                return result
            except (IndexError, AttributeError):
                continue

    # If no pattern matched, check if it's already in canonical format
    if re.match(r'^(G|B[0-9]+|L[0-9]+)$', raw_floor):
        return raw_floor

    return None


def extract_shop_number(location: str, unit_field: Optional[str] = None) -> Optional[str]:
    """
    Extract shop number from location text or unit field

    Args:
        location: Location text (e.g., "Shop 201-202, Ma On Shan Plaza" or "一期, L3, 301舖")
        unit_field: Unit/shop number field

    Returns:
        Shop number or None
    """
    # Prefer explicit unit field
    if unit_field:
        # Clean up common prefixes
        cleaned = unit_field.replace("Shop ", "").replace("Unit ", "").strip()
        if cleaned:
            return cleaned

    # Extract from location text
    if location:
        # Try Chinese shop patterns first (most specific)
        # Pattern: "301舖" or "301號" (shop/number suffix)
        chinese_shop = re.search(r'([0-9A-Z\-/]+)\s*[舖铺號号店]', location, re.I)
        if chinese_shop:
            return chinese_shop.group(1)

        # Try comma-separated segments (common in Chinese malls)
        # e.g., "Shop Name一期, L3, 301舖" → look in segments after commas
        segments = re.split(r'[,，]', location)
        for segment in segments:
            segment = segment.strip()
            # Look for shop number with suffix
            shop_in_segment = re.search(r'([0-9A-Z\-/]+)\s*[舖铺號号店]', segment)
            if shop_in_segment:
                return shop_in_segment.group(1)
            # Look for pure number segment (after floor info)
            if re.match(r'^[0-9A-Z\-/]+$', segment) and len(segment) <= 10:
                return segment

        # Try "Shop XXX" pattern (English)
        shop_match = re.search(r'Shop\s+([A-Z0-9\-/]+)', location, re.I)
        if shop_match:
            return shop_match.group(1)

        # Try "Unit XXX" pattern (English)
        unit_match = re.search(r'Unit\s+([A-Z0-9\-/]+)', location, re.I)
        if unit_match:
            return unit_match.group(1)

        # Last resort: number at start (only if it's pure numeric/alphanumeric, not letters)
        start_match = re.match(r'^([0-9][0-9A-Z\-/]*)', location)
        if start_match:
            return start_match.group(1)

    return None


def extract_floor_and_shop_from_location(location: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract both floor and shop number from location text

    Args:
        location: Full location text

    Returns:
        (floor, shop_number) tuple
    """
    floor = None
    shop_no = None

    if not location:
        return None, None

    # Try to extract floor using search patterns (no anchors)
    for pattern, replacer in FLOOR_PATTERNS_SEARCH:
        match = pattern.search(location)
        if match:
            try:
                floor = replacer(match)
                break
            except (IndexError, AttributeError):
                continue

    # Extract shop number
    shop_no = extract_shop_number(location)

    return floor, shop_no


def clean_text(text: Optional[str]) -> Optional[str]:
    """
    Clean and normalize text

    Args:
        text: Raw text

    Returns:
        Cleaned text or None
    """
    if not text:
        return None

    # Strip whitespace
    text = text.strip()

    # Remove multiple spaces
    text = re.sub(r'\s+', ' ', text)

    return text if text else None
