"""Data models for autonomous scraping system"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional, Tuple
import re
import json
from pathlib import Path


@dataclass
class RawRecord:
    """Raw scraped data with provenance"""
    source_url: str
    source_section: str
    scraped_at: str
    raw_data: Dict[str, Any]
    extraction_method: str  # "api", "html", "playwright"


@dataclass
class NormalizedRecord:
    """Normalized data in canonical schema"""
    # Core fields
    name: Optional[str] = None
    floor: Optional[str] = None  # Canonical: "G", "B1", "L1", etc.
    shop_number: Optional[str] = None
    category: Optional[str] = None

    # Additional fields
    website: Optional[str] = None

    # Multi-language support
    name_en: Optional[str] = None
    name_tc: Optional[str] = None
    name_sc: Optional[str] = None

    # Provenance
    raw_floor: Optional[str] = None
    source_url: Optional[str] = None
    source_section: Optional[str] = None
    extraction_method: Optional[str] = None

    def to_dict(self) -> Dict:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class EvaluationReport:
    """Data quality evaluation"""
    total_records: int
    field_coverage: Dict[str, float]  # field → % populated
    overall_coverage: float

    # Failure analysis
    missing_fields: Dict[str, int]  # field → count missing
    unknown_floors: List[str]  # floor values not normalized
    selector_failures: List[str]  # CSS selectors that failed

    top_failures: List[Dict[str, Any]]  # Top issues to fix

    # Sample records with issues (for debugging)
    sample_issues: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)  # issue_type → sample records

    def passes_threshold(self, threshold: float) -> bool:
        """Check if coverage meets threshold"""
        return self.overall_coverage >= threshold

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class SiteConfig:
    """Site-specific configuration (gets patched during repair loop)"""
    sections: Dict[str, str] = field(default_factory=dict)  # section_name → URL
    extraction_rules: Dict[str, Dict] = field(default_factory=dict)  # section → rules
    floor_patterns: List[Tuple[re.Pattern, str]] = field(default_factory=list)  # (regex, replacement)
    selectors: Dict[str, Dict[str, str]] = field(default_factory=dict)  # section → CSS selectors
    api_endpoints: Dict[str, str] = field(default_factory=dict)  # section → API endpoint

    def save(self, filepath: Path):
        """Save config to JSON"""
        config_dict = {
            'sections': self.sections,
            'extraction_rules': self.extraction_rules,
            'floor_patterns': [(p.pattern, r) for p, r in self.floor_patterns],
            'selectors': self.selectors,
            'api_endpoints': self.api_endpoints
        }
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(config_dict, f, indent=2, ensure_ascii=False)

    @classmethod
    def load(cls, filepath: Path) -> 'SiteConfig':
        """Load config from JSON"""
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Convert floor patterns back to regex
        floor_patterns = [
            (re.compile(pattern), replacement)
            for pattern, replacement in data.get('floor_patterns', [])
        ]

        return cls(
            sections=data.get('sections', {}),
            extraction_rules=data.get('extraction_rules', {}),
            floor_patterns=floor_patterns,
            selectors=data.get('selectors', {}),
            api_endpoints=data.get('api_endpoints', {})
        )


@dataclass
class PageClassification:
    """Classification of a web page"""
    url: str
    page_type: str  # "api", "html", "spa", "playwright_required"
    section_type: str  # "shopping", "dining", "services", etc.
    confidence: float
    evidence: Dict[str, Any] = field(default_factory=dict)
