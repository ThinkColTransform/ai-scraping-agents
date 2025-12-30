# Autonomous Mall Scraper

An AI-powered autonomous scraper for Fortune Malls shopping centers in Hong Kong. Uses Azure OpenAI (GPT-5.2) for intelligent data extraction and quality assurance.

## Overview

This scraper automatically discovers, extracts, normalizes, and validates shop directory data from 16 Fortune Malls shopping centers using a 5-step autonomous workflow:

1. **Recon**: Discover site structure and classify pages
2. **Extract**: Scrape data using optimal strategy
3. **Normalize**: Convert to canonical schema
4. **Evaluate**: Assess data quality
5. **Repair**: Fix issues and iterate until quality threshold met

## Features

- **Autonomous operation**: Minimal configuration required
- **AI-powered repair**: Uses Azure OpenAI GPT-5.2 for intelligent floor mapping
- **Quality-driven**: Iterates until 90% coverage threshold achieved
- **Multi-language support**: English, Traditional Chinese, Simplified Chinese
- **Multiple output formats**: CSV, JSON, quality reports
- **Comprehensive logging**: Detailed execution logs and metrics

## Requirements

- Python 3.8+
- Azure OpenAI credentials (optional - runs in mock mode without)

## Installation

1. Clone or download this repository

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure Azure OpenAI (optional):
```bash
cp .env.example .env
# Edit .env and add your credentials:
# AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
# AZURE_OPENAI_API_KEY=your-api-key
# AOAI_DEPLOYMENT_NAME=gpt-5.2
```

## Usage

### Basic Usage

Run the autonomous scraper:

```bash
python main.py
```

This will:
1. Discover Fortune Malls site structure
2. Extract shop data from all active malls
3. Normalize to canonical format
4. Evaluate data quality (target: 90% coverage)
5. Apply AI-powered repairs if needed
6. Export results to `./output/`

### Output Files

After completion, check `./output/`:
- `normalized_data.json` - All normalized records
- `normalized_data.csv` - Spreadsheet format
- `quality_report.json` - Quality metrics and API call count
- `site_config.json` - Discovered site configuration

## Data Schema

### Normalized Record Format

```json
{
  "name": "Shop Name",
  "floor": "G",              // Canonical: G, B1, L1, L2, etc.
  "shop_number": "G38A",
  "category": "Fashion & Shoes",
  "phone": "2633 5219",
  "website": null,
  "hours": "10:00-22:00",
  "name_en": "Shop Name",
  "name_tc": "商店名稱",
  "name_sc": "商店名称",
  "raw_floor": "Shop G38A, +WOO Phase 2",
  "source_url": "https://www.fortunemalls.com.hk/get/shopping?mall_id=7",
  "source_section": "shopping",
  "extraction_method": "api"
}
```

### Quality Report Format

```json
{
  "summary": {
    "total_records": 823,
    "overall_coverage": "93.9%",
    "field_coverage": {
      "name": "100.0%",
      "floor": "75.6%",
      "shop_number": "100.0%",
      "category": "100.0%"
    }
  },
  "issues": {
    "missing_fields": {
      "floor": 201
    },
    "top_failures": [
      {
        "issue": "Missing floor",
        "count": 201,
        "pct": 0.244
      }
    ]
  },
  "metadata": {
    "root_url": "https://www.fortunemalls.com.hk",
    "iterations": 1,
    "api_calls": 0,
    "timestamp": "2025-12-30T12:22:09.699376"
  }
}
```

## Architecture

### Directory Structure

```
autonomous_mall_scraper/
├── ai/                          # AI agent system
│   ├── azure_openai_client.py   # Azure OpenAI wrapper
│   ├── floor_mapper.py          # AI floor mapping discovery
│   └── prompts.py               # System prompts
├── core/                        # Core scraper logic
│   ├── autonomous_scraper.py    # 5-step orchestrator
│   └── models.py                # Data models
├── utils/                       # Utilities
│   ├── export.py                # Export functions
│   └── normalization.py         # Data normalization
├── main.py                      # Entry point
├── requirements.txt             # Dependencies
└── .env                         # Configuration (create from .env.example)
```

### Key Components

**Azure OpenAI Client** (`ai/azure_openai_client.py`)
- Wraps Azure OpenAI API
- Supports GPT-4, GPT-4-turbo, GPT-5.2
- Tracks API call count
- Mock mode fallback

**Floor Mapper** (`ai/floor_mapper.py`)
- Discovers `malllevel_id` → floor mappings using AI
- Uses shop number patterns (e.g., 201-299 → L2)
- Takes majority vote from multiple signals
- Heuristic fallback when AI unavailable

**Autonomous Scraper** (`core/autonomous_scraper.py`)
- Orchestrates 5-step workflow
- Quality-driven iteration
- Configurable coverage threshold
- Comprehensive logging

## Expected Output

### Console Output

```
============================================================
AUTONOMOUS MALL SCRAPER - STARTING
============================================================
Target: https://www.fortunemalls.com.hk
Coverage threshold: 90%
Max iterations: 5

============================================================
STEP 1: RECON / STRUCTURE DISCOVERY
============================================================
✅ Detected Fortune Malls - using known structure
   Sections: ['shopping', 'dining']

============================================================
STEP 2: EXTRACTION
============================================================
Fetching mall_id=3...
  → Extracted 109 shops
Fetching mall_id=4...
  → Extracted 175 shops
...
✅ Total raw records: 823

============================================================
STEP 3: NORMALIZATION
============================================================
Normalized 823 records

============================================================
STEP 4: EVALUATION
============================================================
Total records: 823
Overall coverage: 93.9%

Field coverage:
  ✅ name: 100.0%
  ⚠️ floor: 75.6%
  ✅ shop_number: 100.0%
  ✅ category: 100.0%

✅ Coverage threshold met: 93.9% >= 90%

======================================================================
SCRAPING COMPLETE
======================================================================
Success: ✅ YES
Iterations: 1
Coverage: 93.9%
Total records: 823
Azure OpenAI API calls: 0
Output directory: C:\Users\...\output
======================================================================
```

## AI-Powered Features

### Floor Mapping Discovery

The system automatically discovers floor mappings from location text patterns:

**Example floor extraction:**
- "Shop 101, 1/F, +WOO Phase 2" → Floor: `L1`
- "Shop 002, G/F, Belvedere Square" → Floor: `G`
- "Shop 301-303, 3/F, Jubilee Square" → Floor: `L3`

**Shop number inference:**
- Shop 001-099 → Ground floor (`G`)
- Shop 101-199 → Level 1 (`L1`)
- Shop 201-299 → Level 2 (`L2`)
- etc.

## Configuration

### Coverage Threshold

Adjust in `main.py`:
```python
coverage_threshold = 0.90  # 90% required
```

### Max Iterations

Control repair loop:
```python
max_iterations = 5  # Default: 5
```

### Azure OpenAI Model

Set in `.env`:
```bash
AOAI_DEPLOYMENT_NAME=gpt-5.2  # Or gpt-4, gpt-4-turbo
```

## Troubleshooting

### No Azure OpenAI credentials

System runs in **mock mode** with simulated AI responses. Real AI agents require valid credentials.

### Low floor coverage

- Check `quality_report.json` for specific issues
- Review `scraper.log` for extraction errors
- Verify location text patterns in raw data

### Unicode errors (Windows)

Already handled with UTF-8 encoding fixes in code.

## Data Quality

Current performance (93.9% overall coverage):

| Field | Coverage |
|-------|----------|
| Name | 100% |
| Floor | 75.6% |
| Shop Number | 100% |
| Category | 100% |

**Note**: Remaining 24.4% missing floors are shops without floor info in location text (e.g., "Shop 2, Lido Avenue").

## Technical Details

### API Endpoint

```
GET https://www.fortunemalls.com.hk/get/shopping?mall_id={id}
```

- Public access (no authentication)
- Returns JSON array of shop objects
- Mall IDs: 1-16 (14 active)

### Canonical Floor Formats

- Ground: `G`
- Basement: `B1`, `B2`, etc.
- Levels: `L1`, `L2`, `L3`, etc.

### Supported Floor Patterns

- English: `1/F`, `G/F`, `B1`, `L1`
- Chinese: `1樓`, `地下`, `地庫`
- Text: `Ground`, `Level 1`, `Basement`

## License

Provided as-is for educational and research purposes.

## Changelog

### Version 2.0.0 (2025-12-30)
- Complete refactoring to autonomous architecture
- AI-powered floor mapping with GPT-5.2 support
- Quality-driven iteration with 90% threshold
- Fixed floor extraction regex patterns
- API call count tracking in quality reports
- Cleaned codebase (removed old/demo files)

### Version 1.0.0 (2025-12-29)
- Initial Fortune Malls scraper
