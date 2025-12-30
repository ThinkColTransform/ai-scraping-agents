"""Data export utilities"""

import csv
import json
from pathlib import Path
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


def export_to_csv(records: List[Any], output_path: Path, fieldnames: List[str] = None):
    """
    Export records to CSV

    Args:
        records: List of records (dicts or dataclass instances)
        output_path: Output CSV file path
        fieldnames: List of field names (optional, auto-detected if not provided)
    """
    if not records:
        logger.warning("No records to export")
        return

    # Convert dataclass instances to dicts
    dict_records = []
    for record in records:
        if hasattr(record, 'to_dict'):
            dict_records.append(record.to_dict())
        elif isinstance(record, dict):
            dict_records.append(record)
        else:
            dict_records.append(record.__dict__)

    # Auto-detect fieldnames if not provided
    if not fieldnames and dict_records:
        fieldnames = list(dict_records[0].keys())

    # Write CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(dict_records)

    logger.info(f"Exported {len(dict_records)} records to {output_path}")


def export_to_json(data: Any, output_path: Path, indent: int = 2):
    """
    Export data to JSON

    Args:
        data: Data to export (dict, list, or dataclass)
        output_path: Output JSON file path
        indent: JSON indentation (default: 2)
    """
    # Convert dataclass instances
    if hasattr(data, 'to_dict'):
        json_data = data.to_dict()
    elif isinstance(data, list):
        json_data = []
        for item in data:
            if hasattr(item, 'to_dict'):
                json_data.append(item.to_dict())
            elif hasattr(item, '__dict__'):
                json_data.append(item.__dict__)
            else:
                json_data.append(item)
    else:
        json_data = data

    # Write JSON
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=indent, ensure_ascii=False)

    logger.info(f"Exported data to {output_path}")


def create_summary_report(
    evaluation: Any,
    output_path: Path,
    metadata: Dict[str, Any] = None
):
    """
    Create a summary report of scraping results

    Args:
        evaluation: EvaluationReport instance
        output_path: Output path for report
        metadata: Additional metadata to include
    """
    report = {
        "summary": {
            "total_records": evaluation.total_records,
            "overall_coverage": f"{evaluation.overall_coverage:.1%}",
            "field_coverage": {
                field: f"{coverage:.1%}"
                for field, coverage in evaluation.field_coverage.items()
            }
        },
        "issues": {
            "missing_fields": evaluation.missing_fields,
            "unknown_floors": evaluation.unknown_floors[:10],  # Top 10
            "top_failures": evaluation.top_failures[:5]  # Top 5
        }
    }

    # Add sample records with issues (for debugging)
    if hasattr(evaluation, 'sample_issues') and evaluation.sample_issues:
        report["sample_records_with_issues"] = evaluation.sample_issues

    if metadata:
        report["metadata"] = metadata

    export_to_json(report, output_path)
