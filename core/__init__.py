"""Core autonomous scraping framework"""

from .models import RawRecord, NormalizedRecord, SiteConfig, EvaluationReport
from .autonomous_scraper import AutonomousMallScraper

__all__ = [
    'RawRecord',
    'NormalizedRecord',
    'SiteConfig',
    'EvaluationReport',
    'AutonomousMallScraper'
]
