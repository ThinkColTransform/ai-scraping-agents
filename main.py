"""
Autonomous Mall Scraper - Main Entry Point

This is the main file to run the autonomous scraping system.

Usage:
    python main.py

Configuration:
    Set your Azure OpenAI credentials in .env file:
    - AZURE_OPENAI_ENDPOINT
    - AZURE_OPENAI_API_KEY
    - AOAI_DEPLOYMENT_NAME
"""

import sys
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv
import os
import codecs

# Fix Windows console encoding for Unicode
if sys.platform == 'win32':
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from core import AutonomousMallScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('scraper.log', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Main entry point"""

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Autonomous Mall Scraper')
    parser.add_argument('url', nargs='?', default=None,
                       help='Target mall website URL (e.g., https://www.hkapm.com.hk/shop/)')
    parser.add_argument('--coverage', type=float, default=0.90,
                       help='Coverage threshold (default: 0.90)')
    parser.add_argument('--max-iterations', type=int, default=5,
                       help='Maximum iterations (default: 5)')
    parser.add_argument('--output-dir', default='./output',
                       help='Output directory (default: ./output)')

    args = parser.parse_args()

    # Load environment variables from .env
    load_dotenv()

    # Verify .env is configured
    if not os.getenv("AZURE_OPENAI_ENDPOINT"):
        logger.warning("="*70)
        logger.warning("⚠️  Azure OpenAI not configured!")
        logger.warning("="*70)
        logger.warning("The system will run in MOCK mode (simulated AI agents).")
        logger.warning("")
        logger.warning("To enable real AI agents:")
        logger.warning("1. Copy .env.example to .env")
        logger.warning("2. Fill in your Azure OpenAI credentials:")
        logger.warning("   - AZURE_OPENAI_ENDPOINT")
        logger.warning("   - AZURE_OPENAI_API_KEY")
        logger.warning("   - AOAI_DEPLOYMENT_NAME")
        logger.warning("="*70)
        logger.warning("")

    # Print header
    logger.info("="*70)
    logger.info("AUTONOMOUS MALL SCRAPER")
    logger.info("="*70)
    logger.info("")

    # Configuration
    # Use command line arg if provided, otherwise use default
    target_url = args.url or "https://www.hkapm.com.hk/shop/"
    coverage_threshold = args.coverage
    max_iterations = args.max_iterations
    output_dir = args.output_dir

    logger.info(f"Target URL: {target_url}")
    logger.info(f"Coverage threshold: {coverage_threshold:.0%}")
    logger.info(f"Max iterations: {max_iterations}")
    logger.info(f"Output directory: {output_dir}")
    logger.info("")

    # Initialize scraper
    scraper = AutonomousMallScraper(
        root_url=target_url,
        output_dir=output_dir,
        use_ai_agents=True  # Will use mock mode if no credentials
    )

    # Run autonomous scraping
    try:
        result = scraper.run(
            coverage_threshold=coverage_threshold,
            max_iterations=max_iterations
        )

        # Print results
        logger.info("")
        logger.info("="*70)
        logger.info("SCRAPING COMPLETE")
        logger.info("="*70)
        logger.info(f"Success: {'✅ YES' if result['success'] else '⚠️  NO'}")
        logger.info(f"Iterations: {result['iterations']}")
        logger.info(f"Coverage: {result['coverage']:.1%}")
        logger.info(f"Total records: {result['total_records']}")
        logger.info(f"Azure OpenAI API calls: {result['api_calls']}")
        logger.info(f"Output directory: {result['output_dir']}")
        logger.info("="*70)

        if result['success']:
            logger.info("")
            logger.info("✅ Coverage threshold achieved!")
            logger.info(f"   Data exported to: {result['output_dir']}")
            logger.info("")
            logger.info("Files created (check output directory for mall-specific filenames):")
        else:
            logger.warning("")
            logger.warning(f"⚠️  Coverage threshold not met: {result['coverage']:.1%} < {coverage_threshold:.0%}")
            logger.warning(f"   Completed {result['iterations']} iterations")
            logger.warning(f"   Check output directory for quality report with details on issues")

        return 0 if result['success'] else 1

    except KeyboardInterrupt:
        logger.info("\n\nScraping interrupted by user")
        return 1

    except Exception as e:
        logger.error(f"\n\nError during scraping: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
