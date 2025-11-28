import logging
import os
from datetime import datetime

import yaml


def load_config(config_path: str = "config/config.yaml"):
    """Read configuration from YAML"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def setup_logging(log_dir: str):
    """ Setup logging configuration
        Adding handlers for file handling for writing to .log file and Streaming logs on the console.

    """
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def create_directories(config: dict):
    """Create necessary directories"""
    for path_key in ['bronze', 'silver', 'gold', 'logs']:
        os.makedirs(config['paths'][path_key], exist_ok=True)
