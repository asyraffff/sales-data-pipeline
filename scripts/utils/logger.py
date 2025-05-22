import logging
import os
from datetime import datetime


# Setup logging
def setup_logging():
    """Configure logging settings with timestamped log file"""
    tagging = "setup_logging"
    try:
        # Create logs directory if it doesn't exist
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Generate timestamp for log file name
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = os.path.join(log_dir, f'pipeline_{timestamp}.log')
        
        # Configure root logger
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        logger = logging.getLogger(__name__)
        logger.info(f"{tagging} - Started new logging session in file: {log_filename}")
        return logger
    except Exception as e:
        print(f"{tagging} - Failed to setup logging: {str(e)}")
        raise