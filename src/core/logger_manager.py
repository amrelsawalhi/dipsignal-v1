import logging
import sys
from logging.handlers import RotatingFileHandler

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO) # Ignore DEBUG noise, focus on INFO and above

    # 1. THE FORMATTER: Professional timestamping (ISO 8601 format)
    # Includes: Date Time | Level | Script Name | Message
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 2. THE FILE HANDLER: Your persistent record
    # Rotates at 5MB, keeps 2 backup files
    file_handler = RotatingFileHandler('dipsignal.log', maxBytes=5*1024*1024, backupCount=2)
    file_handler.setFormatter(formatter)

    # 3. THE STREAM HANDLER: Shows it in your VS Code terminal in real-time
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger