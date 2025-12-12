import logging
import sys

import os

def configure_logger(log_file=None):
    """
    Configures the root logger:
    - File Handler: DEBUG level (All details go here)
    - Console Handler: WARNING level (Only errors/warnings go to screen, to keep tqdm clean)
    """
    if log_file is None:
        log_file = os.getenv("GANTRY_LOG_FILE", "gantry.log")
        
    logger = logging.getLogger("gantry")
    logger.setLevel(logging.DEBUG)
    
    # Reset handlers to prevent duplicates on reload
    if logger.handlers:
        logger.handlers = []

    # 1. File Handler
    fh = logging.FileHandler(log_file, mode='w') # Overwrite mode for now per session
    fh.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(file_formatter)
    logger.addHandler(fh)

    # 2. Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING) # Keep console clean for tqdm
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    ch.setFormatter(console_formatter)
    logger.addHandler(ch)

    return logger

def get_logger():
    return logging.getLogger("gantry")
