import logging
import os

def setup_logging():
    cwd = os.getcwd()
    log_dir = os.path.join(cwd, "logs")

    # Create logs directory if not exists
    os.makedirs(log_dir, exist_ok=True)

    filename = os.path.join(log_dir, "pipeline.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        filename=filename,     # optional
        filemode="a"
    )