import logging
import os

def setup_logging():
    cwd = os.getcwd()
    filename = os.path.join(cwd, "logs", "pipeline.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        filename=filename,     # optional
        filemode="a"
    )