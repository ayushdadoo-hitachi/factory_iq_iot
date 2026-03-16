import logging

def setup_logging():
    root = logging.getLogger()

    # Remove ALL existing handlers (including Spark ones)
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )