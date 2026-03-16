# core/stage_runner.py
from typing import Callable, Iterable, Optional, List, Dict
import importlib

from pyspark.sql import SparkSession

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)

from core.manifest_utils import (
    init_manifest_if_needed,
    is_in_manifest,
    append_manifest,
)

class StageRunner:
    """
    Generic stage runner for: discover -> filter -> process -> finalize.
    """

    def __init__(
        self,
        spark: SparkSession,
        *,
        manifest_path: Optional[str],    # None => no manifest checks/writes
        init_manifest_schema: Optional[str] = None
    ):
        self.spark = spark
        self.manifest_path = manifest_path
        if manifest_path:
            init_manifest_if_needed(spark, manifest_path, init_manifest_schema)

    def run(
        self,
        items: Iterable[str],
        *,
        is_already_done: Optional[Callable[[str], bool]] = None,  # defaults to manifest check
        process_one: Callable[[str], None],                       # stage hook
        finalize_one: Optional[Callable[[str], None]] = None,     # e.g., archive/move
    ) -> List[str]:
        """
        Returns items successfully processed (according to process_one completion).
        """
        processed: List[str] = []
        for idx, item in enumerate(items, start=1):
            # Skip if already processed
            if self.manifest_path:
                if is_already_done is None:
                    if is_in_manifest(self.spark, self.manifest_path, item):
                        logger.info("Skipping already-processed: %s", item)
                        continue
                else:
                    if is_already_done(item):
                        logger.info("Skipping already-processed: %s", item)
                        continue

            try:
                logger.debug("[%d] processing: %s", idx, item)
                process_one(item)  # stage-owned logic
                processed.append(item)

                if self.manifest_path:
                    append_manifest(self.spark, self.manifest_path, item, "success")

                if finalize_one:
                    finalize_one(item)

            except Exception:
                logger.exception("Failed: %s", item)
                if self.manifest_path:
                    append_manifest(self.spark, self.manifest_path, item, "failed")
                # Continue next item
        return processed