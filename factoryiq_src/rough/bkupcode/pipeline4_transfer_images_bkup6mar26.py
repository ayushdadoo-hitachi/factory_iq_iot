import logging
from databricks.sdk.runtime import dbutils   # REQUIRED inside modules

logger = logging.getLogger(__name__)


def run_pipeline4_transfer_images(
    source: str,
    dest: str,
    dry_run: bool = False,
):
    """
    Stage 4 – Transfer weld path images from Volumes to ABFSS.
    """
    logger.info("--------------------------------------------------")
    logger.info("Starting Pipeline 4 – Image Transfer")
    logger.info("--------------------------------------------------")

    files = dbutils.fs.ls(source)

    logger.info(f"Found {len(files)} items to copy.")

    for f in files:
        target_path = dest + "/" + f.name

        logger.info(
            f"{'DRY RUN:' if dry_run else 'COPYING:'} {f.path} -> {target_path}"
        )

        if not dry_run:
            dbutils.fs.cp(f.path, target_path, recurse=True)

    logger.info("Pipeline 4 – Image Transfer completed successfully")