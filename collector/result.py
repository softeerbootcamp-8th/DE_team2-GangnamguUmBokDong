"""Collector execution result models."""

from dataclasses import asdict, dataclass
from enum import StrEnum
import json


class CollectionStatus(StrEnum):
    """Represent the final status of a collection run."""

    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"


@dataclass(frozen=True)
class CollectionResult:
    """Represent the result of a collector execution.

    Attributes:
        run_id: Logical identifier of the collection run.
        source: Name of the collected data source.
        total_pages: Total number of API pages attempted.
        success_pages: Number of successfully collected pages.
        failed_pages: API pages that failed after internal retries.
        failure_ratio: Ratio of failed pages to total pages.
        status: Final collection status.
    """

    run_id: str
    source: str
    total_pages: int
    success_pages: int
    failed_pages: list[int]
    failure_ratio: float
    status: CollectionStatus

    def to_json(self) -> str:
        """Serialize the collection result as JSON.

        Returns:
            str: JSON representation of the collection result.
        """
        return json.dumps(
            asdict(self),
            ensure_ascii=False,
        )


def build_collection_result(
    *,
    run_id: str,
    source: str,
    total_pages: int,
    failed_pages: list[int],
    max_failure_ratio: float,
) -> CollectionResult:
    """Build the final result of a collection run.

    Args:
        run_id: Logical identifier of the collection run.
        source: Name of the data source.
        total_pages: Total number of API pages.
        failed_pages: Pages that failed after all retries.
        max_failure_ratio: Maximum acceptable page failure ratio.

    Returns:
        CollectionResult: Final collection result.
    """
    if total_pages <= 0:
        raise ValueError("total_pages must be greater than 0.")

    failed_count = len(failed_pages)
    success_pages = total_pages - failed_count
    failure_ratio = failed_count / total_pages

    if failed_count == 0:
        status = CollectionStatus.SUCCESS
    elif failure_ratio <= max_failure_ratio:
        status = CollectionStatus.PARTIAL_SUCCESS
    else:
        status = CollectionStatus.FAILED

    return CollectionResult(
        run_id=run_id,
        source=source,
        total_pages=total_pages,
        success_pages=success_pages,
        failed_pages=failed_pages,
        failure_ratio=failure_ratio,
        status=status,
    )