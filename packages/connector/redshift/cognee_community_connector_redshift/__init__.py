"""Amazon Redshift connector package for Cognee."""

from .config import RedshiftConfig
from .redshift import (
    DOCUMENT_SOURCE_ATTR,
    REDSHIFT_SOURCE_NAME,
    REDSHIFT_TABLE_NAME,
    RedshiftClient,
    redshift_source,
)

__all__ = [
    "DOCUMENT_SOURCE_ATTR",
    "REDSHIFT_SOURCE_NAME",
    "REDSHIFT_TABLE_NAME",
    "RedshiftClient",
    "RedshiftConfig",
    "redshift_source",
]
