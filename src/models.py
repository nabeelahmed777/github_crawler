from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Repository:
    """Immutable data model for GitHub repositories"""

    id: str
    name: str
    owner: str
    name_with_owner: str
    stargazers_count: int
    url: str
    description: Optional[str]
    primary_language: Optional[str]
    created_at: datetime
    updated_at: datetime
    crawled_at: datetime
