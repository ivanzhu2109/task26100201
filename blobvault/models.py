"""BlobVault 对外数据模型。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import BinaryIO


@dataclass(frozen=True)
class BatchItem:
    """批次中的一个待写入项：顺序可读的二进制流 + 可选别名。"""

    stream: BinaryIO
    alias: str | None = None


@dataclass(frozen=True)
class ItemResult:
    """批次内单个项的提交结果。"""

    digest: str
    size: int
    alias: str | None


@dataclass(frozen=True)
class BatchResult:
    """批次提交结果；已提交批次重试时原样返回。"""

    batch_id: str
    items: tuple[ItemResult, ...]
