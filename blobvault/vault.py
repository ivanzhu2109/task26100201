"""BlobVault 批次入库主入口。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import BinaryIO, Iterable

from .db import MetadataStore
from .errors import (
    AliasConflictError,
    BatchItemsRequiredError,
    NotFoundError,
)
from .models import BatchItem, BatchResult, ItemResult
from .storage import FileStore


class BlobVault:
    """批次写入、按摘要或别名流式读取的对象库。"""

    def __init__(self, root: str | Path) -> None:
        root_path = Path(root)
        root_path.mkdir(parents=True, exist_ok=True)
        self._files = FileStore(root_path)
        self._meta = MetadataStore(root_path / "meta.sqlite")

    def close(self) -> None:
        self._meta.close()

    def __enter__(self) -> "BlobVault":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def commit_batch(
        self,
        batch_id: str,
        items: Iterable[BatchItem] | None = None,
    ) -> BatchResult:
        """提交一个批次；已提交批次可只传 batch_id 取回原结果。"""
        existing = self._meta.get_batch_result(batch_id)
        if existing is not None:
            return _deserialize_result(existing)
        if items is None:
            raise BatchItemsRequiredError(
                f"batch {batch_id!r} is not committed; items are required"
            )

        staged: list[tuple[Path, str, int, str | None]] = []
        try:
            for item in items:
                path, digest, size = self._files.stage(item.stream)
                staged.append((path, digest, size, item.alias))
        except BaseException:
            for path, _, _, _ in staged:
                self._files.discard(path)
            raise

        try:
            _validate_aliases(staged)
        except BaseException:
            for path, _, _, _ in staged:
                self._files.discard(path)
            raise

        placed: list[str] = []
        try:
            seen: set[str] = set()
            for path, digest, _, _ in staged:
                if digest in seen or self._meta.has_object(digest):
                    self._files.discard(path)
                    continue
                seen.add(digest)
                if self._files.place(path, digest):
                    placed.append(digest)
            records = [
                (digest, size, alias) for _, digest, size, alias in staged
            ]
            result = BatchResult(
                batch_id=batch_id,
                items=tuple(
                    ItemResult(digest=digest, size=size, alias=alias)
                    for _, digest, size, alias in staged
                ),
            )
            self._meta.commit_batch(
                batch_id,
                records,
                _serialize_result(result),
            )
            return result
        except BaseException:
            for digest in placed:
                if not self._meta.has_object(digest):
                    self._files.remove_object(digest)
            raise

    def open_by_digest(self, digest: str) -> BinaryIO:
        """按摘要流式读取已提交对象。"""
        if not self._meta.has_object(digest):
            raise NotFoundError(f"object {digest!r} is not committed")
        return self._files.open(digest)

    def open_by_alias(self, alias: str) -> BinaryIO:
        """按别名流式读取已提交对象。"""
        digest = self._meta.resolve_alias(alias)
        if digest is None:
            raise NotFoundError(f"alias {alias!r} is not committed")
        return self._files.open(digest)


def _validate_aliases(
    staged: list[tuple[Path, str, int, str | None]],
) -> dict[str, str]:
    """校验同批别名互不重复；同别名同摘要的去重为一。"""
    registrations: dict[str, str] = {}
    for _, digest, _, alias in staged:
        if alias is None:
            continue
        bound = registrations.get(alias)
        if bound is not None and bound != digest:
            raise AliasConflictError(alias, existing_digest=bound)
        registrations[alias] = digest
    return registrations


def _serialize_result(result: BatchResult) -> str:
    return json.dumps(
        {
            "batch_id": result.batch_id,
            "items": [
                {"digest": i.digest, "size": i.size, "alias": i.alias}
                for i in result.items
            ],
        }
    )


def _deserialize_result(payload: str) -> BatchResult:
    data = json.loads(payload)
    return BatchResult(
        batch_id=data["batch_id"],
        items=tuple(
            ItemResult(digest=i["digest"], size=i["size"], alias=i["alias"])
            for i in data["items"]
        ),
    )
