"""BlobVault 批次入库组件。

存储布局（root 目录下）：
    vault.db            SQLite，存对象摘要、别名、批次结果
    objects/xx/<digest> 按内容寻址的原始文件（仅已提交内容）
    staging/            批次写入期的临时文件，提交或失败清理后不留存
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from typing import BinaryIO, Optional, Sequence

from .errors import (
    AliasConflictError,
    BatchAlreadyCommittedError,
    BlobVaultError,
    ObjectNotFoundError,
)

_CHUNK_SIZE = 1024 * 1024

_SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
    digest TEXT PRIMARY KEY,
    size   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS aliases (
    alias  TEXT PRIMARY KEY,
    digest TEXT NOT NULL REFERENCES objects(digest)
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id     TEXT PRIMARY KEY,
    result_json  TEXT NOT NULL,
    committed_at TEXT NOT NULL
);
"""


@dataclasses.dataclass(frozen=True)
class BatchItem:
    """批次中的一个待写入项：二进制流 + 可选别名。"""

    stream: BinaryIO
    alias: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class BatchEntryResult:
    """批次中单项的写入结果。"""

    digest: str
    size: int
    alias: Optional[str]
    deduplicated: bool


@dataclasses.dataclass(frozen=True)
class BatchResult:
    """整批提交结果。"""

    batch_id: str
    entries: tuple[BatchEntryResult, ...]


class BlobVault:
    """内容寻址的批次入库库：原始内容存本地文件，元数据存 SQLite。"""

    def __init__(self, root: str) -> None:
        self._root = os.path.abspath(root)
        self._objects_dir = os.path.join(self._root, "objects")
        self._staging_dir = os.path.join(self._root, "staging")
        os.makedirs(self._objects_dir, exist_ok=True)
        os.makedirs(self._staging_dir, exist_ok=True)
        self._db = sqlite3.connect(os.path.join(self._root, "vault.db"))
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.executescript(_SCHEMA)
        self._db.commit()

    # ------------------------------------------------------------------
    # 批次写入
    # ------------------------------------------------------------------

    def commit_batch(
        self,
        batch_id: str,
        items: Optional[Sequence[BatchItem]] = None,
    ) -> BatchResult:
        """提交一个批次；已提交批次可只凭 batch_id 取回原结果。

        整批原子生效：任何一项读取失败或别名冲突都不会留下部分提交，
        也不影响历史已提交对象。
        """
        if not batch_id:
            raise ValueError("batch_id 不能为空")

        stored = self._load_batch_result(batch_id)
        if stored is not None:
            if items is not None:
                raise BatchAlreadyCommittedError(
                    f"批次 {batch_id!r} 已提交，请勿重复传输数据"
                )
            return stored
        if items is None:
            raise BlobVaultError(f"批次 {batch_id!r} 未提交，必须提供数据流")

        self._check_intra_batch_aliases(items)

        staged: list[tuple[str, int, str]] = []  # (digest, size, staging_path)
        try:
            for item in items:
                staged.append(self._stage_stream(item.stream))
            self._check_alias_conflicts(items, staged)
            entries = self._install_staged(items, staged)
        except BaseException:
            self._discard_staged(staged)
            raise
        self._discard_staged(staged)

        result = BatchResult(batch_id=batch_id, entries=tuple(entries))
        self._record_batch(result)
        return result

    def _check_intra_batch_aliases(self, items: Sequence[BatchItem]) -> None:
        seen: set[str] = set()
        for item in items:
            if item.alias is None:
                continue
            if not item.alias:
                raise AliasConflictError("别名不能为空字符串")
            if item.alias in seen:
                raise AliasConflictError(f"批次内别名重复: {item.alias!r}")
            seen.add(item.alias)

    def _stage_stream(self, stream: BinaryIO) -> tuple[str, int, str]:
        """顺序读取输入流到临时文件，返回 (digest, size, staging_path)。"""
        hasher = hashlib.sha256()
        size = 0
        fd, path = self._new_staging_file()
        try:
            with os.fdopen(fd, "wb") as out:
                while True:
                    chunk = stream.read(_CHUNK_SIZE)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    out.write(chunk)
                    size += len(chunk)
        except BaseException:
            self._remove_quietly(path)
            raise
        return hasher.hexdigest(), size, path

    def _check_alias_conflicts(
        self,
        items: Sequence[BatchItem],
        staged: list[tuple[str, int, str]],
    ) -> None:
        for item, (digest, _size, _path) in zip(items, staged):
            if item.alias is None:
                continue
            row = self._db.execute(
                "SELECT digest FROM aliases WHERE alias = ?", (item.alias,)
            ).fetchone()
            if row is not None and row[0] != digest:
                raise AliasConflictError(
                    f"别名 {item.alias!r} 已指向其他摘要"
                )

    def _install_staged(
        self,
        items: Sequence[BatchItem],
        staged: list[tuple[str, int, str]],
    ) -> list[BatchEntryResult]:
        """把暂存文件落位到 objects/ 并在同一事务中登记对象与别名。"""
        known = {
            row[0]
            for row in self._db.execute("SELECT digest FROM objects")
        }
        entries: list[BatchEntryResult] = []
        new_rows: list[tuple[str, int]] = []
        for item, (digest, size, path) in zip(items, staged):
            deduplicated = digest in known
            if not deduplicated:
                os.replace(path, self._object_path(digest))
                known.add(digest)
                new_rows.append((digest, size))
            entries.append(
                BatchEntryResult(
                    digest=digest,
                    size=size,
                    alias=item.alias,
                    deduplicated=deduplicated,
                )
            )
        with self._db:
            self._db.executemany(
                "INSERT INTO objects (digest, size) VALUES (?, ?)", new_rows
            )
            self._db.executemany(
                "INSERT OR IGNORE INTO aliases (alias, digest) VALUES (?, ?)",
                [(e.alias, e.digest) for e in entries if e.alias is not None],
            )
        return entries

    def _record_batch(self, result: BatchResult) -> None:
        payload = json.dumps(
            [dataclasses.asdict(e) for e in result.entries],
            ensure_ascii=False,
        )
        with self._db:
            self._db.execute(
                "INSERT INTO batches (batch_id, result_json, committed_at)"
                " VALUES (?, ?, ?)",
                (
                    result.batch_id,
                    payload,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def _load_batch_result(self, batch_id: str) -> Optional[BatchResult]:
        row = self._db.execute(
            "SELECT result_json FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if row is None:
            return None
        entries = tuple(
            BatchEntryResult(**raw) for raw in json.loads(row[0])
        )
        return BatchResult(batch_id=batch_id, entries=entries)

    # ------------------------------------------------------------------
    # 读取（仅已提交内容）
    # ------------------------------------------------------------------

    def open_by_digest(self, digest: str) -> BinaryIO:
        if not self.has_object(digest):
            raise ObjectNotFoundError(f"对象不存在或未提交: {digest!r}")
        return open(self._object_path(digest), "rb")

    def open_by_alias(self, alias: str) -> BinaryIO:
        return self.open_by_digest(self.resolve_alias(alias))

    def resolve_alias(self, alias: str) -> str:
        row = self._db.execute(
            "SELECT digest FROM aliases WHERE alias = ?", (alias,)
        ).fetchone()
        if row is None:
            raise ObjectNotFoundError(f"别名不存在: {alias!r}")
        return row[0]

    def has_object(self, digest: str) -> bool:
        row = self._db.execute(
            "SELECT 1 FROM objects WHERE digest = ?", (digest,)
        ).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # 路径与清理
    # ------------------------------------------------------------------

    def _object_path(self, digest: str) -> str:
        subdir = os.path.join(self._objects_dir, digest[:2])
        os.makedirs(subdir, exist_ok=True)
        return os.path.join(subdir, digest)

    def _new_staging_file(self) -> tuple[int, str]:
        return tempfile.mkstemp(prefix="stage-", dir=self._staging_dir)

    def _discard_staged(
        self,
        staged: list[tuple[str, int, str]],
    ) -> None:
        """清理暂存文件；只删 staging/ 下的临时文件，绝不触碰 objects/。

        提交成功后调用时，已被 os.replace 移走的文件会安静跳过，
        仅删除因去重而未落位的副本。
        """
        for _digest, _size, path in staged:
            self._remove_quietly(path)

    @staticmethod
    def _remove_quietly(path: str) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> "BlobVault":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
