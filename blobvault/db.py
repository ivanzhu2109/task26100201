"""SQLite 元数据持久化层。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from .errors import AliasConflictError

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
    batch_id    TEXT PRIMARY KEY,
    result_json TEXT NOT NULL
);
"""


class MetadataStore:
    """管理对象、别名与批次状态的 SQLite 存储。"""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(str(db_path))
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def get_batch_result(self, batch_id: str) -> str | None:
        """返回已提交批次的序列化结果，未提交返回 None。"""
        row = self._conn.execute(
            "SELECT result_json FROM batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        return row[0] if row else None

    def resolve_alias(self, alias: str) -> str | None:
        """返回别名指向的摘要，不存在返回 None。"""
        row = self._conn.execute(
            "SELECT digest FROM aliases WHERE alias = ?",
            (alias,),
        ).fetchone()
        return row[0] if row else None

    def has_object(self, digest: str) -> bool:
        """判断摘要对应的对象是否已提交。"""
        row = self._conn.execute(
            "SELECT 1 FROM objects WHERE digest = ?",
            (digest,),
        ).fetchone()
        return row is not None

    def commit_batch(
        self,
        batch_id: str,
        items: list[tuple[str, int, str | None]],
        result_json: str,
    ) -> None:
        """在单个事务中写入对象、别名与批次记录。

        任一步失败（含别名冲突）整体回滚，不留下部分提交。
        """
        try:
            with self._conn:
                self._check_alias_conflicts(items)
                self._conn.executemany(
                    "INSERT OR IGNORE INTO objects (digest, size) VALUES (?, ?)",
                    [(digest, size) for digest, size, _ in items],
                )
                self._conn.executemany(
                    "INSERT OR IGNORE INTO aliases (alias, digest) VALUES (?, ?)",
                    [
                        (alias, digest)
                        for digest, _, alias in items
                        if alias is not None
                    ],
                )
                self._conn.execute(
                    "INSERT INTO batches (batch_id, result_json) VALUES (?, ?)",
                    (batch_id, result_json),
                )
        except sqlite3.IntegrityError as exc:
            raise AliasConflictError(alias="<unknown>") from exc

    def _check_alias_conflicts(
        self, items: list[tuple[str, int, str | None]]
    ) -> None:
        """同事务内校验：已有别名指向不同摘要则整批失败。"""
        for digest, _, alias in items:
            if alias is None:
                continue
            row = self._conn.execute(
                "SELECT digest FROM aliases WHERE alias = ?",
                (alias,),
            ).fetchone()
            if row is not None and row[0] != digest:
                raise AliasConflictError(alias, existing_digest=row[0])
