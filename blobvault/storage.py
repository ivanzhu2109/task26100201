"""内容寻址的本地文件存储。"""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path
from typing import BinaryIO

_CHUNK_SIZE = 1024 * 1024


class FileStore:
    """以 SHA-256 摘要为文件名的对象文件存储。"""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._objects_dir = root / "objects"
        self._staging_dir = root / "staging"
        self._objects_dir.mkdir(parents=True, exist_ok=True)
        self._staging_dir.mkdir(parents=True, exist_ok=True)

    def stage(self, stream: BinaryIO) -> tuple[Path, str, int]:
        """把顺序流写入临时文件，返回 (临时路径, 摘要, 字节数)。"""
        staged_path = self._staging_dir / f".tmp-{uuid.uuid4().hex}"
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(staged_path, "wb") as out:
                while True:
                    chunk = stream.read(_CHUNK_SIZE)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    out.write(chunk)
                    size += len(chunk)
        except BaseException:
            self.discard(staged_path)
            raise
        return staged_path, hasher.hexdigest(), size

    def place(self, staged_path: Path, digest: str) -> bool:
        """把临时文件就位为正式对象；已存在则丢弃临时文件。返回是否新放置。"""
        target = self.object_path(digest)
        if target.exists():
            self.discard(staged_path)
            return False
        os.replace(staged_path, target)
        return True

    def discard(self, staged_path: Path) -> None:
        """清理临时文件。"""
        try:
            os.unlink(staged_path)
        except FileNotFoundError:
            pass

    def remove_object(self, digest: str) -> None:
        """删除对象文件（仅用于失败回滚自己新放置的文件）。"""
        try:
            os.unlink(self.object_path(digest))
        except FileNotFoundError:
            pass

    def object_path(self, digest: str) -> Path:
        return self._objects_dir / digest

    def open(self, digest: str) -> BinaryIO:
        return open(self.object_path(digest), "rb")
