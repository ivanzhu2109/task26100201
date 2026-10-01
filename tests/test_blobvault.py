"""BlobVault 批次入库组件的自动化测试。"""

from __future__ import annotations

import hashlib
import io
import sqlite3
import tempfile
import unittest
from pathlib import Path

from blobvault import (
    AliasConflictError,
    BatchItem,
    BatchItemsRequiredError,
    BlobVault,
    NotFoundError,
)


def digest_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class FailingStream(io.RawIOBase):
    """读到中途抛错的顺序流，模拟客户端上传中断。"""

    def __init__(self, prefix: bytes, fail_after: int) -> None:
        self._prefix = prefix
        self._fail_after = fail_after
        self._read = 0

    def read(self, n: int = -1) -> bytes:
        if self._read >= self._fail_after:
            raise IOError("simulated upload failure")
        if not self._prefix:
            self._read = self._fail_after
            return b""
        chunk, self._prefix = self._prefix[:n], self._prefix[n:]
        self._read += len(chunk)
        return chunk


class BlobVaultTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.vault = BlobVault(self.root)
        self.addCleanup(self.vault.close)

    def object_files(self) -> list[str]:
        return sorted(p.name for p in (self.root / "objects").iterdir())

    def staging_files(self) -> list[str]:
        return sorted(p.name for p in (self.root / "staging").iterdir())

    def table_counts(self) -> tuple[int, int, int]:
        conn = sqlite3.connect(str(self.root / "meta.sqlite"))
        try:
            objects = conn.execute("SELECT COUNT(*) FROM objects").fetchone()[0]
            aliases = conn.execute("SELECT COUNT(*) FROM aliases").fetchone()[0]
            batches = conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
            return objects, aliases, batches
        finally:
            conn.close()


class TestCommitAndRead(BlobVaultTestCase):
    def test_commit_then_read_by_digest_and_alias(self) -> None:
        result = self.vault.commit_batch(
            "b1",
            [
                BatchItem(io.BytesIO(b"model-weights"), "release/weights"),
                BatchItem(io.BytesIO(b"tokenizer"), "release/tokenizer"),
                BatchItem(io.BytesIO(b"readme")),
            ],
        )
        self.assertEqual(result.batch_id, "b1")
        self.assertEqual(len(result.items), 3)
        self.assertEqual(
            result.items[0].digest, digest_of(b"model-weights")
        )
        with self.vault.open_by_digest(digest_of(b"model-weights")) as f:
            self.assertEqual(f.read(), b"model-weights")
        with self.vault.open_by_alias("release/tokenizer") as f:
            self.assertEqual(f.read(), b"tokenizer")

    def test_read_rejects_uncommitted(self) -> None:
        with self.assertRaises(NotFoundError):
            self.vault.open_by_digest(digest_of(b"ghost"))
        with self.assertRaises(NotFoundError):
            self.vault.open_by_alias("no/such/alias")

    def test_dedup_within_and_across_batches(self) -> None:
        self.vault.commit_batch(
            "b1",
            [
                BatchItem(io.BytesIO(b"same"), "a1"),
                BatchItem(io.BytesIO(b"same"), "a2"),
            ],
        )
        self.vault.commit_batch("b2", [BatchItem(io.BytesIO(b"same"), "a3")])
        self.assertEqual(self.object_files(), [digest_of(b"same")])
        for alias in ("a1", "a2", "a3"):
            with self.vault.open_by_alias(alias) as f:
                self.assertEqual(f.read(), b"same")

    def test_uncommitted_batch_requires_items(self) -> None:
        with self.assertRaises(BatchItemsRequiredError):
            self.vault.commit_batch("never-seen")

    def test_alias_conflict_with_different_digest_fails_batch(self) -> None:
        self.vault.commit_batch("b1", [BatchItem(io.BytesIO(b"old"), "tag")])
        with self.assertRaises(AliasConflictError):
            self.vault.commit_batch(
                "b2", [BatchItem(io.BytesIO(b"new"), "tag")]
            )
        self.assertEqual(self.table_counts(), (1, 1, 1))

    def test_alias_compatible_when_same_digest(self) -> None:
        self.vault.commit_batch("b1", [BatchItem(io.BytesIO(b"x"), "tag")])
        result = self.vault.commit_batch(
            "b2", [BatchItem(io.BytesIO(b"x"), "tag")]
        )
        self.assertEqual(result.items[0].digest, digest_of(b"x"))
        self.assertEqual(self.table_counts(), (1, 1, 2))

    def test_duplicate_alias_within_batch_fails(self) -> None:
        with self.assertRaises(AliasConflictError):
            self.vault.commit_batch(
                "b1",
                [
                    BatchItem(io.BytesIO(b"one"), "dup"),
                    BatchItem(io.BytesIO(b"two"), "dup"),
                ],
            )
        self.assertEqual(self.table_counts(), (0, 0, 0))
        self.assertEqual(self.object_files(), [])
        self.assertEqual(self.staging_files(), [])


class TestFailureAtomicity(BlobVaultTestCase):
    def test_read_failure_on_last_item_rolls_back_whole_batch(self) -> None:
        historical = b"historical-weights"
        self.vault.commit_batch(
            "b1", [BatchItem(io.BytesIO(historical), "release/weights")]
        )
        before_counts = self.table_counts()
        before_files = self.object_files()

        with self.assertRaises(IOError):
            self.vault.commit_batch(
                "b2",
                [
                    BatchItem(io.BytesIO(historical), "release/weights-v2"),
                    BatchItem(io.BytesIO(b"brand-new-blob"), "release/extra"),
                    BatchItem(FailingStream(b"partial", fail_after=3), "bad"),
                ],
            )

        self.assertEqual(self.table_counts(), before_counts)
        self.assertEqual(self.object_files(), before_files)
        self.assertEqual(self.staging_files(), [])
        with self.vault.open_by_alias("release/weights") as f:
            self.assertEqual(f.read(), historical)
        with self.assertRaises(NotFoundError):
            self.vault.open_by_alias("release/extra")
        with self.assertRaises(NotFoundError):
            self.vault.open_by_digest(digest_of(b"brand-new-blob"))

    def test_alias_conflict_rolls_back_whole_batch(self) -> None:
        historical = b"historical-weights"
        self.vault.commit_batch(
            "b1", [BatchItem(io.BytesIO(historical), "release/weights")]
        )
        before_counts = self.table_counts()
        before_files = self.object_files()

        with self.assertRaises(AliasConflictError):
            self.vault.commit_batch(
                "b2",
                [
                    BatchItem(io.BytesIO(historical), "release/weights"),
                    BatchItem(io.BytesIO(b"brand-new-blob"), "release/extra"),
                    BatchItem(io.BytesIO(b"hijack"), "release/weights"),
                ],
            )

        self.assertEqual(self.table_counts(), before_counts)
        self.assertEqual(self.object_files(), before_files)
        self.assertEqual(self.staging_files(), [])
        with self.vault.open_by_alias("release/weights") as f:
            self.assertEqual(f.read(), historical)
        with self.assertRaises(NotFoundError):
            self.vault.open_by_alias("release/extra")
        with self.assertRaises(NotFoundError):
            self.vault.open_by_digest(digest_of(b"brand-new-blob"))

    def test_failed_batch_id_can_be_retried_with_data(self) -> None:
        with self.assertRaises(IOError):
            self.vault.commit_batch(
                "b1", [BatchItem(FailingStream(b"x", fail_after=0))]
            )
        result = self.vault.commit_batch(
            "b1", [BatchItem(io.BytesIO(b"recovered"), "ok")]
        )
        self.assertEqual(result.items[0].digest, digest_of(b"recovered"))
        with self.vault.open_by_alias("ok") as f:
            self.assertEqual(f.read(), b"recovered")


class TestCommittedBatchRetry(BlobVaultTestCase):
    def test_retry_with_only_batch_id_returns_original_result(self) -> None:
        original = self.vault.commit_batch(
            "b1",
            [
                BatchItem(io.BytesIO(b"weights"), "release/weights"),
                BatchItem(io.BytesIO(b"config"), "release/config"),
            ],
        )
        before_counts = self.table_counts()
        before_files = self.object_files()

        replayed = self.vault.commit_batch("b1")

        self.assertEqual(replayed, original)
        self.assertEqual(self.table_counts(), before_counts)
        self.assertEqual(self.object_files(), before_files)
        self.assertEqual(self.staging_files(), [])

    def test_retry_does_not_touch_streams(self) -> None:
        self.vault.commit_batch("b1", [BatchItem(io.BytesIO(b"data"), "a")])

        class ExplodingStream(io.RawIOBase):
            def read(self, n: int = -1) -> bytes:
                raise AssertionError("stream must not be read on replay")

        result = self.vault.commit_batch(
            "b1", [BatchItem(ExplodingStream(), "a")]
        )
        self.assertEqual(result.items[0].digest, digest_of(b"data"))


if __name__ == "__main__":
    unittest.main()
