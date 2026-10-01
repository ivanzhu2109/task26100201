"""BlobVault 批次入库组件的自动化测试。

运行：python3 -m unittest discover -s tests -v
"""
import hashlib
import io
import os
import tempfile
import unittest

from blobvault import (
    AliasConflictError,
    BatchAlreadyCommittedError,
    BatchItem,
    BlobVault,
    BlobVaultError,
    ObjectNotFoundError,
)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class FailingStream:
    """顺序读取若干字节后抛错的流，模拟客户端超时/断连。"""

    def __init__(self, data: bytes, fail_after: int):
        self._buf = io.BytesIO(data)
        self._remaining = fail_after

    def read(self, n: int = -1) -> bytes:
        if self._remaining <= 0:
            raise ConnectionError("模拟读取失败：客户端超时")
        chunk = self._buf.read(min(n, self._remaining) if n > 0 else self._remaining)
        self._remaining -= len(chunk)
        return chunk


class BlobVaultTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = self._tmp.name
        self.vault = BlobVault(self.root)
        self.addCleanup(self.vault.close)

    def object_files(self) -> list[str]:
        base = os.path.join(self.root, "objects")
        return [
            os.path.join(dirpath, name)
            for dirpath, _dirs, files in os.walk(base)
            for name in files
        ]

    def staging_files(self) -> list[str]:
        base = os.path.join(self.root, "staging")
        return os.listdir(base)

    def committed_digests(self) -> set[str]:
        return {
            row[0]
            for row in self.vault._db.execute("SELECT digest FROM objects")
        }

    def committed_aliases(self) -> dict[str, str]:
        return dict(self.vault._db.execute("SELECT alias, digest FROM aliases"))

    def committed_batches(self) -> list[str]:
        return [
            row[0]
            for row in self.vault._db.execute("SELECT batch_id FROM batches")
        ]


class TestBasicCommitAndRead(BlobVaultTestCase):
    def test_commit_then_read_by_digest_and_alias(self) -> None:
        data = b"model-weights" * 4096
        result = self.vault.commit_batch(
            "b1", [BatchItem(io.BytesIO(data), alias="release/latest")]
        )
        self.assertEqual(result.entries[0].digest, sha256(data))
        self.assertEqual(result.entries[0].size, len(data))
        self.assertFalse(result.entries[0].deduplicated)
        with self.vault.open_by_digest(sha256(data)) as f:
            self.assertEqual(f.read(), data)
        with self.vault.open_by_alias("release/latest") as f:
            self.assertEqual(f.read(), data)
        self.assertEqual(self.vault.resolve_alias("release/latest"), sha256(data))

    def test_uncommitted_content_is_invisible(self) -> None:
        self.assertFalse(self.vault.has_object(sha256(b"ghost")))
        with self.assertRaises(ObjectNotFoundError):
            self.vault.open_by_digest(sha256(b"ghost"))
        with self.assertRaises(ObjectNotFoundError):
            self.vault.open_by_alias("no/such/alias")

    def test_dedup_within_and_across_batches(self) -> None:
        data = b"shared-content" * 1000
        r1 = self.vault.commit_batch(
            "b1",
            [
                BatchItem(io.BytesIO(data), alias="a/one"),
                BatchItem(io.BytesIO(data), alias="a/two"),
            ],
        )
        self.assertFalse(r1.entries[0].deduplicated)
        self.assertTrue(r1.entries[1].deduplicated)
        r2 = self.vault.commit_batch("b2", [BatchItem(io.BytesIO(data))])
        self.assertTrue(r2.entries[0].deduplicated)
        self.assertEqual(len(self.object_files()), 1, "相同内容只保留一份文件")

    def test_alias_compatible_when_pointing_to_same_digest(self) -> None:
        data = b"same-bytes"
        self.vault.commit_batch("b1", [BatchItem(io.BytesIO(data), alias="model/v1")])
        result = self.vault.commit_batch(
            "b2", [BatchItem(io.BytesIO(data), alias="model/v1")]
        )
        self.assertEqual(result.entries[0].digest, sha256(data))
        self.assertEqual(self.committed_aliases(), {"model/v1": sha256(data)})


class TestAtomicFailure(BlobVaultTestCase):
    def test_last_item_read_failure_rolls_back_whole_batch(self) -> None:
        history = b"historical-model" * 2048
        self.vault.commit_batch(
            "b0", [BatchItem(io.BytesIO(history), alias="model/stable")]
        )
        before_objects = self.object_files()

        new_data = b"new-model" * 2048
        with self.assertRaises(ConnectionError):
            self.vault.commit_batch(
                "b1",
                [
                    BatchItem(io.BytesIO(history), alias="model/copy"),
                    BatchItem(io.BytesIO(new_data), alias="model/new"),
                    BatchItem(FailingStream(b"broken" * 100, fail_after=10)),
                ],
            )

        self.assertEqual(self.committed_digests(), {sha256(history)})
        self.assertEqual(self.committed_aliases(), {"model/stable": sha256(history)})
        self.assertEqual(self.committed_batches(), ["b0"])
        self.assertEqual(self.object_files(), before_objects, "历史文件无损")
        self.assertEqual(self.staging_files(), [], "暂存文件已清理")
        with self.vault.open_by_alias("model/stable") as f:
            self.assertEqual(f.read(), history)
        with self.assertRaises(ObjectNotFoundError):
            self.vault.open_by_digest(sha256(new_data))

    def test_alias_conflict_rolls_back_whole_batch(self) -> None:
        history = b"historical-model" * 2048
        self.vault.commit_batch(
            "b0", [BatchItem(io.BytesIO(history), alias="model/stable")]
        )
        before_objects = self.object_files()

        other = b"different-content" * 1024
        with self.assertRaises(AliasConflictError):
            self.vault.commit_batch(
                "b1",
                [
                    BatchItem(io.BytesIO(history), alias="model/copy"),
                    BatchItem(io.BytesIO(other), alias="model/stable"),
                ],
            )

        self.assertEqual(self.committed_digests(), {sha256(history)})
        self.assertEqual(self.committed_aliases(), {"model/stable": sha256(history)})
        self.assertEqual(self.committed_batches(), ["b0"])
        self.assertEqual(self.object_files(), before_objects)
        self.assertEqual(self.staging_files(), [])
        with self.vault.open_by_alias("model/stable") as f:
            self.assertEqual(f.read(), history)

    def test_intra_batch_duplicate_alias_rejected(self) -> None:
        with self.assertRaises(AliasConflictError):
            self.vault.commit_batch(
                "b1",
                [
                    BatchItem(io.BytesIO(b"x"), alias="dup"),
                    BatchItem(io.BytesIO(b"y"), alias="dup"),
                ],
            )
        self.assertEqual(self.committed_batches(), [])

    def test_failed_batch_id_can_be_retried_with_data(self) -> None:
        with self.assertRaises(ConnectionError):
            self.vault.commit_batch(
                "b1", [BatchItem(FailingStream(b"abc", fail_after=1))]
            )
        data = b"recovered-payload"
        result = self.vault.commit_batch(
            "b1", [BatchItem(io.BytesIO(data), alias="model/v2")]
        )
        self.assertEqual(result.entries[0].digest, sha256(data))
        with self.vault.open_by_alias("model/v2") as f:
            self.assertEqual(f.read(), data)


class TestIdempotentRetry(BlobVaultTestCase):
    def test_committed_batch_retry_by_id_only(self) -> None:
        data1 = b"weights-a" * 1024
        data2 = b"weights-b" * 1024
        original = self.vault.commit_batch(
            "b1",
            [
                BatchItem(io.BytesIO(data1), alias="release/a"),
                BatchItem(io.BytesIO(data2), alias="release/b"),
            ],
        )
        before_objects = self.object_files()
        before_batches = self.committed_batches()

        replay = self.vault.commit_batch("b1")
        self.assertEqual(replay, original, "只凭批次号取回原成功结果")
        self.assertEqual(self.object_files(), before_objects, "不产生新对象")
        self.assertEqual(self.committed_batches(), before_batches, "不产生新提交")

    def test_retry_result_survives_reopen(self) -> None:
        data = b"persistent" * 512
        original = self.vault.commit_batch(
            "b1", [BatchItem(io.BytesIO(data), alias="model/v1")]
        )
        self.vault.close()
        self.vault = BlobVault(self.root)
        replay = self.vault.commit_batch("b1")
        self.assertEqual(replay, original)
        with self.vault.open_by_alias("model/v1") as f:
            self.assertEqual(f.read(), data)

    def test_committed_batch_rejects_new_data(self) -> None:
        self.vault.commit_batch("b1", [BatchItem(io.BytesIO(b"a"))])
        with self.assertRaises(BatchAlreadyCommittedError):
            self.vault.commit_batch("b1", [BatchItem(io.BytesIO(b"a"))])

    def test_uncommitted_batch_id_without_data_rejected(self) -> None:
        with self.assertRaises(BlobVaultError):
            self.vault.commit_batch("never-seen")


if __name__ == "__main__":
    unittest.main()
