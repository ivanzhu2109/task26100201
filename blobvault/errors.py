"""BlobVault 异常类型定义。"""


class BlobVaultError(Exception):
    """组件内所有异常的基类。"""


class AliasConflictError(BlobVaultError):
    """批次内别名重复，或别名已指向其他摘要。"""

    def __init__(self, alias: str, existing_digest: str | None = None) -> None:
        self.alias = alias
        self.existing_digest = existing_digest
        if existing_digest is None:
            message = f"alias {alias!r} is duplicated within the batch"
        else:
            message = (
                f"alias {alias!r} already points to digest {existing_digest!r}"
            )
        super().__init__(message)


class NotFoundError(BlobVaultError):
    """请求的摘要或别名不存在（或未提交）。"""


class BatchItemsRequiredError(BlobVaultError):
    """批次尚未提交，但调用方未提供数据流。"""
