"""BlobVault 异常类型定义。"""


class BlobVaultError(Exception):
    """组件内所有异常的基类。"""


class AliasConflictError(BlobVaultError):
    """批次内别名重复，或别名已指向不同摘要。"""


class BatchAlreadyCommittedError(BlobVaultError):
    """批次已提交，但调用方又提供了与已提交结果不一致的新数据。"""


class ObjectNotFoundError(BlobVaultError):
    """按摘要或别名读取时，对象不存在或未提交。"""
