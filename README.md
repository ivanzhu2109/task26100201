# task26100201

## BlobVault：批次入库的对象存储组件

以库的形式供上层调用。原始内容保存在本地文件（按 SHA-256 内容寻址），
摘要、别名与批次状态持久化到 SQLite。仅依赖 Python 标准库（3.10+）。

### 能力

- `commit_batch(batch_id, items)`：批次写入。每批多个顺序二进制流，每项可附别名。
  整批原子生效：输入流读取失败或别名冲突时不留下任何部分提交，历史对象无损。
- 已提交批次重试只需 `commit_batch(batch_id)`，不重传数据即返回原成功结果，
  且不产生新提交；未提交批次可用原批次号重新提供数据写入。
- 同批及跨批按内容去重，相同内容只保留一份文件。
- 别名在批内互不重复；已有别名指向相同摘要视为兼容，指向不同摘要整批失败。
  不提供别名重绑与对象删除。
- `open_by_digest(digest)` / `open_by_alias(alias)`：流式读回，仅已提交内容可见。

### 目录结构

```
blobvault/
  vault.py     # BlobVault 主入口（编排、批次原子性）
  storage.py   # FileStore：内容寻址文件存储（暂存 + 就位）
  db.py        # MetadataStore：SQLite 元数据（对象/别名/批次）
  models.py    # BatchItem / BatchResult / ItemResult
  errors.py    # 异常类型
tests/
  test_blobvault.py
```

### 用法示例

```python
from blobvault import BlobVault, BatchItem

with BlobVault("/data/vault") as vault:
    result = vault.commit_batch("release-2026-10", [
        BatchItem(open("weights.bin", "rb"), alias="release/weights"),
        BatchItem(open("config.json", "rb"), alias="release/config"),
    ])
    # 客户端超时重试：只传批次号即可取回原结果
    again = vault.commit_batch("release-2026-10")
    assert again == result

    with vault.open_by_alias("release/weights") as f:
        chunk = f.read(1 << 20)
```

### 运行测试

```
python3 -m unittest discover -s tests -v
```
