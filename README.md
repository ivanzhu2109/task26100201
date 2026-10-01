# task26100201

## BlobVault：批次化内容寻址存储库

纯 Python 标准库实现（无第三方依赖、无服务进程）。原始内容保存在本地文件，
摘要、别名与批次状态持久化到 SQLite。

### 特性

- **批次原子提交**：一批中的对象与别名整体生效；输入流读取失败或别名冲突时
  不留下任何部分提交，历史已提交对象不受影响。
- **内容寻址去重**：以完整内容的 SHA-256 作为对象标识，同批及跨批去重，
  相同内容只保留一份文件。
- **别名约束**：同批内别名互不重复；已有别名指向相同摘要视为兼容，
  指向不同摘要则整批失败。不提供别名重绑与对象删除。
- **幂等重试**：已提交批次只凭 `batch_id` 即可取回原成功结果，无需重传数据，
  且不产生新提交；未提交批次可用原批次号重新提供数据写入。
- **读取隔离**：只能读取已提交内容，暂存中的数据对外不可见。

### 用法

```python
import io
from blobvault import BlobVault, BatchItem

vault = BlobVault("/path/to/vault")

# 批次写入（流只需支持顺序 read，不要求可回退）
result = vault.commit_batch("release-2026-10", [
    BatchItem(open("weights.bin", "rb"), alias="model/latest"),
    BatchItem(io.BytesIO(b"config"), alias="model/config"),
])
for entry in result.entries:
    print(entry.digest, entry.size, entry.alias, entry.deduplicated)

# 已提交批次重试：只传批次号，返回原结果
again = vault.commit_batch("release-2026-10")
assert again == result

# 流式读回（仅已提交内容）
with vault.open_by_alias("model/latest") as f:
    while chunk := f.read(1 << 20):
        ...
with vault.open_by_digest(result.entries[0].digest) as f:
    ...

vault.close()
```

### 存储布局

```
<root>/
├── vault.db            # SQLite：objects / aliases / batches
├── objects/xx/<digest> # 已提交的内容文件（按摘要前两位分目录）
└── staging/            # 批次写入期的临时文件，提交或失败后清空
```

### 运行测试

```bash
python3 -m unittest discover -s tests -v
```
