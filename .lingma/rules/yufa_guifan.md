---
trigger: always_on
---
## Python 导入规范

### 1. 集中导入原则
- **禁止局部导入**：严禁在函数内部、类定义内部或条件语句块中使用 `import` 或 `from ... import ...` 语句。
- **文件顶部导入**：所有模块依赖必须集中在文件的最顶部进行声明，确保依赖关系清晰可见，便于维护和静态分析。

### 2. 导入顺序建议
为了提高代码可读性，建议按照以下顺序排列导入语句，每组之间空一行：
1. **标准库导入**（如 `os`, `sys`, `json`）
2. **第三方库导入**（如 `pandas`, `numpy`, `requests`）
3. **本地应用/库特定导入**（如 `from backend.utils import date_utils`）

### 3. 示例
# ✅ 正确：集中在文件顶部
import os
import sys
import pandas as pd
from backend.utils import crypto_utils

def process_data():
    # 业务逻辑代码
    pass

# ❌ 错误：在函数内部导入
def bad_example():
    import numpy as np  # 禁止这样做
    return np.array([1, 2, 3])
