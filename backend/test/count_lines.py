import os
import fnmatch

# 需要忽略的文件/目录模式
IGNORE_PATTERNS = [
    # 敏感配置与环境文件
    '.env',
    '.env.*',
    '*.key',
    '*.pem',

    # 日志与临时文件
    '*.log',
    '*.log.*',
    'server.log',
    '*.tmp',
    '*.temp',
    '*.swp',
    '*.swo',

    # Python 编译与缓存文件
    '__pycache__',
    '*.pyc',
    '*.pyo',
    '*.pyd',
    '*.pyc.*',
    '.pytest_cache',
    '.coverage',

    # 数据文件与数据库
    '*.db',
    '*.sqlite',
    '*.sqlite3',
    '*.csv',
    '*.xlsx',
    '*.xls',
    '*.json.tmp',
    '*.parquet',

    # 前端与项目构建产物
    'node_modules',
    'dist',
    'build',
    '.nuxt',
    '.cache',
    '.vitepress/dist',

    # IDE 与系统文件
    '.vscode',
    '.idea',
    '.DS_Store',
    'Thumbs.db',

    # 测试与非核心文档
    'docs',
    'README.md',

    # 规则目录
    '.trae',
    '.lingma'
]

# 需要统计的文件类型
INCLUDE_PATTERNS = {
    'backend': ['*.py'],
    'data-sync-service': ['*.py'],
    'dashboard/src': ['*.vue', '*.js', '*.ts'],
    'root': ['*.py']
}

def is_ignored(path):
    """检查路径是否应该被忽略"""
    for pattern in IGNORE_PATTERNS:
        # 检查目录名
        if fnmatch.fnmatch(os.path.basename(path), pattern):
            return True
        # 检查完整路径
        if fnmatch.fnmatch(path, f'**/{pattern}'):
            return True
    return False

def count_lines_in_file(filepath):
    """统计单个文件的行数"""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            return len(lines)
    except Exception as e:
        print(f"无法读取文件: {filepath} - {e}")
        return 0

def count_lines_in_directory(base_dir, file_patterns):
    """统计目录下符合模式的文件行数"""
    total_lines = 0
    file_count = 0
    file_details = []

    for root, dirs, files in os.walk(base_dir):
        # 过滤掉忽略的目录
        dirs[:] = [d for d in dirs if not is_ignored(os.path.join(root, d))]

        for filename in files:
            filepath = os.path.join(root, filename)

            # 检查文件是否被忽略
            if is_ignored(filepath):
                continue

            # 检查文件是否符合包含模式
            matched = False
            for pattern in file_patterns:
                if fnmatch.fnmatch(filename, pattern):
                    matched = True
                    break

            if matched:
                lines = count_lines_in_file(filepath)
                total_lines += lines
                file_count += 1
                file_details.append((filepath, lines))

    return total_lines, file_count, file_details

def main():
    print("=" * 80)
    print("项目代码行数统计工具")
    print("=" * 80)

    total_total = 0
    total_files = 0
    all_details = []

    # 统计 backend 目录
    backend_dir = os.path.join(os.path.dirname(__file__), '..')
    if os.path.exists(backend_dir):
        print("\n【后端代码】- backend/")
        print("-" * 60)
        lines, files, details = count_lines_in_directory(backend_dir, INCLUDE_PATTERNS['backend'])
        print(f"文件数: {files}")
        print(f"代码行数: {lines:,}")
        total_total += lines
        total_files += files
        all_details.extend([('backend/' + os.path.relpath(f, backend_dir), l) for f, l in details])

    # 统计 data-sync-service 目录
    sync_service_dir = os.path.join(os.path.dirname(__file__), '../../data-sync-service')
    if os.path.exists(sync_service_dir):
        print("\n【数据同步服务】- data-sync-service/")
        print("-" * 60)
        lines, files, details = count_lines_in_directory(sync_service_dir, INCLUDE_PATTERNS['data-sync-service'])
        print(f"文件数: {files}")
        print(f"代码行数: {lines:,}")
        total_total += lines
        total_files += files
        all_details.extend([('data-sync-service/' + os.path.relpath(f, sync_service_dir), l) for f, l in details])

    # 统计 dashboard/src 目录
    dashboard_dir = os.path.join(os.path.dirname(__file__), '../../dashboard/src')
    if os.path.exists(dashboard_dir):
        print("\n【前端代码】- dashboard/src/")
        print("-" * 60)
        lines, files, details = count_lines_in_directory(dashboard_dir, INCLUDE_PATTERNS['dashboard/src'])
        print(f"文件数: {files}")
        print(f"代码行数: {lines:,}")
        total_total += lines
        total_files += files
        all_details.extend([('dashboard/src/' + os.path.relpath(f, dashboard_dir), l) for f, l in details])

    # 统计项目根目录下的核心脚本
    root_dir = os.path.join(os.path.dirname(__file__), '../..')
    print("\n【根目录核心脚本】")
    print("-" * 60)
    root_lines = 0
    root_files = 0
    for filename in os.listdir(root_dir):
        filepath = os.path.join(root_dir, filename)
        if os.path.isfile(filepath) and not is_ignored(filepath):
            matched = False
            for pattern in INCLUDE_PATTERNS['root']:
                if fnmatch.fnmatch(filename, pattern):
                    matched = True
                    break

            if matched:
                lines = count_lines_in_file(filepath)
                root_lines += lines
                root_files += 1
                all_details.append((filename, lines))

    print(f"文件数: {root_files}")
    print(f"代码行数: {root_lines:,}")
    total_total += root_lines
    total_files += root_files

    # 输出总计
    print("\n" + "=" * 80)
    print("【总计】")
    print("-" * 60)
    print(f"总文件数: {total_files}")
    print(f"总代码行数: {total_total:,}")
    print("=" * 80)

    # 输出文件详情（按行数排序）
    print("\n【文件详情】（按行数降序排列）")
    print("-" * 80)
    all_details.sort(key=lambda x: x[1], reverse=True)
    for filepath, lines in all_details:
        print(f"{lines:>6} 行 - {filepath}")

if __name__ == "__main__":
    main()
