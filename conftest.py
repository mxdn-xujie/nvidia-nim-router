"""pytest 根配置：确保项目根目录可导入。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))