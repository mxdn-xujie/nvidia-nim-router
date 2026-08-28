"""CSV 解析导入模块（§5 容错重点）。

解析规则（按文档 §5.2 逐条实现）：
1. 编码支持 UTF-8 / UTF-8-BOM；
2. 空行直接跳过，不计入任何计数；
3. 表头按内容自动识别：存在 nvapi- 开头字段的一律按数据行处理；
   无 nvapi- 字段且包含表头特征词（email/password/apikey 等）的行判定为表头并跳过；
4. 字段逐个 strip，兼容「逗号+空格」脏数据；
5. 列序容错：自动定位 apikey（nvapi- 开头）/ email（含 @）/ password（剩余字段）；
6. 单列容错：整行仅一个 nvapi- 字段时 email/password 存 NULL；
7. 非法行（无 nvapi- 且非表头）计入 invalid 并记录原文件行号；
8. 去重：库内重复与文件内重复均计入 duplicates，并返回脱敏 Key 明细。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # 仅类型提示用，避免运行时循环依赖
    from .database import Database

# 表头特征词（小写匹配，包含式判断）
HEADER_KEYWORDS = ("email", "password", "apikey", "api_key", "api key")


@dataclass
class ParsedRow:
    """一条合法数据行。"""

    email: str | None
    password: str | None
    apikey: str


@dataclass
class ParsedCsv:
    """CSV 文本解析结果。"""

    rows: list[ParsedRow] = field(default_factory=list)
    invalid_lines: list[int] = field(default_factory=list)  # 原文件行号（从 1 开始）
    header_detected: bool = False
    total_in_file: int = 0  # 非空行总数（含表头行）


def decode_csv_bytes(data: bytes) -> str:
    """按 UTF-8 / UTF-8-BOM 解码，其他编码直接报错。"""
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("CSV 文件编码必须是 UTF-8 或 UTF-8-BOM") from exc


def _looks_like_header(fields: list[str]) -> bool:
    """表头判定：任一字段包含表头特征词。"""
    for raw in fields:
        f = raw.lower()
        if any(k in f for k in HEADER_KEYWORDS):
            return True
    return False


def parse_csv_text(text: str) -> ParsedCsv:
    """解析 CSV 文本（已按 utf-8-sig 解码），返回结构化结果。"""
    # 兜底去掉可能残留的 BOM 字符（前端粘贴文本场景）
    if text.startswith("\ufeff"):
        text = text[1:]

    result = ParsedCsv()
    for line_no, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue  # 规则 2：空行跳过，不计入任何计数
        result.total_in_file += 1

        fields = [f.strip() for f in line.split(",")]
        non_empty = [f for f in fields if f]

        has_apikey_field = any(f.startswith("nvapi-") for f in non_empty)
        if not has_apikey_field:
            # 规则 3：无 nvapi- 字段 → 表头或非法行
            if _looks_like_header(non_empty):
                result.header_detected = True
                continue
            result.invalid_lines.append(line_no)  # 规则 7
            continue

        # 数据行：规则 5 列序容错定位
        apikey = next(f for f in non_empty if f.startswith("nvapi-"))
        rest = [f for f in non_empty if f != apikey]
        email: str | None = None
        password: str | None = None
        for f in rest:
            if email is None and "@" in f:
                email = f
            elif password is None:
                password = f
        # 规则 6：单列时 email/password 保持 None
        result.rows.append(ParsedRow(email=email, password=password, apikey=apikey))
    return result


def _mask_key(apikey: str) -> str:
    """重复 Key 的脱敏展示（与上游管理页掩码规则一致）。"""
    if apikey.startswith("nvapi-"):
        return f"nvapi-****{apikey[-4:]}"
    return f"****{apikey[-4:]}"


def import_csv(db: Database, content: str) -> dict:
    """解析并导入到数据库，返回文档 §5.2 规定的结果结构（含重复明细）。"""
    parsed = parse_csv_text(content)
    added = 0
    duplicates: list[str] = []  # 脱敏后的重复 Key
    seen_in_file: set[str] = set()
    for row in parsed.rows:
        if row.apikey in seen_in_file:
            # 文件内重复：同一 Key 在本次导入内容中出现多次
            duplicates.append(_mask_key(row.apikey))
            continue
        seen_in_file.add(row.apikey)
        if db.add_upstream(row.email, row.password, row.apikey):
            added += 1
        else:
            # 库内重复：Key 已存在于上游池
            duplicates.append(_mask_key(row.apikey))
    return {
        "added": added,
        "duplicates": len(duplicates),
        "duplicate_keys": duplicates[:20],  # 最多展示 20 个，避免响应过大
        "invalid": len(parsed.invalid_lines),
        "invalid_lines": parsed.invalid_lines,
        "total_in_file": parsed.total_in_file,
        "header_detected": parsed.header_detected,
    }
