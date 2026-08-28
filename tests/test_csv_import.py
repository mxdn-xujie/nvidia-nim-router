"""csv_import 单元测试（§5.3 验收标准）。"""
import pytest

from app.csv_import import decode_csv_bytes, import_csv, parse_csv_text
from app.database import Database


@pytest.fixture
def db(tmp_path):
    return Database(str(tmp_path / "test.db"))


def make_csv(with_header: bool, rows: int = 37, blank_lines: bool = True) -> str:
    """构造与实际 accounts.csv 同构的文件内容（带/不带表头）。"""
    lines = []
    if with_header:
        lines.append("email,password,apikey")
    for i in range(rows):
        lines.append(f"user{i}@example.com,pass{i:03d},nvapi-key{i:03d}xxxx")
        if blank_lines and i % 7 == 3:
            lines.append("")  # 穿插空行
    return "\n".join(lines) + "\n"


# ---------- 验收标准 §5.3 ----------
def test_import_with_header(db):
    """带表头文件：added=37，表头跳过，空行忽略，invalid=0。"""
    result = import_csv(db, make_csv(with_header=True))
    assert result["added"] == 37
    assert result["skipped"] == 0
    assert result["invalid"] == 0
    assert result["header_detected"] is True
    assert result["total_in_file"] == 38  # 表头 1 + 数据 37


def test_import_without_header(db):
    """无表头文件：同样 added=37（表头有无自适应）。"""
    result = import_csv(db, make_csv(with_header=False))
    assert result["added"] == 37
    assert result["skipped"] == 0
    assert result["invalid"] == 0
    assert result["header_detected"] is False
    assert result["total_in_file"] == 37


def test_reimport_all_skipped(db):
    """重复导入：added=0, skipped=37（去重生效）。"""
    text = make_csv(with_header=True)
    first = import_csv(db, text)
    assert first["added"] == 37
    second = import_csv(db, text)
    assert second["added"] == 0
    assert second["skipped"] == 37


# ---------- 解析规则 ----------
def test_bom_and_encoding():
    text = "email,password,apikey\na@b.com,p1,nvapi-xxx\n"
    assert decode_csv_bytes(text.encode("utf-8-sig")) == text
    assert decode_csv_bytes(b"\xef\xbb\xbf" + text.encode()) == text
    with pytest.raises(ValueError):
        decode_csv_bytes("中文,密码,nvapi-x".encode("gbk"))


def test_comma_space_tolerance(db):
    """字段分隔容忍「逗号+空格」。"""
    result = import_csv(db, "a@b.com , pass1 , nvapi-abc123")
    assert result["added"] == 1
    _, total = db.list_upstreams()
    assert total == 1
    u = db.get_all_upstreams()[0]
    assert u.email == "a@b.com"
    assert u.password == "pass1"
    assert u.apikey == "nvapi-abc123"


def test_column_order_tolerance(db):
    """列序容错：apikey 在任意列都能定位。"""
    text = (
        "nvapi-aaa111, x@y.com, pw\n"
        "x@y.com, nvapi-bbb222, pw\n"
        "x@y.com, pw, nvapi-ccc333\n"
    )
    result = import_csv(db, text)
    assert result["added"] == 3
    keys = {u.apikey: u for u in db.get_all_upstreams()}
    assert set(keys) == {"nvapi-aaa111", "nvapi-bbb222", "nvapi-ccc333"}
    assert all(u.email == "x@y.com" and u.password == "pw" for u in keys.values())


def test_single_column(db):
    """单列容错：仅 apikey，email/password 存 NULL。"""
    result = import_csv(db, "nvapi-only1\nnvapi-only2\n")
    assert result["added"] == 2
    for u in db.get_all_upstreams():
        assert u.email is None
        assert u.password is None


def test_invalid_lines_recorded():
    """非法行：计入 invalid 并记录原文件行号。"""
    text = "email,password,apikey\nok@x.com,p,nvapi-good\n垃圾行没有key\nanother bad line\n"
    parsed = parse_csv_text(text)
    assert parsed.header_detected is True
    assert parsed.invalid_lines == [3, 4]
    assert len(parsed.rows) == 1
    assert parsed.total_in_file == 4


def test_header_like_data_row_is_data():
    """存在 nvapi- 字段的行一律按数据行处理（即使含表头特征词）。"""
    parsed = parse_csv_text("email,password,nvapi-real-key")
    assert parsed.header_detected is False
    assert len(parsed.rows) == 1
    assert parsed.rows[0].apikey == "nvapi-real-key"
    # 无 @ 字段时，剩余第一个字段按规则归为 password
    assert parsed.rows[0].password == "email"


def test_blank_lines_not_counted():
    parsed = parse_csv_text("\n\n  \nnvapi-a\n\n")
    assert parsed.total_in_file == 1
    assert len(parsed.rows) == 1


def test_empty_file(db):
    result = import_csv(db, "\n\n \n")
    assert result["added"] == 0
    assert result["total_in_file"] == 0
    assert result["header_detected"] is False