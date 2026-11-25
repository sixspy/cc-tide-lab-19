"""窗水位推进：关窗封印 + 水位推进 + 装载读新窗 + 并发锁。"""
from pathlib import Path
import os
import tempfile
import threading

import pytest
from starlette.testclient import TestClient

_tmp = tempfile.mkdtemp()
os.environ.setdefault("数据目录", _tmp)
os.environ.setdefault("夹具目录", str(Path(__file__).resolve().parents[2] / "fixtures"))

from app import seed
from app import db as _db
from app.db import 连接
from app.main import app
from app.services import 装载, 关窗, 贯验, 目录, 夹具
from app.modules import 窗水位推进

# 重绑定到本文件独占库，避免与基线测试文件共享 sqlite 文件造成跨文件状态串扰。
_库 = Path(_tmp) / "窗水位推进.db"
_db.库路径 = _库


def _装(编号: str, 事件时: str, 运单="运单-T", 批次=None):
    return 装载.装一批({
        "批次编号": 批次 or f"批-{编号}",
        "事件": [{
            "事件编号": 编号,
            "运单键": 运单,
            "网点键": "网点-东站",
            "事件时": 事件时,
        }],
    })


@pytest.fixture(autouse=True)
def _重置库():
    # WAL 连接可能令文件 unlink 不彻底，先 checkpoint，再 drop 全部业务表后重建。
    _db.库路径 = _库
    seed.初始化库()
    conn = _db.连接()
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        for 表 in [
            r["name"] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        ]:
            conn.execute(f"DROP TABLE IF EXISTS {表}")
        conn.commit()
    finally:
        conn.close()
    seed.初始化库()
    yield


def test_成片关窗后水位推进且封印旧窗():
    规 = 目录.窗规()
    旧窗号, 旧起, 旧止 = 规["当前窗号"], 规["窗起"], 规["窗止"]
    _装("e1", "2026-11-24T00:10:00Z")
    报告 = 关窗.关当前窗()

    新规 = 目录.窗规()
    # 现行当前窗号必须已变，且起止按窗长推进
    assert 新规["当前窗号"] != 旧窗号
    assert 新规["窗起"] == 旧止
    assert 新规["窗止"] == "2026-11-24T02:00:00Z"
    # 关窗回执带水位推进段，可区分关前后窗号
    assert 报告["水位推进"]["旧窗号"] == 旧窗号
    assert 报告["水位推进"]["新窗号"] == 新规["当前窗号"]
    assert 报告["水位推进"]["valid"] is True
    assert 报告["水位推进"]["归档键"]

    水位 = 窗水位推进.水位只读()
    assert 水位["旧窗号"] == 旧窗号
    assert 水位["新窗号"] == 新规["当前窗号"]
    assert 水位["valid"] is True
    assert 旧起 in 水位["归档键"] and 旧窗号 in 水位["归档键"]

    conn = 连接()
    try:
        封 = conn.execute(
            "SELECT * FROM 窗水位账 WHERE 旧窗号=? AND valid=1", (旧窗号,)
        ).fetchone()
        assert 封 is not None
        assert 封["新窗号"] == 新规["当前窗号"]
        assert 封["新窗起"] == 旧止
        # 切片钉在旧窗，封印即只读归档
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM 窗切片账 WHERE 窗号=?", (旧窗号,)
        ).fetchone()["c"] >= 1
    finally:
        conn.close()


def test_空窗封印后水位同样推进():
    规 = 目录.窗规()
    旧窗号 = 规["当前窗号"]
    报告 = 关窗.关当前窗()
    assert 报告["成片数"] == 0
    assert 报告["缓冲消耗"] == 0

    新规 = 目录.窗规()
    assert 新规["当前窗号"] != 旧窗号
    assert 新规["窗起"] == 规["窗止"]
    水位 = 窗水位推进.水位只读()
    assert 水位["valid"] is True
    assert 水位["旧窗号"] == 旧窗号
    # 空窗也有封印记录
    conn = 连接()
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM 窗水位账 WHERE 旧窗号=? AND valid=1",
            (旧窗号,),
        ).fetchone()["c"] == 1
    finally:
        conn.close()


def test_推进失败则物化与窗规同回滚(monkeypatch):
    _装("e-rb", "2026-11-24T00:09:00Z")
    规前 = 目录.窗规()

    def 炸(*a, **k):
        raise RuntimeError("模拟推进失败")
    monkeypatch.setattr(窗水位推进, "推进", 炸)
    with pytest.raises(RuntimeError, match="模拟推进失败"):
        关窗.关当前窗()

    规后 = 目录.窗规()
    assert 规后["当前窗号"] == 规前["当前窗号"]
    assert 规后["窗起"] == 规前["窗起"]
    conn = 连接()
    try:
        assert conn.execute("SELECT COUNT(*) AS c FROM 窗切片账").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) AS c FROM 关窗批次").fetchone()["c"] == 0
        assert conn.execute("SELECT COUNT(*) AS c FROM 窗水位账").fetchone()["c"] == 0
        # 缓冲命运未被消耗，仍是在缓冲
        assert conn.execute(
            "SELECT 命运 FROM 入窗缓冲 WHERE 事件编号=?", ("e-rb",)
        ).fetchone()["命运"] == "在缓冲"
    finally:
        conn.close()


def test_下一趟装载写新窗不撞旧窗():
    _装("e-old", "2026-11-24T00:20:00Z")
    关窗.关当前窗()
    新规 = 目录.窗规()
    # 新窗起止内的事件必须进新窗缓冲
    报告 = _装("e-new", "2026-11-24T01:20:00Z", 批次="第二批")
    assert 报告["入缓冲"] == 1
    行 = next(x for x in 报告["明细"] if x["命运"] == "入缓冲")
    assert 行["窗号"] == 新规["当前窗号"]
    conn = 连接()
    try:
        撞 = conn.execute(
            "SELECT COUNT(*) AS c FROM 入窗缓冲 WHERE 窗号=? AND 命运='在缓冲'",
            (行["窗号"],),
        ).fetchone()["c"]
        assert 撞 == 1
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM 入窗缓冲 WHERE 窗号=? AND 命运='在缓冲'",
            ("W-demo",),
        ).fetchone()["c"] == 0
    finally:
        conn.close()


def test_并发_装载先持锁则旧窗缓冲被同窗关窗物化不残留(monkeypatch):
    # 确定性交错一：装载先拿到写锁，事件落进推进前的旧窗缓冲；
    # 随后关窗必须把它一并物化，封印旧窗下不得残留「在缓冲」。
    _装("e-base", "2026-11-24T00:05:00Z")
    已入锁, 放行 = threading.Event(), threading.Event()

    from app.services import 时间 as 时间服务
    原判定 = 时间服务.在窗内

    def 堵判定(*a, **k):
        已入锁.set()
        放行.wait(timeout=10)
        return 原判定(*a, **k)
    monkeypatch.setattr(时间服务, "在窗内", 堵判定)
    装载结果 = {}
    t = threading.Thread(target=lambda: 装载结果.update(
        报告=_装("e-race", "2026-11-24T00:45:00Z", 批次="并发批")))
    t.start()
    assert 已入锁.wait(timeout=10)
    放行.set()
    t.join(timeout=10)
    assert not t.is_alive()
    关 = 关窗.关当前窗()

    assert 关["窗号"] == "W-demo"
    assert 关["缓冲消耗"] == 2
    conn = 连接()
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM 入窗缓冲 WHERE 窗号='W-demo' AND 命运='在缓冲'"
        ).fetchone()["c"] == 0
        assert conn.execute(
            "SELECT 命运 FROM 观测点 WHERE 事件编号='e-race'"
        ).fetchone()["命运"] == "已成片"
        assert conn.execute(
            "SELECT COALESCE(SUM(事件数),0) AS c FROM 窗切片账 WHERE 窗号='W-demo'"
        ).fetchone()["c"] == 2
    finally:
        conn.close()


def test_并发_关窗先持锁则装载放行后读新窗不写旧窗(monkeypatch):
    # 确定性交错二：关窗先持锁走到封印推进并堵住，装载在锁外等待；
    # 关窗提交放行后，装载读到的必须是推进后的新窗。
    _装("e-base", "2026-11-24T00:05:00Z")
    关窗入锁, 放行 = threading.Event(), threading.Event()
    原推进 = 窗水位推进.推进

    def 堵推进(conn, 规):
        关窗入锁.set()
        放行.wait(timeout=10)
        return 原推进(conn, 规)
    monkeypatch.setattr(窗水位推进, "推进", 堵推进)
    装载结果 = {}
    关线程 = threading.Thread(target=关窗.关当前窗)
    关线程.start()
    assert 关窗入锁.wait(timeout=10)
    灌线程 = threading.Thread(target=lambda: 装载结果.update(
        报告=_装("e-race", "2026-11-24T01:05:00Z", 批次="并发批")))
    灌线程.start()
    放行.set()
    关线程.join(timeout=10)
    灌线程.join(timeout=10)
    assert not 关线程.is_alive() and not 灌线程.is_alive()

    新窗号 = 目录.窗规()["当前窗号"]
    assert 新窗号 != "W-demo"
    assert 装载结果["报告"]["入缓冲"] == 1
    conn = 连接()
    try:
        # 封印旧窗下不得再有在缓冲行；新事件落在推进后的新窗
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM 入窗缓冲 WHERE 窗号='W-demo' AND 命运='在缓冲'"
        ).fetchone()["c"] == 0
        行 = conn.execute(
            "SELECT 窗号,命运 FROM 入窗缓冲 WHERE 事件编号='e-race'"
        ).fetchone()
        assert 行 is not None
        assert 行["窗号"] == 新窗号 and 行["命运"] == "在缓冲"
    finally:
        conn.close()


def test_并发双关窗只有一个成功封印():
    _装("e-double", "2026-11-24T00:30:00Z")
    屏障 = threading.Barrier(2)
    # 关窗前快照意图：模拟双击时两个请求都盯着同一个窗号
    意图窗号 = 目录.窗规()["当前窗号"]
    结果 = []

    def 关():
        try:
            屏障.wait(timeout=10)
            结果.append(("ok", 关窗.关当前窗(意图窗号)))
        except Exception as ex:  # noqa: BLE001
            结果.append(("err", str(ex)))

    ts = [threading.Thread(target=关) for _ in range(2)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=20)
    assert all(not t.is_alive() for t in ts)
    assert len(结果) == 2
    assert sorted(k for k, _ in 结果) == ["err", "ok"]

    conn = 连接()
    try:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM 窗水位账 WHERE valid=1"
        ).fetchone()["c"] == 1
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM 窗切片账"
        ).fetchone()["c"] == 1
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM 关窗批次"
        ).fetchone()["c"] == 1
    finally:
        conn.close()


def test_水位只读接口():
    client = TestClient(app)
    初 = client.get("/api/窗水位推进").json()
    assert set(初) >= {"旧窗号", "新窗号", "归档键", "valid"}
    assert 初["valid"] is False
    assert 初["旧窗号"] is None and 初["新窗号"] is None

    关窗.关当前窗()
    后 = client.get("/api/窗水位推进").json()
    assert 后["valid"] is True
    assert 后["旧窗号"] == "W-demo"
    assert 后["新窗号"] == 目录.窗规()["当前窗号"]
    assert 后["归档键"]


def test_夹具主链仍贯验对齐且带水位检查():
    装载.装一批(夹具.读取("窗内成片样例"))
    关窗.关当前窗()
    验 = 贯验.跑一次()
    assert 验["是否对齐"] is True
    assert 窗水位推进.extra_verify_checks(贯验快照()) == []


def test_贯验扩展能识别封印与窗规撕裂():
    装载.装一批(夹具.读取("窗内成片样例"))
    关窗.关当前窗()
    # 手工制造撕裂：窗规退回旧窗号
    目录.改窗规({"当前窗号": "W-demo"})
    错 = 窗水位推进.extra_verify_checks(贯验快照())
    assert any("窗号" in e for e in 错)


def 贯验快照() -> dict:
    conn = 连接()
    try:
        return {
            "规": {r["键"]: r["值"] for r in conn.execute("SELECT 键,值 FROM 窗规")},
            "封印": [dict(r) for r in conn.execute(
                "SELECT * FROM 窗水位账 WHERE valid=1 ORDER BY 编号 DESC"
            )],
            "在缓冲": conn.execute(
                "SELECT * FROM 入窗缓冲 WHERE 命运='在缓冲'"
            ).fetchall(),
        }
    finally:
        conn.close()
