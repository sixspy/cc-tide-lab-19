"""窗水位推进：关窗封印旧窗并把窗规推进到下一窗。

形态（切流账册推进 + 双写两轨的合体）：
- 关窗成功后，旧窗规被钉进「窗水位账」只读归档（封印），现行窗规改写为推进后的新窗起止。
- 切片账写旧窗号，下一趟装载读新窗号，读引用天然分开，切片窗号不撞车。
- 物化与改窗规在同一事务、同一把关窗锁内：任一失败整体回滚。
- 旧窗号在窗水位账内唯一；同窗并发关窗第二个拿锁后撞唯一约束，绝不双成功。
"""
from __future__ import annotations
import threading

from app.db import 连接
from app.seed import 此刻
from app.services import 时间

NAME = "窗水位推进"
CONTRACT = "关窗后按窗长秒自动推进下一窗起止"

HOOKS = (
    "before_load",
    "after_load",
    "decide_window",
    "before_close",
    "after_close",
    "extra_verify_checks",
)

# 关窗编排与装载共用的写锁：关窗持锁做物化+封印+推进，装载持锁读现行窗规后落缓冲，
# 杜绝「推进与装载并发把事件写进已封印旧窗」。
关窗锁 = threading.Lock()


def describe() -> dict:
    return {
        "空桩": NAME,
        "契约": CONTRACT,
        "钩子": list(HOOKS),
        "默认启用": True,
        "说明": "关窗编排内接线：封印旧窗进窗水位账，窗规推进到下一窗，装载改读新窗",
    }


def 取锁() -> threading.Lock:
    """关窗/装载共用的串行写锁。"""
    return 关窗锁


def 下一窗号(旧窗号: str, 新窗起: str) -> str:
    """按选定形态命名新窗：保留窗号前缀，挂新窗起的整点戳。"""
    前缀 = 旧窗号.rsplit("-", 1)[0] if "-" in 旧窗号 else 旧窗号
    return 时间.格式窗号(前缀, 新窗起)


def 推进(conn, 规: dict) -> dict:
    """封印旧窗并推进窗规。须在关窗事务与关窗锁内调用，同成败。

    返回封印记录（含归档键、新旧窗号、新旧起止、valid）。
    """
    旧窗号 = 规["当前窗号"]
    旧起 = 规["窗起"]
    旧止 = 规["窗止"]
    窗长 = int(规.get("窗长秒") or 时间.窗长秒(旧起, 旧止))
    新起, 新止 = 时间.推进窗(旧起, 旧止, 窗长)
    新窗号 = 下一窗号(旧窗号, 新起)
    归档键 = f"{旧窗号}@{旧起}~{旧止}"
    时刻 = 此刻()
    # 旧窗号/归档键唯一：同窗第二次关窗在此撞约束，调用方回滚并报错，不会双成功。
    cur = conn.execute(
        """INSERT INTO 窗水位账
           (归档键,旧窗号,旧窗起,旧窗止,新窗号,新窗起,新窗止,窗长秒,时刻,valid)
           VALUES (?,?,?,?,?,?,?,?,?,1)""",
        (归档键, 旧窗号, 旧起, 旧止, 新窗号, 新起, 新止, 窗长, 时刻),
    )
    for 键, 值 in (("当前窗号", 新窗号), ("窗起", 新起), ("窗止", 新止)):
        conn.execute(
            "INSERT INTO 窗规(键,值) VALUES (?,?) ON CONFLICT(键) DO UPDATE SET 值=excluded.值",
            (键, 值),
        )
    return {
        "归档键": 归档键,
        "旧窗号": 旧窗号,
        "新窗号": 新窗号,
        "旧窗起": 旧起,
        "旧窗止": 旧止,
        "新窗起": 新起,
        "新窗止": 新止,
        "窗长秒": 窗长,
        "编号": cur.lastrowid,
        "时刻": 时刻,
        "valid": True,
    }


def 水位只读() -> dict:
    """只读接口：最近一次封印的旧窗号、新窗号、归档键、valid。"""
    conn = 连接()
    try:
        row = conn.execute(
            "SELECT * FROM 窗水位账 WHERE valid=1 ORDER BY 编号 DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return {"旧窗号": None, "新窗号": None, "归档键": None, "valid": False}
        return {
            "旧窗号": row["旧窗号"],
            "新窗号": row["新窗号"],
            "归档键": row["归档键"],
            "valid": bool(row["valid"]),
        }
    finally:
        conn.close()


def extra_verify_checks(snapshot: dict) -> list[str]:
    """附加贯验：封印账册与现行窗规、在缓冲窗号必须互证。"""
    错: list[str] = []
    封印 = snapshot.get("封印") or []
    规 = snapshot.get("规") or {}
    if 封印:
        最新 = 封印[0]
        if 规.get("当前窗号") != 最新["新窗号"]:
            错.append(
                f"窗号撕裂：现行窗号={规.get('当前窗号')} 与最新封印新窗号={最新['新窗号']} 不一致"
            )
        if 最新["新窗起"] != 最新["旧窗止"]:
            错.append(
                f"水位不连续：封印 {最新['旧窗号']} 旧窗止 {最新['旧窗止']} ≠ 新窗起 {最新['新窗起']}"
            )
        已封窗号 = {s["旧窗号"] for s in 封印}
        for r in snapshot.get("在缓冲") or []:
            if r["窗号"] in 已封窗号:
                错.append(f"封印窗 {r['窗号']} 仍有在缓冲事件 {r['事件编号']}，装载写错窗")
    return 错
