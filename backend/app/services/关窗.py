"""关窗：缓冲按运单物化为窗切片，封印旧窗并推进水位。

编排（水位推进是关窗的一环，不另起事务）：
取关窗锁 → BEGIN IMMEDIATE → 物化切片/消耗缓冲 → 窗水位推进封印旧窗、改现行窗规
→ 写关窗批次 → commit。任一步失败整体回滚，物化与改窗规同成败。
"""
from __future__ import annotations
import json
import uuid
from collections import defaultdict
from app.db import 连接
from app.seed import 此刻
from app.modules import 窗水位推进


class 窗已封(Exception):
    """目标窗已封印并推进，禁止重复关窗。"""


def 关当前窗(意图窗号: str | None = None) -> dict:
    with 窗水位推进.取锁():
        conn = 连接()
        try:
            conn.isolation_level = None
            conn.execute("BEGIN IMMEDIATE")
            try:
                规 = {r["键"]: r["值"] for r in conn.execute("SELECT 键,值 FROM 窗规")}
                窗号 = 规.get("当前窗号", "W-demo")
                窗起 = 规.get("窗起", "")
                窗止 = 规.get("窗止", "")
                if 意图窗号 is not None and 意图窗号 != 窗号:
                    raise 窗已封(
                        f"窗 {意图窗号} 已封印推进，现行窗为 {窗号}，禁止重复关窗"
                    )
                rows = conn.execute(
                    "SELECT * FROM 入窗缓冲 WHERE 窗号=? AND 命运='在缓冲'", (窗号,)
                ).fetchall()
                by_waybill: dict[str, list] = defaultdict(list)
                for r in rows:
                    by_waybill[r["运单键"]].append(r)
                成片 = 0
                时刻 = 此刻()
                for 运单, evs in by_waybill.items():
                    切片 = f"片-{窗号}-{运单}-{uuid.uuid4().hex[:6]}"
                    conn.execute(
                        """INSERT INTO 窗切片账(切片编号,窗号,运单键,事件数,窗起,窗止,物化时,命运)
                           VALUES (?,?,?,?,?,?,?,?)""",
                        (切片, 窗号, 运单, len(evs), 窗起, 窗止, 时刻, "已成片"),
                    )
                    for e in evs:
                        conn.execute(
                            "UPDATE 入窗缓冲 SET 命运=? WHERE 事件编号=?",
                            ("已物化", e["事件编号"]),
                        )
                        conn.execute(
                            "UPDATE 观测点 SET 命运=? WHERE 事件编号=?",
                            ("已成片", e["事件编号"]),
                        )
                    成片 += 1
                # 空窗同样封印推进（空窗封印语义）。
                封印 = 窗水位推进.推进(conn, 规)
                报告 = {
                    "批次编号": f"关窗-{uuid.uuid4().hex[:8]}",
                    "窗号": 窗号,
                    "成片数": 成片,
                    "缓冲消耗": len(rows),
                    "水位推进": 封印,
                }
                conn.execute(
                    """INSERT INTO 关窗批次(编号,窗号,时刻,成片数,报告) VALUES (?,?,?,?,?)""",
                    (报告["批次编号"], 窗号, 时刻, 成片, json.dumps(报告, ensure_ascii=False)),
                )
                conn.commit()
                return 报告
            except Exception:
                conn.rollback()
                raise
        finally:
            conn.close()
