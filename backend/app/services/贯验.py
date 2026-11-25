"""窗贯验：缓冲/切片/迟到互证，附加窗水位封印检查。"""
from __future__ import annotations
from app.db import 连接
from app.seed import 此刻
from app.modules import 窗水位推进


def 跑一次() -> dict:
    conn = 连接()
    try:
        缓冲 = conn.execute(
            "SELECT COUNT(*) AS c FROM 入窗缓冲 WHERE 命运='在缓冲'"
        ).fetchone()["c"]
        已物化缓冲 = conn.execute(
            "SELECT COUNT(*) AS c FROM 入窗缓冲 WHERE 命运='已物化'"
        ).fetchone()["c"]
        切片事件 = conn.execute(
            "SELECT COALESCE(SUM(事件数),0) AS c FROM 窗切片账"
        ).fetchone()["c"]
        迟到 = conn.execute("SELECT COUNT(*) AS c FROM 迟到匣").fetchone()["c"]
        规 = {r["键"]: r["值"] for r in conn.execute("SELECT 键,值 FROM 窗规")}
        封印 = [dict(r) for r in conn.execute(
            "SELECT * FROM 窗水位账 WHERE valid=1 ORDER BY 编号 DESC"
        )]
        在缓冲 = [dict(r) for r in conn.execute(
            "SELECT * FROM 入窗缓冲 WHERE 命运='在缓冲'"
        )]
        水位检查 = 窗水位推进.extra_verify_checks(
            {"规": 规, "封印": 封印, "在缓冲": 在缓冲}
        )
        # 基线口径：已物化缓冲事件数应等于切片事件数之和
        对齐 = 1 if int(已物化缓冲) == int(切片事件) and not 水位检查 else 0
        说明 = "缓冲物化与切片事件数一致" if int(已物化缓冲) == int(切片事件) else f"撕裂：已物化缓冲={已物化缓冲} 切片事件={切片事件}"
        if 水位检查:
            说明 = "；".join([说明, *水位检查])
        时刻 = 此刻()
        conn.execute(
            """INSERT INTO 窗贯验(时刻,缓冲笔数,切片笔数,迟到笔数,是否对齐,说明)
               VALUES (?,?,?,?,?,?)""",
            (时刻, 缓冲, int(切片事件), 迟到, 对齐, 说明),
        )
        conn.commit()
        return {
            "时刻": 时刻,
            "在缓冲": 缓冲,
            "已物化缓冲": int(已物化缓冲),
            "切片事件": int(切片事件),
            "迟到": 迟到,
            "是否对齐": bool(对齐),
            "说明": 说明,
            "水位检查": 水位检查,
        }
    finally:
        conn.close()
