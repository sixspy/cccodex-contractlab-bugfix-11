
import hashlib, secrets, sqlite3, threading
from datetime import datetime
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 补齐与清理两名值班同时抢写须排队：后到者只记孤儿争用，不执行写
class GovernanceGate:
    def __init__(self):
        self._lock = threading.Lock()

    def acquire(self, blocking: bool = True) -> bool:
        return self._lock.acquire(blocking)

    def release(self) -> None:
        self._lock.release()


governance_gate = GovernanceGate()

# 孤儿缓冲治理口径（见 docs/孤儿缓冲治理说明.md）：甲路径，当天禁止改口
GOVERNANCE_PATH = "甲"
ISOLATION_TABLE_ENABLED = False


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # 甲路径：每个连接开启外键，无批次/空批号/不存在批次的缓冲插入由数据库直接拒绝
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


def insert_buffer(conn, batch_id: int, reason: str):
    """缓冲行唯一写入接缝：外键在数据库层兜底，无真实批次即 IntegrityError。"""
    return conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (batch_id, reason))


def record_event(conn, event: str, detail: str, actor: str = "系统"):
    conn.execute(
        "INSERT INTO governance_events(event,detail,actor,created_at) VALUES (?,?,?,?)",
        (event, detail, actor, datetime.now().isoformat(timespec="seconds")),
    )


class LoginIn(BaseModel):
    username: str
    password: str


def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS contracts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  version TEXT UNIQUE NOT NULL,
  subject TEXT NOT NULL,
  mode TEXT NOT NULL,
  track TEXT NOT NULL,
  status TEXT NOT NULL,
  required_fields TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ingress_batches(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subject TEXT NOT NULL,
  payload_fields TEXT NOT NULL,
  contract_version TEXT,
  result TEXT NOT NULL,
  detail TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS buffers(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL,
  reason TEXT NOT NULL,
  FOREIGN KEY(batch_id) REFERENCES ingress_batches(id) ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS governance_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event TEXT NOT NULL,
  detail TEXT NOT NULL,
  actor TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"""


def _migrate_buffers_fk(conn):
    """旧 buffers 表无外键：登记并剔除历史孤儿后，表重建为带外键的正式表（甲路径，不进隔离表）。"""
    ddl = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='buffers'"
    ).fetchone()
    if ddl and "FOREIGN KEY" in (ddl[0] or ""):
        return
    orphans = conn.execute(
        """SELECT b.id, b.batch_id, b.reason FROM buffers b
           LEFT JOIN ingress_batches i ON i.id = b.batch_id
           WHERE i.id IS NULL OR b.batch_id IS NULL"""
    ).fetchall()
    for o in orphans:
        record_event(
            conn, "孤儿迁移",
            f"历史缓冲行#{o['id']}无真实批次(batch_id={o['batch_id']})：{o['reason']}，甲路径剔除不进隔离表",
        )
    # 表重建期间须临时关闭外键
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("ALTER TABLE buffers RENAME TO buffers_legacy")
    conn.execute(
        """CREATE TABLE buffers(
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             batch_id INTEGER NOT NULL,
             reason TEXT NOT NULL,
             FOREIGN KEY(batch_id) REFERENCES ingress_batches(id) ON DELETE RESTRICT
           )"""
    )
    conn.execute(
        """INSERT INTO buffers(id,batch_id,reason)
           SELECT l.id,l.batch_id,l.reason FROM buffers_legacy l
           JOIN ingress_batches i ON i.id=l.batch_id
           WHERE l.batch_id IS NOT NULL"""
    )
    conn.execute("DROP TABLE buffers_legacy")
    conn.execute("PRAGMA foreign_keys=ON")
    if orphans:
        conn.commit()


def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript(SCHEMA_SQL)
        _migrate_buffers_fk(conn)
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3", "订单入站", "严格", "正式", "启用", "order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow", "订单入站", "兼容", "影子", "试跑", "order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2", "库存入站", "严格", "正式", "停用", "sku,qty"))


@app.on_event("startup")
def _s():
    init_db()


@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}


@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16)
    SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}


class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present


@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    username = require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    with connect() as c:
        ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1", (body.subject,)).fetchone()
        if not ctr:
            raise HTTPException(404, "无启用正式契约")
        need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
        missing = sorted(need - fields)
        now = datetime.now().isoformat(timespec="seconds")
        if missing:
            if ctr["mode"] == "兼容":
                # 甲路径：批次行与缓冲行同一连接、同一事务双写；缓冲插入失败整笔回滚
                try:
                    cur = c.execute(
                        "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                        (body.subject, body.payload_fields, ctr["version"], "缓冲", "缺字段:" + ",".join(missing), now))
                    insert_buffer(c, cur.lastrowid, "缺字段进双写缓冲")
                except sqlite3.IntegrityError:
                    c.rollback()
                    with connect() as ev:
                        record_event(ev, "双写回滚", f"主题={body.subject} 缓冲行外键失败，批次与缓冲整笔回滚", username)
                    raise HTTPException(500, "双写缓冲落库失败：缓冲行未绑定真实批次，已整笔回滚")
                return {"result": "缓冲", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
            cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                            (body.subject, body.payload_fields, ctr["version"], "拒收", "缺字段:" + ",".join(missing), now))
            return {"result": "拒收", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
        cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                        (body.subject, body.payload_fields, ctr["version"], "通过", "字段齐全", now))
        return {"result": "通过", "version": ctr["version"], "batch_id": cur.lastrowid}


@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        # 入站列表只输出真实批次行，主键 id 恒非空，不存在空批号位
        return {"items": [dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall()]}


@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        # 外键已保证每行绑定真实批次；INNER JOIN 不会再吞掉孤儿，队列与账册同源
        rows = c.execute("""SELECT b.*, i.subject, i.payload_fields
                            FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id
                            ORDER BY b.id DESC""").fetchall()
        return {"items": [dict(r) for r in rows]}


@app.get("/api/buffers/{batch_id}")
def buffer_detail(batch_id: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        batch = c.execute("SELECT * FROM ingress_batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            raise HTTPException(404, "批次不存在")
        brows = c.execute("SELECT * FROM buffers WHERE batch_id=? ORDER BY id", (batch_id,)).fetchall()
        return {"batch_id": batch_id, "buffered": len(brows) > 0,
                "batch": dict(batch), "buffer_rows": [dict(r) for r in brows]}


@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        total = c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
        passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
        buffered = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
        rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
        # 只数绑定真实批次的缓冲行；无批次缓冲不得计入对账
        buf_rows = c.execute(
            """SELECT COUNT(*) c FROM buffers b
               JOIN ingress_batches i ON i.id=b.batch_id WHERE b.batch_id IS NOT NULL"""
        ).fetchone()["c"]
        orphan_rows = c.execute(
            """SELECT COUNT(*) c FROM buffers b
               LEFT JOIN ingress_batches i ON i.id=b.batch_id
               WHERE i.id IS NULL OR b.batch_id IS NULL"""
        ).fetchone()["c"]
    return {"ingress_total": total, "passed": passed, "buffered": buffered, "rejected": rejected,
            "buffer_rows": buf_rows, "orphan_rows": orphan_rows,
            "aligned": buffered == buf_rows and orphan_rows == 0}


class BatchOpIn(BaseModel):
    batch_id: int


class BackfillIn(BaseModel):
    batch_id: int
    payload_fields: str


def _gate_or_conflict(conn, action: str, username: str):
    if not governance_gate.acquire(blocking=False):
        record_event(conn, "孤儿争用", f"{action} 后到抢写，写入门被占用，仅登记争用不执行写", username)
        conn.commit()  # 先留痕再拒绝，避免异常回滚抹掉争用记录
        raise HTTPException(409, "补齐/清理正忙，本次为后到争用，已登记孤儿争用且未执行写操作")


@app.post("/api/governance/sweep")
def sweep(authorization: str | None = Header(default=None)):
    username = require_user(authorization)
    with connect() as c:
        _gate_or_conflict(c, "孤儿清扫", username)
        try:
            orphans = c.execute(
                """SELECT b.id, b.batch_id, b.reason FROM buffers b
                   LEFT JOIN ingress_batches i ON i.id=b.batch_id
                   WHERE i.id IS NULL OR b.batch_id IS NULL"""
            ).fetchall()
            for o in orphans:
                record_event(c, "孤儿清扫",
                             f"缓冲行#{o['id']}无真实批次(batch_id={o['batch_id']})：{o['reason']}", username)
                c.execute("DELETE FROM buffers WHERE id=?", (o["id"],))
            return {"removed": len(orphans)}
        finally:
            governance_gate.release()


@app.post("/api/governance/backfill")
def backfill(body: BackfillIn, authorization: str | None = Header(default=None)):
    username = require_user(authorization)
    if not body.payload_fields.strip():
        raise HTTPException(400, "补齐载荷字段不能为空")
    with connect() as c:
        _gate_or_conflict(c, "补齐", username)
        try:
            batch = c.execute("SELECT * FROM ingress_batches WHERE id=?", (body.batch_id,)).fetchone()
            if not batch:
                raise HTTPException(404, "批次不存在，无法补齐")
            c.execute("UPDATE ingress_batches SET payload_fields=?, result=?, detail=? WHERE id=?",
                      (body.payload_fields, "通过", "补齐后字段齐全", body.batch_id))
            c.execute("DELETE FROM buffers WHERE batch_id=?", (body.batch_id,))
            record_event(c, "补齐完成", f"批次#{body.batch_id} 补齐后转通过，绑定缓冲行出队", username)
            return {"batch_id": body.batch_id, "result": "通过"}
        finally:
            governance_gate.release()


@app.post("/api/governance/cleanup")
def cleanup(body: BatchOpIn, authorization: str | None = Header(default=None)):
    username = require_user(authorization)
    with connect() as c:
        _gate_or_conflict(c, "清理", username)
        try:
            batch = c.execute("SELECT * FROM ingress_batches WHERE id=?", (body.batch_id,)).fetchone()
            if not batch:
                raise HTTPException(404, "批次不存在，无法清理")
            # 禁止清理误删正式通过：拦下并记孤儿偏了
            if batch["result"] == "通过":
                record_event(c, "孤儿偏了",
                             f"请求清理正式通过批次#{body.batch_id}的缓冲行，已拦截未删除", username)
                c.commit()  # 偏差留痕先落库，再拒绝
                raise HTTPException(409, "正式通过批次的缓冲行禁止清理，已登记孤儿偏了")
            cur = c.execute("DELETE FROM buffers WHERE batch_id=?", (body.batch_id,))
            record_event(c, "清理完成", f"批次#{body.batch_id} 清理缓冲行 {cur.rowcount} 行", username)
            return {"batch_id": body.batch_id, "removed": cur.rowcount}
        finally:
            governance_gate.release()


@app.get("/api/governance/events")
def governance_events(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM governance_events ORDER BY id DESC").fetchall()
        return {"items": [dict(r) for r in rows]}


@app.get("/api/governance/policy")
def governance_policy(authorization: str | None = Header(default=None)):
    require_user(authorization)
    return {
        "path": GOVERNANCE_PATH,
        "isolation_table_enabled": ISOLATION_TABLE_ENABLED,
        "rule": "甲路径：缓冲行与批次同一事务双写并以外键约束绑定真实批次；不启用隔离表，口径不得混用",
        "locked_date": "2026-11-18",
        "doc": "docs/孤儿缓冲治理说明.md",
        "events": ["孤儿迁移", "孤儿清扫", "外键拒绝", "双写回滚", "孤儿争用", "孤儿偏了"],
    }


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
