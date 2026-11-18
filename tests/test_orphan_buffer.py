# 孤儿缓冲治理测试 —— 顺序即题目要求：
# 1) 先插入无批次孤儿行复现队列错乱；2) 再落外键失败测；3) 最后验同事务外键治理与并发争用。

import threading
import pytest
from fastapi.testclient import TestClient

import app as A
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


@pytest.fixture()
def client():
    c = TestClient(app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    c.headers.update({"Authorization": f"Bearer {tok}"})
    return c


def _add_compatible_contract():
    # 兼容模式正式契约：缺字段触发双写缓冲
    with A.connect() as c:
        c.execute(
            "INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
            ("v5", "订单入站", "兼容", "正式", "启用", "order_id,amount,currency,channel"),
        )


def _insert_orphan_raw(batch_id=999999, reason="孤儿:批次不存在"):
    """绕过应用、关闭外键，直接物理插入无批次孤儿行（复现旧缺陷）。"""
    conn = A.connect()
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (batch_id, reason))
        conn.commit()
    finally:
        conn.close()


# ---- 第 1 步：复现队列错乱 -------------------------------------------------

def test_01_orphan_row_breaks_queue_and_reconcile(client):
    # 先来一笔正常双写缓冲
    _add_compatible_contract()
    r = client.post("/api/ingress/validate", json={"subject": "订单入站", "payload_fields": "order_id"})
    assert r.json()["result"] == "缓冲"

    # 插入无批次孤儿行，复现队列/账册错乱
    _insert_orphan_raw()
    _insert_orphan_raw(batch_id=0, reason="孤儿:无批号")

    # 队列只展示绑定真实批次的行，孤儿不进正式缓冲队列
    buf = client.get("/api/buffers").json()["items"]
    assert all(b["batch_id"] for b in buf), "队列出现空批号"
    assert len(buf) == 1

    # 对账：孤儿单独披露、不计入缓冲账册
    recon = client.get("/api/reconcile").json()
    assert recon["orphan_rows"] == 2
    assert recon["buffer_rows"] == 1
    assert recon["buffered"] == 1
    assert recon["aligned"] is False

    # 清扫后账册对齐，且清扫有留痕
    sweep = client.post("/api/governance/sweep").json()
    assert sweep["removed"] == 2
    recon2 = client.get("/api/reconcile").json()
    assert recon2["orphan_rows"] == 0 and recon2["aligned"] is True
    kinds = {e["event"] for e in client.get("/api/governance/events").json()["items"]}
    assert "孤儿清扫" in kinds


# ---- 第 2 步：外键失败测 ---------------------------------------------------

def test_02_foreign_key_rejects_buffer_without_real_batch(client):
    # 连接默认开启外键：不存在批次的缓冲行插不进去
    conn = A.connect()
    try:
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        with pytest.raises(A.sqlite3.IntegrityError):
            conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (424242, "孤儿尝试"))
            conn.commit()
        conn.rollback()
        assert conn.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"] == 0
    finally:
        conn.close()


def test_03_double_write_is_atomic_same_transaction(client, monkeypatch):
    _add_compatible_contract()

    def boom(conn, batch_id, reason):
        raise A.sqlite3.IntegrityError("FOREIGN KEY constraint failed")

    monkeypatch.setattr(A, "insert_buffer", boom)
    r = client.post("/api/ingress/validate", json={"subject": "订单入站", "payload_fields": "order_id"})
    assert r.status_code == 500

    with A.connect() as c:
        # 同事务回滚：没有半截批次，也没有缓冲行
        assert c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"] == 0
        assert c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"] == 0
    kinds = {e["event"] for e in client.get("/api/governance/events").json()["items"]}
    assert "双写回滚" in kinds


# ---- 第 3 步：同事务外键治理 + 补齐/清理 -----------------------------------

def _make_buffered_batch():
    with A.connect() as c:
        cur = c.execute(
            """INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)
               VALUES ('订单入站','order_id','v5','缓冲','缺字段:amount','t1')"""
        )
        bid = cur.lastrowid
        c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (bid, "缺字段进双写缓冲"))
        return bid


def test_04_backfill_binds_real_batch_and_clears_buffer(client):
    bid = _make_buffered_batch()
    r = client.post("/api/governance/backfill", json={"batch_id": bid, "payload_fields": "order_id,amount,currency,channel"})
    assert r.status_code == 200 and r.json()["batch_id"] == bid
    with A.connect() as c:
        row = c.execute("SELECT result,payload_fields FROM ingress_batches WHERE id=?", (bid,)).fetchone()
        assert row["result"] == "通过" and "channel" in row["payload_fields"]
        assert c.execute("SELECT COUNT(*) c FROM buffers WHERE batch_id=?", (bid,)).fetchone()["c"] == 0
    detail = client.get(f"/api/buffers/{bid}").json()
    # 缓冲详情对已补齐批次返回空队列状态，且任何返回行均带真实批号
    assert detail["batch_id"] == bid and detail["buffered"] is False
    recon = client.get("/api/reconcile").json()
    assert recon["aligned"] is True and recon["orphan_rows"] == 0


def test_05_cleanup_removes_rejected_buffer_but_never_passed(client):
    # 拒收批次的残留缓冲行：可清理
    with A.connect() as c:
        cur = c.execute(
            """INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)
               VALUES ('订单入站','x','v3','拒收','缺字段','t2')"""
        )
        rej = cur.lastrowid
        c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (rej, "拒收残留"))
        # 通过批次若挂了缓冲行：禁止清理误删正式通过
        cur2 = c.execute(
            """INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)
               VALUES ('订单入站','全','v3','通过','字段齐全','t3')"""
        )
        passed = cur2.lastrowid
        c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (passed, "误挂通过批次"))

    ok = client.post("/api/governance/cleanup", json={"batch_id": rej})
    assert ok.status_code == 200 and ok.json()["removed"] == 1
    blocked = client.post("/api/governance/cleanup", json={"batch_id": passed})
    assert blocked.status_code == 409
    with A.connect() as c:
        assert c.execute("SELECT COUNT(*) c FROM buffers WHERE batch_id=?", (rej,)).fetchone()["c"] == 0
        assert c.execute("SELECT COUNT(*) c FROM buffers WHERE batch_id=?", (passed,)).fetchone()["c"] == 1
    kinds = {e["event"] for e in client.get("/api/governance/events").json()["items"]}
    assert "孤儿偏了" in kinds


def test_06_backfill_and_cleanup_contention_serialized(client, monkeypatch):
    bid = _make_buffered_batch()
    entered = threading.Event()
    release = threading.Event()
    results = []

    import app as mod

    # 让先到者持门期间阻塞，制造确定的同时抢写
    orig = mod.GovernanceGate.acquire

    def slow_acquire(self, blocking=True):
        acquired = orig(self, blocking)
        if acquired and not entered.is_set():
            entered.set()
            release.wait(timeout=5)
        return acquired

    monkeypatch.setattr(mod.GovernanceGate, "acquire", slow_acquire)

    def first():
        results.append(("first", client.post("/api/governance/backfill",
                                              json={"batch_id": bid, "payload_fields": "order_id,amount,currency,channel"}).status_code))

    t = threading.Thread(target=first)
    t.start()
    entered.wait(timeout=5)
    # 后到者抢写：只记争用，不执行写
    late = client.post("/api/governance/cleanup", json={"batch_id": bid})
    assert late.status_code == 409 and "争用" in late.json()["detail"]
    release.set()
    t.join(timeout=5)

    assert results == [("first", 200)]
    kinds = {e["event"] for e in client.get("/api/governance/events").json()["items"]}
    assert "孤儿争用" in kinds


def test_07_policy_locks_path_a_and_pages_have_no_empty_batch(client):
    pol = client.get("/api/governance/policy").json()
    assert pol["path"] == "甲" and pol["isolation_table_enabled"] is False
    assert "外键" in pol["rule"]

    # 缓冲队列、入站列表、对账、详情四处无空批号
    _make_buffered_batch()
    _insert_orphan_raw()
    for row in client.get("/api/buffers").json()["items"]:
        assert row["batch_id"]
    for row in client.get("/api/ingress").json()["items"]:
        assert row["id"]
    assert "orphan_rows" in client.get("/api/reconcile").json()
    assert client.get("/api/buffers/999999").status_code == 404


def test_08_concurrent_requests_keep_db_consistent(client):
    _add_compatible_contract()
    errs = []

    def worker(i):
        try:
            r = client.post("/api/ingress/validate", json={"subject": "订单入站", "payload_fields": "order_id"})
            if r.status_code != 200:
                errs.append((i, r.status_code))
        except Exception as e:  # pragma: no cover - 防御性
            errs.append((i, repr(e)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not errs
    recon = client.get("/api/reconcile").json()
    assert recon["orphan_rows"] == 0 and recon["aligned"] is True
