"""HTTP 层集成测试：建批 → 确认 → 放行 → 供数，以及 409 语义。"""
import os
import tempfile
import unittest

from fastapi.testclient import TestClient

from app import config, qc, store
from app.main import app


def seed(stations, readings):
    conn = store.connect()
    for sid, corr in stations:
        conn.execute("INSERT INTO stations(id, correction) VALUES (?, ?)", (sid, corr))
    for rid, sid, val in readings:
        conn.execute(
            "INSERT INTO readings(id, station_id, value) VALUES (?, ?, ?)",
            (rid, sid, val),
        )
    conn.commit()
    conn.close()


class QcApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.old_path = config.SQLITE_PATH
        config.SQLITE_PATH = os.path.join(self.tmp, "api.db")
        store.init_db()
        self.client = TestClient(app)

    def tearDown(self):
        config.SQLITE_PATH = self.old_path

    def test_full_flow_and_serving(self):
        # 未发布：建批 409
        r = self.client.post(
            "/api/qc/batches",
            json={"station_id": 1, "lower_limit": 0, "upper_limit": 50},
        )
        self.assertEqual(r.status_code, 409)

        store.set_meta("snapshot_done", "1")
        seed([(1, 10)], [(101, 1, 5), (102, 1, -50), (103, 1, 90)])

        # 非整数上下限被请求模型拒绝（422）
        r = self.client.post(
            "/api/qc/batches",
            json={"station_id": 1, "lower_limit": 1.5, "upper_limit": 50},
        )
        self.assertEqual(r.status_code, 422)

        r = self.client.post(
            "/api/qc/batches",
            json={"station_id": 1, "lower_limit": 0, "upper_limit": 50},
        )
        self.assertEqual(r.status_code, 201)
        bid = r.json()["id"]
        self.assertEqual(r.json()["anomaly_count"], 2)

        # 未确认完不能放行
        self.assertEqual(
            self.client.post(f"/api/qc/batches/{bid}/release").status_code, 409
        )
        # 确认正常读数被拒
        self.assertEqual(
            self.client.post(
                f"/api/qc/batches/{bid}/confirm", json={"reading_id": 101}
            ).status_code,
            409,
        )
        for rid in (102, 103):
            self.assertEqual(
                self.client.post(
                    f"/api/qc/batches/{bid}/confirm", json={"reading_id": rid}
                ).status_code,
                200,
            )
        r = self.client.post(f"/api/qc/batches/{bid}/release")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], qc.RELEASED)

        # 供数
        r = self.client.get(f"/api/qc/released/{bid}")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(r.json()["readings"]), 3)
        rows = self.client.get("/api/qc/released").json()["rows"]
        self.assertEqual([x["id"] for x in rows], [bid])

        # 页面可打开
        self.assertEqual(self.client.get("/qc").status_code, 200)
        self.assertEqual(self.client.get("/").status_code, 200)

    def test_invalidated_batch_not_served(self):
        store.set_meta("snapshot_done", "1")
        seed([(1, 10)], [(101, 1, 5)])
        bid = self.client.post(
            "/api/qc/batches",
            json={"station_id": 1, "lower_limit": -100, "upper_limit": 100},
        ).json()["id"]
        self.client.post(f"/api/qc/batches/{bid}/release")
        # 源端变更经同一事务落库并失效批次
        conn = store.connect()
        conn.execute("BEGIN IMMEDIATE")
        qc.invalidate_for_changes(
            conn, [("stations", "UPDATE", None, {"id": 1, "correction": 11})]
        )
        conn.execute("UPDATE stations SET correction = 11 WHERE id = 1")
        conn.commit()
        conn.close()
        self.assertEqual(
            self.client.get(f"/api/qc/released/{bid}").status_code, 409
        )
        self.assertEqual(self.client.get("/api/qc/released").json()["rows"], [])
        # 历史仍可在批次列表与明细中查看
        d = self.client.get(f"/api/qc/batches/{bid}").json()
        self.assertEqual(d["status"], qc.INVALIDATED)
        self.assertEqual(len(d["readings"]), 1)

    def test_unknown_batch_409(self):
        self.assertEqual(self.client.get("/api/qc/batches/42").status_code, 409)


if __name__ == "__main__":
    unittest.main()
