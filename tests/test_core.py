import os
import tempfile
import unittest
from qctl import db


class CoreTests(unittest.TestCase):
    def setUp(self):
        fd, self.dbpath = tempfile.mkstemp(prefix="qctl-test-", suffix=".db")
        os.close(fd)
        # remove file so init creates schema
        try:
            os.remove(self.dbpath)
        except Exception:
            pass
        db.init_db(self.dbpath)

    def tearDown(self):
        try:
            os.remove(self.dbpath)
        except Exception:
            pass

    def test_enqueue_and_list(self):
        job = {"id": "t1", "command": "echo x", "max_retries": 1}
        db.enqueue_job(self.dbpath, job)
        rows = db.list_jobs(self.dbpath, state="pending")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "t1")

    def test_fail_and_dlq(self):
        job = {"id": "t2", "command": "falsecmd", "max_retries": 0}
        db.enqueue_job(self.dbpath, job)
        # fetch and lock
        j = db.fetch_and_lock_job(self.dbpath)
        self.assertIsNotNone(j)
        self.assertEqual(j["id"], "t2")
        # fail once; since max_retries=0, it should go to dead
        db.fail_job(self.dbpath, "t2", attempts=j.get("attempts", 0), max_retries=j.get("max_retries", 0), error_msg="err", backoff_base=2)
        rows = db.list_jobs(self.dbpath, state="dead")
        self.assertTrue(any(r["id"] == "t2" for r in rows))

    def test_dlq_retry(self):
        job = {"id": "t3", "command": "dummy", "max_retries": 0}
        db.enqueue_job(self.dbpath, job)
        j = db.fetch_and_lock_job(self.dbpath)
        db.fail_job(self.dbpath, "t3", attempts=j.get("attempts", 0), max_retries=j.get("max_retries", 0), error_msg="err", backoff_base=2)
        # now retry dead job
        db.retry_dead_job(self.dbpath, "t3")
        rows = db.list_jobs(self.dbpath, state="pending")
        self.assertTrue(any(r["id"] == "t3" for r in rows))


if __name__ == "__main__":
    unittest.main()
