import os
import time
import subprocess
import tempfile
import unittest


class WorkerIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()

    def test_workers_process_jobs_once(self):
        # use a temp copy of the project folder to avoid clashing DB
        tmp = tempfile.mkdtemp(prefix="qctl-int-")
        # copy files
        import shutil
        shutil.copytree(self.cwd, os.path.join(tmp, "work"), dirs_exist_ok=True)
        workdir = os.path.join(tmp, "work")
        # enqueue a bunch of simple jobs
        cmds = []
        for i in range(5):
            # use a simple cross-platform command
            job = '{"id":"wj%d","command":"echo x","max_retries":1}' % i
            cmds.append(['python', 'queuectl.py', 'enqueue', job])
        # run enqueues
        for c in cmds:
            subprocess.check_call(c, cwd=workdir)

        # start worker in background
        p = subprocess.Popen(['python', 'queuectl.py', 'worker', 'start', '--count', '2'], cwd=workdir)
        try:
            time.sleep(6)
            # request stop
            subprocess.check_call(['python', 'queuectl.py', 'worker', 'stop'], cwd=workdir)
            p.wait(timeout=10)
        finally:
            if p.poll() is None:
                p.terminate()

        # check all wj* jobs are completed or dead (not pending/processing)
        out = subprocess.check_output(['python', 'queuectl.py', 'list'], cwd=workdir)
        lines = out.decode('utf-8').strip().splitlines()
        states = {}
        import json
        for line in lines:
            try:
                j = json.loads(line)
                states[j.get('id')] = j.get('state')
            except Exception:
                continue
        for i in range(5):
            sid = f"wj{i}"
            self.assertIn(sid, states)
            self.assertIn(states[sid], ('completed', 'dead'))


if __name__ == '__main__':
    unittest.main()
