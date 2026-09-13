"""Bounded Windows stdio workers. One in-flight HTTP request per worker.

Never replay a failed request: a POST may already have executed upstream.
Only workers whose EOF frame was consumed may be reused by another session.
"""
import base64
import json
import os
from pathlib import Path
import queue
import subprocess
import threading
import time


class _Worker:
    def __init__(self, python):
        self.proc=subprocess.Popen([python,'-u','-c',Path(__file__).with_name('windows_agent_http.py').read_text()],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
        self.records=queue.Queue(maxsize=128)
        self.closed=threading.Event()
        self.close_lock=threading.Lock()
        def reader():
            while not self.closed.is_set():
                try:
                    raw=self.proc.stdout.readline(2*1024*1024+1)
                except (OSError,ValueError):
                    break
                try:
                    record=json.loads(raw) if raw and len(raw)<=2*1024*1024 else {'error':'relay closed'}
                except (ValueError,UnicodeError):
                    record={'error':'invalid relay record'}
                while not self.closed.is_set():
                    try:
                        self.records.put(record,timeout=0.1)
                        break
                    except queue.Full:
                        continue
                if not raw or 'error' in record:
                    break
        threading.Thread(target=reader,daemon=True).start()

    def close(self):
        with self.close_lock:
            if self.closed.is_set():
                return
            self._close()

    def _close(self):
        self.closed.set()
        try:
            self.proc.stdin.close()  # EOF interrupts Windows HTTP, including a partial stream.
        except (OSError,ValueError):
            pass
        try:
            self.proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=3)
        self.proc.stdout.close()


class WindowsRelayPool:
    def __init__(self,python,max_workers=4):
        if not 1<=max_workers<=16:
            raise ValueError('invalid relay worker limit')
        self.python=python
        self.max_workers=max_workers
        self.condition=threading.Condition()
        self.workers=set()
        self.idle=[]
        self.closed=False

    def acquire(self,deadline):
        with self.condition:
            while True:
                if self.closed:
                    raise RuntimeError('Windows relay pool stopped')
                if time.monotonic()>=deadline:
                    raise TimeoutError('Windows relay pool busy')
                if self.idle:
                    return self.idle.pop()
                if len(self.workers)<self.max_workers:
                    worker=_Worker(self.python)
                    self.workers.add(worker)
                    return worker
                self.condition.wait(timeout=max(0,deadline-time.monotonic()))

    def release(self,worker,reusable):
        with self.condition:
            if worker not in self.workers:
                return
            if reusable and not self.closed and worker.proc.poll() is None:
                self.idle.append(worker)
                self.condition.notify()
                return
            # Keep the slot occupied until the abandoned HTTP process exits.
        worker.close()
        with self.condition:
            self.workers.discard(worker)
            self.condition.notify()

    def close(self):
        with self.condition:
            self.closed=True
            workers=list(self.workers)
            self.workers.clear()
            self.idle.clear()
            self.condition.notify_all()
        for worker in workers:
            worker.close()


class WindowsResponse:
    def __init__(self,python,url,payload,headers,timeout,pool=None):
        self.deadline=time.monotonic()+timeout
        self.pool=pool or WindowsRelayPool(python,1)
        self.owns_pool=pool is None
        self.worker=self.pool.acquire(self.deadline)
        self.proc=self.worker.proc
        self.closed=False
        self.eof=False
        self.failed=False
        try:
            request={'url':url,'payload':payload,'headers':headers,'timeout':max(0.1,self.deadline-time.monotonic())}
            payload_bytes=(json.dumps(request,ensure_ascii=False)+'\n').encode()
            if len(payload_bytes)>2*1024*1024:
                raise ValueError('Windows Agent request exceeds limit')
            def writer():
                try:
                    remaining=memoryview(payload_bytes)
                    while remaining and not self.worker.closed.is_set():
                        count=os.write(self.proc.stdin.fileno(),remaining)
                        remaining=remaining[count:]
                except (OSError,ValueError):
                    try:
                        self.worker.records.put_nowait({'error':'relay request write failed'})
                    except queue.Full:
                        pass
            threading.Thread(target=writer,daemon=True).start()
            first=self._record()
            if not isinstance(first.get('headers'),dict):
                raise RuntimeError('Windows Agent relay did not return headers')
            self.headers={k:v for k,v in first['headers'].items() if v}
        except Exception:
            self.failed=True
            self.close()
            raise

    def _record(self,deadline=None):
        try:
            record=self.worker.records.get(timeout=max(0,(self.deadline if deadline is None else deadline)-time.monotonic()))
        except queue.Empty:
            self.failed=True
            raise TimeoutError('Windows Agent request timed out') from None
        if 'error' in record:
            self.failed=True
            raise RuntimeError('Windows Hermes 本机 API 连接或执行失败'+(' (HTTP '+str(record['status'])+')' if record.get('status') else ''))
        return record

    def readline(self,size):
        if self.eof:
            return b''
        try:
            record=self._record()
            if record.get('eof'):
                self.eof=True
                return b''
            data=base64.b64decode(record['body'],validate=True)
            if len(data)>size:
                raise RuntimeError('Windows Agent response exceeds limit')
            return data
        except Exception:
            self.failed=True
            raise

    def read(self,size):
        return self.readline(size)

    def close(self):
        if self.closed:
            return
        self.closed=True
        if not self.failed and not self.eof:
            try:
                # A caller can stop at SSE [DONE] or the one JSON body frame.
                # Never drain arbitrary body data into another session.
                record=self._record(deadline=min(self.deadline,time.monotonic()+0.25))
                self.eof=record.get('eof') is True
            except Exception:
                self.failed=True
        self.pool.release(self.worker,self.eof and not self.failed)
        if self.owns_pool:
            self.pool.close()

    def __enter__(self): return self
    def __exit__(self,exc_type,*args):
        if exc_type:
            self.failed=True
        self.close()
