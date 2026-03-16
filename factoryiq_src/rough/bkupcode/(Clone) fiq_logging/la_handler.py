# fiq_logging/la_handler.py
import logging, datetime, hashlib, hmac, base64, json, time, random, urllib.request
from queue import Queue, Empty
from threading import Thread, Event
from fiq_logging.context import get_context

class LogAnalyticsBatchHandler(logging.Handler):
    """
    Azure Log Analytics HTTP Data Collector handler.
    - Batches records (batch_size) or flushes every flush_interval seconds.
    - Retries with exponential backoff on transient errors.
    - Timezone-aware timestamps.
    - Non-blocking (background worker thread).
    """
    def __init__(
        self,
        workspace_id: str,
        shared_key: str,
        log_type: str = "DatabricksAppLogs",
        batch_size: int = 50,
        flush_interval: float = 2.0,
        max_retries: int = 5,
        timeout_sec: int = 5,
        max_message_len: int = 16000
    ):
        super().__init__()
        self.workspace_id = workspace_id
        self.shared_key = shared_key
        self.log_type = log_type
        self.batch_size = int(batch_size)
        self.flush_interval = float(flush_interval)
        self.max_retries = int(max_retries)
        self.timeout_sec = int(timeout_sec)
        self.max_message_len = int(max_message_len)

        self._q: Queue = Queue()
        self._stop = Event()
        self._worker = Thread(target=self._run, name="LAHandlerWorker", daemon=True)
        self._worker.start()

    # ---------- HTTP signing ----------
    def _build_signature(self, date, content_length, method, content_type, resource):
        x_headers = "x-ms-date:" + date
        string_to_hash = f"{method}\n{content_length}\n{content_type}\n{x_headers}\n{resource}"
        bytes_to_hash = string_to_hash.encode("utf-8")
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = base64.b64encode(
            hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
        ).decode()
        return f"SharedKey {self.workspace_id}:{encoded_hash}"

    def _post_batch(self, records):
        if not records:
            return
        body = json.dumps(records).encode("utf-8")
        method, resource, content_type = "POST", "/api/logs", "application/json"
        date = datetime.datetime.now(datetime.UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")
        sig = self._build_signature(date, len(body), method, content_type, resource)
        url = f"https://{self.workspace_id}.ods.opinsights.azure.com{resource}?api-version=2016-04-01"
        headers = {
            "content-type": content_type,
            "Authorization": sig,
            "Log-Type": self.log_type,
            "x-ms-date": date,
        }

        attempt = 0
        while True:
            try:
                req = urllib.request.Request(url, data=body, headers=headers)
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as _:
                    return
            except Exception as e:
                attempt += 1
                if attempt > self.max_retries:
                    # Last resort: log to stderr logger; don't crash pipeline
                    logging.getLogger(__name__).warning(f"Log Analytics post failed after {attempt} attempts: {e}")
                    return
                sleep_s = min(2 ** attempt + random.uniform(0, 0.5), 30)
                time.sleep(sleep_s)

    # ---------- worker loop ----------
    def _run(self):
        buf = []
        last = time.time()
        while not self._stop.is_set():
            try:
                item = self._q.get(timeout=0.5)
                buf.append(item)
                if len(buf) >= self.batch_size:
                    self._post_batch(buf)
                    buf.clear()
                    last = time.time()
            except Empty:
                pass
            if buf and (time.time() - last >= self.flush_interval):
                self._post_batch(buf)
                buf.clear()
                last = time.time()
        if buf:
            self._post_batch(buf)

    # ---------- logging API ----------
    def emit(self, record: logging.LogRecord):
        try:
            # Truncate very long messages to protect ingestion costs
            msg = record.getMessage()
            if msg and len(msg) > self.max_message_len:
                msg = msg[: self.max_message_len - 3] + "..."

            ts = datetime.datetime.fromtimestamp(record.created, datetime.UTC).isoformat()
            ctx = get_context()  # pull fresh context so stage/robot can change mid-run

            doc = {
                "timestamp": ts,              # -> timestamp_t (datetime)
                "Level": record.levelname,    # -> Level (string)
                "Message": msg,               # -> Message (string)
                "logger": record.name,        # -> logger_s
                "module": record.module,      # -> module_s
                "funcName": record.funcName,  # -> funcName_s
                "lineno": record.lineno,      # -> lineno_d
                "process": record.process,    # -> process_d
                "thread": record.thread,      # -> thread_d
                # Context
                "robotId": ctx.get("robotId"),
                "stage": ctx.get("stage"),
                "runTs": ctx.get("runTs"),
                "notebook": ctx.get("notebook"),
                "workspaceUrl": ctx.get("workspaceUrl"),
                "clusterId": ctx.get("clusterId"),
                "jobRunId": ctx.get("jobRunId"),
            }
            if record.exc_info:
                import traceback
                doc["exception"] = "".join(traceback.format_exception(*record.exc_info))

            self._q.put(doc, block=False)
        except Exception:
            self.handleError(record)

    def close(self):
        try:
            self._stop.set()
            self._worker.join(timeout=3.0)
        finally:
            super().close()