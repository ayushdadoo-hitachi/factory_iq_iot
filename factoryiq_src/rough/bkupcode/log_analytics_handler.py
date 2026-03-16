# core/log_analytics_handler.py
import logging
import datetime
import hashlib
import hmac
import base64
import json
import os
import urllib.request

class LogAnalyticsHandler(logging.Handler):
    """
    Sends Python logging records to Azure Log Analytics via HTTP Data Collector API.
    Creates/uses a custom table named <Log-Type>_CL (default: DatabricksAppLogs_CL).
    """

    def __init__(self, workspace_id: str, shared_key: str, log_type: str = "DatabricksAppLogs", timeout_sec: int = 5):
        super().__init__()
        self.workspace_id = workspace_id
        self.shared_key = shared_key
        self.log_type = log_type
        self.timeout_sec = timeout_sec

    def _build_signature(self, date: str, content_length: int, method: str, content_type: str, resource: str) -> str:
        x_headers = 'x-ms-date:' + date
        string_to_hash = f"{method}\n{content_length}\n{content_type}\n{x_headers}\n{resource}"
        bytes_to_hash = string_to_hash.encode("utf-8")
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode()
        return f"SharedKey {self.workspace_id}:{encoded_hash}"

    def _post_batch(self, batch):
        body = json.dumps(batch).encode("utf-8")
        method = "POST"
        content_type = "application/json"
        resource = "/api/logs"
        date = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
        sig = self._build_signature(date, len(body), method, content_type, resource)
        url = f"https://{self.workspace_id}.ods.opinsights.azure.com{resource}?api-version=2016-04-01"
        headers = {
            "content-type": content_type,
            "Authorization": sig,
            "Log-Type": self.log_type,
            "x-ms-date": date
        }
        req = urllib.request.Request(url, data=body, headers=headers)
        with urllib.request.urlopen(req, timeout=self.timeout_sec) as _:
            pass

    def emit(self, record: logging.LogRecord):
        # Convert a single record to JSON and send immediately (simple, reliable).
        try:
            doc = self._format_record(record)
            self._post_batch([doc])
        except Exception:
            # Prevent logging failures from crashing app or recursive logging
            self.handleError(record)

    def _format_record(self, record: logging.LogRecord) -> dict:
        # Structure fields; you can add/remove contextual fields as needed
        doc = {
            "timestamp": datetime.datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
            "process": record.process,
            "thread": record.thread,
            # Useful Databricks context (if available)
            "notebook": os.getenv("DATABRICKS_NOTEBOOK_PATH"),
            "workspaceUrl": os.getenv("DATABRICKS_WORKSPACE_URL"),
            "robotId": os.getenv("ROBOT_ID"),
            # Your pipeline context keys can be added here as well
        }
        if record.exc_info:
            import traceback
            doc["exception"] = "".join(traceback.format_exception(*record.exc_info))
        return doc