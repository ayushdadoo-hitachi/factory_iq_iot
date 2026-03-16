# ---- BEGIN: Log Analytics handler (hardcoded credentials) ----

import logging, datetime, hashlib, hmac, base64, json, os, urllib.request

# >>>>>> Replace with YOUR values
LA_WORKSPACE_ID = "c8e73781-d27f-49ed-86e4-4df8f61b7010"
LA_SHARED_KEY   = "HCQSSAgJ+Hzab7+Xm+cV1uTfOCNmZ2zLQ7clZ54CoxBI1waMoRNlXcRdP+VmFad6LyrIzlOgD6gkYcg6G06GwQ=="
LA_LOG_TYPE     = "DatabricksAppLogs"   # Creates table DatabricksAppLogs_CL
LA_LEVEL        = "INFO"                # Capture INFO and above


class _LAHandler(logging.Handler):
    """Minimal Log Analytics Python logging handler."""
    def __init__(self, workspace_id, shared_key, log_type="DatabricksAppLogs", timeout_sec=5):
        super().__init__()
        self.workspace_id = workspace_id
        self.shared_key = shared_key
        self.log_type = log_type
        self.timeout_sec = timeout_sec

    def _build_signature(self, date, content_length, method, content_type, resource):
        x_headers = 'x-ms-date:' + date
        string_to_hash = f"{method}\n{content_length}\n{content_type}\n{x_headers}\n{resource}"
        bytes_to_hash = string_to_hash.encode("utf-8")
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = base64.b64encode(
            hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
        ).decode()
        return f"SharedKey {self.workspace_id}:{encoded_hash}"

    def _post(self, records):
        body = json.dumps(records).encode("utf-8")
        method, resource, content_type = "POST", "/api/logs", "application/json"
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

    def emit(self, record):
        try:
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
                # Context
                "robotId": os.getenv("ROBOT_ID"),
                "notebook": os.getenv("DATABRICKS_NOTEBOOK_PATH"),
                "workspaceUrl": os.getenv("DATABRICKS_WORKSPACE_URL"),
            }
            if record.exc_info:
                import traceback
                doc["exception"] = "".join(traceback.format_exception(*record.exc_info))
            self._post([doc])
        except Exception:
            self.handleError(record)


# Attach handler ONCE
_root = logging.getLogger()
if not any(isinstance(h, _LAHandler) for h in _root.handlers):
    _la = _LAHandler(LA_WORKSPACE_ID, LA_SHARED_KEY, log_type=LA_LOG_TYPE)
    _la.setLevel(LA_LEVEL)
    _root.addHandler(_la)

# ---- END: Log Analytics handler ----