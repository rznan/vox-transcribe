from enum import Enum

# ==========================
# Enums
# ==========================


class TaskStatus(str, Enum):
    SUBMITTED = "SUBMITTED"
    QUEUED = "QUEUED"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    REQUEUED = "REQUEUED"


class TaskAttemptStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"


class WorkerStatus(str, Enum):
    IDLE = "IDLE"
    BUSY = "BUSY"
    OFFLINE = "OFFLINE"


class WorkerRuntime(str, Enum):
    CPU = "CPU"
    GPU = "GPU"
