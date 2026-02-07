from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, computed_field


class Priority(StrEnum):
    HIGH = "high"
    LOW = "low"


class TaskStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"


class TaskRecord(BaseModel):
    task_id: str
    priority: Priority
    status: TaskStatus
    scheduled_by: str
    scheduled_at: datetime
    processed_by: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    latency_ms: float | None = None

    def to_redis(self) -> dict[str, str]:
        return self.model_dump(mode="json", exclude_none=True)

    # def to_redis(self) -> dict[str, str]:
    #     return {
    #         k: str(v)
    #         for k, v in self.model_dump(mode="json", exclude_none=True).items()
    #     }


class ScheduleResponse(BaseModel):
    task_id: str
    status: TaskStatus
    priority: Priority
    worker: str
    scheduled_at: datetime


class TaskStatusResponse(TaskRecord):
    @computed_field
    @property
    def same_worker(self) -> bool | None:
        if self.processed_by:
            return self.scheduled_by == self.processed_by
        return None
