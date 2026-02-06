import contextvars
import logging

ctx_worker = contextvars.ContextVar("worker", default="-")
ctx_service = contextvars.ContextVar("service", default="MAIN")
ctx_task_id = contextvars.ContextVar("task_id", default="-")


class ContextFormatter(logging.Formatter):
    def format(self, record):
        record.worker = ctx_worker.get()
        record.service = ctx_service.get()
        record.task_id = ctx_task_id.get()
        return super().format(record)


def setup(worker_name: str, level: int = logging.INFO):
    ctx_worker.set(worker_name)

    formatter = ContextFormatter("[%(worker)s] [%(service)s] [%(task_id)s] %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger("fastdis")
    logger.addHandler(handler)
    logger.setLevel(level)
