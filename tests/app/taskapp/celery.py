from celery import Celery
from kombu import Exchange, Queue
import os


celery_app = Celery(
    "worker",
    broker=os.environ["CELERY_BROKER_URL"],
    backend=None,
    broker_heartbeat=0,
)


celery_app.conf.task_queues = (
    Queue("default", Exchange("default"), routing_key="default"),
)

celery_app.conf.update(
    worker_prefetch_multiplier=1,
    result_rexpires=3600,
    task_time_limit=600,
    task_soft_time_limit=540,
    task_default_queue="default",
    task_default_exchange_type="default",
    task_default_routing_key="default",
    task_ignore_result=True,
    accept_content=["json"],
    timezone="UTC",
    broker_heartbeat=None,
    worker_send_task_events=False,
    event_queue_expires=120,
    task_serializer="json",
    imports=(
        "tasks",
    )
)
