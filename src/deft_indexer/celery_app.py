from celery import Celery
from celery.signals import worker_process_init

from .config import get_settings

settings = get_settings()


@worker_process_init.connect
def init_worker_metrics(**kwargs):
    """Start Prometheus HTTP server in each worker process."""
    try:
        import structlog
        from prometheus_client import start_http_server

        log = structlog.get_logger(__name__)
        start_http_server(settings.metrics_port)
        log.info("worker_metrics_server_started", port=settings.metrics_port)
    except OSError as e:
        # Port already in use - likely another worker or API process
        # This is fine, metrics will be available on whichever process started first
        pass


celery_app = Celery(
    "deft-indexer",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["deft_indexer.tasks"],
)

celery_app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_time_limit=300,
    beat_schedule={
        "poll-contracts": {
            "task": "deft_indexer.tasks.enqueue_active_contracts",
            "schedule": 30.0,
        },
        "verify-canonical": {
            "task": "deft_indexer.tasks.verify_recent_canonical",
            "schedule": 15.0,
            "args": (1,),  # chain_id=1 (Ethereum mainnet)
        },
    },
)

# Set as default app so @shared_task uses this broker
celery_app.set_default()
