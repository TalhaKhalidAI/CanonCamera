"""
Production-ready Async MinIO Pipeline with Queue-based Worker Architecture.

Architecture:
    Producer(s) -> UploadQueue -> UploadWorker(s) -> MinIO
    Producer(s) -> DownloadQueue -> DownloadWorker(s) -> Local/Stream

Features:
    - Bounded queues with backpressure
    - Configurable worker pool size
    - Retry with exponential backoff
    - Progress tracking and metrics
    - Graceful shutdown with drain
    - Connection pooling via miniopy-async
    - Structured logging with correlation IDs
    - Type-safe throughout

Dependencies:
    pip install miniopy-async aiofiles
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import logging
import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    Protocol,
    Set,
    TypeVar,
)

from miniopy_async import Minio
from miniopy_async.commonconfig import Tags
from miniopy_async.error import S3Error

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Context & Configuration
# ---------------------------------------------------------------------------

correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="unknown")

T = TypeVar("T")


def _log_extra(**kwargs: Any) -> dict[str, Any]:
    """Build logging extra dict with correlation ID."""
    return {"correlation_id": correlation_id_var.get(), **kwargs}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MinioConfig:
    """MinIO client configuration."""

    endpoint: str
    access_key: str
    secret_key: str
    secure: bool = True
    region: str = "us-east-1"
    # Connection pool settings
    max_connections: int = 25
    timeout: float = 30.0


@dataclass(frozen=True)
class PipelineConfig:
    """Pipeline queue and worker configuration."""

    # Queue bounds (backpressure)
    upload_queue_size: int = 100
    download_queue_size: int = 100
    delete_queue_size: int = 50

    # Worker counts
    upload_workers: int = 4
    download_workers: int = 4
    delete_workers: int = 2

    # Retry settings
    max_retries: int = 3
    base_delay: float = 0.5
    max_delay: float = 10.0

    # Batch settings
    max_batch_size: int = 50
    batch_flush_interval: float = 5.0

    # Graceful shutdown
    shutdown_timeout: float = 30.0
    drain_timeout: float = 60.0


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------

class OperationType(Enum):
    UPLOAD = auto()
    DOWNLOAD = auto()
    DELETE = auto()
    COPY = auto()
    LIST = auto()


class TaskStatus(Enum):
    PENDING = auto()
    PROCESSING = auto()
    COMPLETED = auto()
    FAILED = auto()
    RETRYING = auto()


@dataclass
class MinioTask:
    """Base task for pipeline operations."""

    task_id: str
    operation: OperationType
    bucket: str
    object_name: str
    status: TaskStatus = field(default=TaskStatus.PENDING)
    attempts: int = 0
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    metadata: Dict[str, str] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    content_type: Optional[str] = None

    # Upload-specific
    file_path: Optional[Path] = None
    data: Optional[bytes] = None
    data_generator: Optional[Callable[[], AsyncIterator[bytes]]] = None
    file_size: Optional[int] = None

    # Download-specific
    destination_path: Optional[Path] = None
    stream_callback: Optional[Callable[[bytes], Coroutine[Any, Any, None]]] = None

    # Copy-specific
    source_bucket: Optional[str] = None
    source_object: Optional[str] = None

    def duration_ms(self) -> Optional[float]:
        if self.completed_at:
            return (self.completed_at - self.created_at) * 1000
        return None


@dataclass
class PipelineMetrics:
    """Real-time pipeline metrics."""

    tasks_submitted: int = 0
    tasks_completed: int = 0
    tasks_failed: int = 0
    tasks_retried: int = 0
    bytes_uploaded: int = 0
    bytes_downloaded: int = 0
    objects_deleted: int = 0
    active_workers: int = 0
    queue_depth: int = 0
    avg_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    _latencies: List[float] = field(default_factory=list)

    def record_latency(self, ms: float) -> None:
        self._latencies.append(ms)
        if len(self._latencies) > 1000:
            self._latencies = self._latencies[-1000:]
        self.avg_latency_ms = sum(self._latencies) / len(self._latencies)
        sorted_lat = sorted(self._latencies)
        p99_idx = int(len(sorted_lat) * 0.99)
        self.p99_latency_ms = sorted_lat[p99_idx] if sorted_lat else 0.0


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class MinioPipelineError(Exception):
    """Base pipeline exception."""
    pass


class BucketNotFoundError(MinioPipelineError):
    pass


class ObjectNotFoundError(MinioPipelineError):
    pass


class UploadFailedError(MinioPipelineError):
    pass


class DownloadFailedError(MinioPipelineError):
    pass


class QuotaExceededError(MinioPipelineError):
    pass


# ---------------------------------------------------------------------------
# Retry Decorator
# ---------------------------------------------------------------------------

def retry_on_s3_error(
    max_retries: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 10.0,
    retryable_errors: Optional[Set[str]] = None,
) -> Callable[[Callable[..., Coroutine[Any, Any, T]]], Callable[..., Coroutine[Any, Any, T]]]:
    """
    Retry decorator for S3/MinIO operations.

    Retries on transient errors: network issues, 5xx, throttling.
    Does NOT retry on 4xx client errors (except 429 Too Many Requests).
    """
    _default_retryable = {
        "RequestTimeout",
        "InternalError",
        "ServiceUnavailable",
        "SlowDown",
        "OperationAborted",
        "NetworkError",
        "ConnectionError",
        "TimeoutError",
    }
    retryable = retryable_errors or _default_retryable

    def decorator(
        func: Callable[..., Coroutine[Any, Any, T]]
    ) -> Callable[..., Coroutine[Any, Any, T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exc: Optional[Exception] = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except S3Error as e:
                    last_exc = e
                    error_code = getattr(e, "code", "Unknown")
                    if error_code not in retryable and error_code != "TooManyRequests":
                        raise
                    if attempt < max_retries - 1:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(
                            "S3 error %s on %s (attempt %d/%d), retrying in %.2fs",
                            error_code,
                            func.__name__,
                            attempt + 1,
                            max_retries,
                            delay,
                            extra=_log_extra(),
                        )
                        await asyncio.sleep(delay)
                except (ConnectionError, TimeoutError, OSError) as e:
                    last_exc = e
                    if attempt < max_retries - 1:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(
                            "Network error on %s (attempt %d/%d), retrying in %.2fs: %s",
                            func.__name__,
                            attempt + 1,
                            max_retries,
                            delay,
                            e,
                            extra=_log_extra(),
                        )
                        await asyncio.sleep(delay)
            raise last_exc  # type: ignore[misc]
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# MinIO Client Manager
# ---------------------------------------------------------------------------

class MinioClientManager:
    """
    Lifecycle manager for async MinIO client.

    Handles connection pooling, health checks, and graceful cleanup.
    """

    def __init__(self, config: MinioConfig):
        self.config = config
        self._client: Optional[Minio] = None
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> Minio:
        await self.connect()
        return self._client  # type: ignore[return-value]

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def connect(self) -> Minio:
        async with self._lock:
            if self._client is not None:
                return self._client

            self._client = Minio(
                self.config.endpoint,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                secure=self.config.secure,
                region=self.config.region,
            )
            logger.info(
                "MinIO client connected to %s (region=%s)",
                self.config.endpoint,
                self.config.region,
                extra=_log_extra(),
            )
            return self._client

    async def close(self) -> None:
        async with self._lock:
            if self._client is not None:
                await self._client.close_session()
                self._client = None
                logger.info("MinIO client disconnected", extra=_log_extra())

    @property
    def client(self) -> Minio:
        if self._client is None:
            raise MinioPipelineError("MinIO client not connected. Use 'async with' or call connect()")
        return self._client

    @retry_on_s3_error(max_retries=3)
    async def ensure_bucket(self, bucket: str) -> None:
        """Ensure bucket exists, create if not."""
        if not await self.client.bucket_exists(bucket):
            await self.client.make_bucket(bucket)
            logger.info("Created bucket: %s", bucket, extra=_log_extra())

    @retry_on_s3_error(max_retries=3)
    async def get_object_info(self, bucket: str, object_name: str) -> Optional[Dict[str, Any]]:
        """Get object metadata without downloading."""
        try:
            stat = await self.client.stat_object(bucket, object_name)
            return {
                "size": stat.size,
                "etag": stat.etag,
                "content_type": stat.content_type,
                "last_modified": stat.last_modified,
                "metadata": stat.metadata,
            }
        except S3Error as e:
            if e.code == "NoSuchKey":
                return None
            raise


# ---------------------------------------------------------------------------
# Pipeline Core
# ---------------------------------------------------------------------------

class MinioPipeline:
    """
    Async queue-based MinIO pipeline with worker pools.

    Usage:
        config = MinioConfig(endpoint="localhost:9000", ...)
        pipeline = MinioPipeline(config)

        async with pipeline:
            # Submit upload tasks
            task = MinioTask(
                task_id="uuid",
                operation=OperationType.UPLOAD,
                bucket="my-bucket",
                object_name="path/to/file.jpg",
                file_path=Path("/local/file.jpg"),
            )
            await pipeline.submit_upload(task)

            # Submit download tasks
            download_task = MinioTask(
                task_id="uuid2",
                operation=OperationType.DOWNLOAD,
                bucket="my-bucket",
                object_name="path/to/file.jpg",
                destination_path=Path("/local/downloads/file.jpg"),
            )
            await pipeline.submit_download(download_task)

            # Graceful shutdown
            await pipeline.drain()
    """

    def __init__(
        self,
        minio_config: MinioConfig,
        pipeline_config: Optional[PipelineConfig] = None,
    ):
        self.minio_config = minio_config
        self.config = pipeline_config or PipelineConfig()
        self.client_mgr = MinioClientManager(minio_config)

        # Queues (bounded for backpressure)
        self._upload_queue: asyncio.Queue[MinioTask] = asyncio.Queue(
            maxsize=self.config.upload_queue_size
        )
        self._download_queue: asyncio.Queue[MinioTask] = asyncio.Queue(
            maxsize=self.config.download_queue_size
        )
        self._delete_queue: asyncio.Queue[MinioTask] = asyncio.Queue(
            maxsize=self.config.delete_queue_size
        )

        # Metrics
        self.metrics = PipelineMetrics()
        self._metrics_lock = asyncio.Lock()

        # Worker task references
        self._workers: List[asyncio.Task[None]] = []
        self._shutdown_event = asyncio.Event()
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def __aenter__(self) -> MinioPipeline:
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    async def start(self) -> None:
        """Start the pipeline and all worker pools."""
        if self._running:
            return

        await self.client_mgr.connect()
        self._running = True
        self._shutdown_event.clear()

        # Start upload workers
        for i in range(self.config.upload_workers):
            task = asyncio.create_task(
                self._upload_worker(f"upload-{i}"),
                name=f"minio-upload-worker-{i}",
            )
            self._workers.append(task)

        # Start download workers
        for i in range(self.config.download_workers):
            task = asyncio.create_task(
                self._download_worker(f"download-{i}"),
                name=f"minio-download-worker-{i}",
            )
            self._workers.append(task)

        # Start delete workers
        for i in range(self.config.delete_workers):
            task = asyncio.create_task(
                self._delete_worker(f"delete-{i}"),
                name=f"minio-delete-worker-{i}",
            )
            self._workers.append(task)

        logger.info(
            "Pipeline started: %d upload, %d download, %d delete workers",
            self.config.upload_workers,
            self.config.download_workers,
            self.config.delete_workers,
            extra=_log_extra(),
        )

    async def stop(self) -> None:
        """Stop the pipeline gracefully."""
        if not self._running:
            return

        self._running = False
        self._shutdown_event.set()

        # Cancel all workers
        for worker in self._workers:
            worker.cancel()

        # Wait for graceful shutdown
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._workers, return_exceptions=True),
                timeout=self.config.shutdown_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Pipeline shutdown timed out after %.1fs",
                self.config.shutdown_timeout,
                extra=_log_extra(),
            )

        self._workers.clear()
        await self.client_mgr.close()

        logger.info("Pipeline stopped", extra=_log_extra())

    async def drain(self) -> None:
        """Wait for all queued tasks to complete before stopping."""
        logger.info(
            "Draining pipeline: upload=%d, download=%d, delete=%d",
            self._upload_queue.qsize(),
            self._download_queue.qsize(),
            self._delete_queue.qsize(),
            extra=_log_extra(),
        )

        # Wait for queues to empty
        await asyncio.wait_for(
            asyncio.gather(
                self._upload_queue.join(),
                self._download_queue.join(),
                self._delete_queue.join(),
            ),
            timeout=self.config.drain_timeout,
        )

        await self.stop()

    # ------------------------------------------------------------------
    # Task Submission
    # ------------------------------------------------------------------

    async def submit_upload(self, task: MinioTask) -> None:
        """Submit an upload task. Blocks if queue is full (backpressure)."""
        task.operation = OperationType.UPLOAD
        await self._upload_queue.put(task)
        async with self._metrics_lock:
            self.metrics.tasks_submitted += 1
            self.metrics.queue_depth = self._total_queue_depth()

    async def submit_download(self, task: MinioTask) -> None:
        """Submit a download task. Blocks if queue is full."""
        task.operation = OperationType.DOWNLOAD
        await self._download_queue.put(task)
        async with self._metrics_lock:
            self.metrics.tasks_submitted += 1
            self.metrics.queue_depth = self._total_queue_depth()

    async def submit_delete(self, task: MinioTask) -> None:
        """Submit a delete task. Blocks if queue is full."""
        task.operation = OperationType.DELETE
        await self._delete_queue.put(task)
        async with self._metrics_lock:
            self.metrics.tasks_submitted += 1
            self.metrics.queue_depth = self._total_queue_depth()

    def _total_queue_depth(self) -> int:
        return (
            self._upload_queue.qsize()
            + self._download_queue.qsize()
            + self._delete_queue.qsize()
        )

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    async def _upload_worker(self, worker_id: str) -> None:
        """Upload worker: processes tasks from upload queue."""
        while not self._shutdown_event.is_set():
            try:
                task = await asyncio.wait_for(
                    self._upload_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            try:
                await self._process_upload(task)
            except Exception as e:
                logger.exception(
                    "Upload worker %s failed on task %s: %s",
                    worker_id,
                    task.task_id,
                    e,
                    extra=_log_extra(task_id=task.task_id),
                )
                task.status = TaskStatus.FAILED
                task.error = str(e)
            finally:
                self._upload_queue.task_done()
                task.completed_at = time.time()
                if task.duration_ms():
                    async with self._metrics_lock:
                        self.metrics.record_latency(task.duration_ms())  # type: ignore[arg-type]

    async def _download_worker(self, worker_id: str) -> None:
        """Download worker: processes tasks from download queue."""
        while not self._shutdown_event.is_set():
            try:
                task = await asyncio.wait_for(
                    self._download_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            try:
                await self._process_download(task)
            except Exception as e:
                logger.exception(
                    "Download worker %s failed on task %s: %s",
                    worker_id,
                    task.task_id,
                    e,
                    extra=_log_extra(task_id=task.task_id),
                )
                task.status = TaskStatus.FAILED
                task.error = str(e)
            finally:
                self._download_queue.task_done()
                task.completed_at = time.time()
                if task.duration_ms():
                    async with self._metrics_lock:
                        self.metrics.record_latency(task.duration_ms())  # type: ignore[arg-type]

    async def _delete_worker(self, worker_id: str) -> None:
        """Delete worker: processes tasks from delete queue."""
        while not self._shutdown_event.is_set():
            try:
                task = await asyncio.wait_for(
                    self._delete_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            try:
                await self._process_delete(task)
            except Exception as e:
                logger.exception(
                    "Delete worker %s failed on task %s: %s",
                    worker_id,
                    task.task_id,
                    e,
                    extra=_log_extra(task_id=task.task_id),
                )
                task.status = TaskStatus.FAILED
                task.error = str(e)
            finally:
                self._delete_queue.task_done()
                task.completed_at = time.time()

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------

    @retry_on_s3_error(max_retries=3)
    async def _process_upload(self, task: MinioTask) -> None:
        """Process a single upload task."""
        task.status = TaskStatus.PROCESSING
        task.attempts += 1

        client = self.client_mgr.client

        # Build metadata
        metadata = task.metadata.copy()
        if task.content_type:
            metadata["Content-Type"] = task.content_type

        # Upload from file path
        if task.file_path:
            await client.fput_object(
                task.bucket,
                task.object_name,
                str(task.file_path),
                content_type=task.content_type or "application/octet-stream",
                metadata=metadata,
                tags=Tags(for_storage=True, **task.tags) if task.tags else None,
            )
            task.file_size = task.file_path.stat().st_size

        # Upload from bytes
        elif task.data is not None:
            from io import BytesIO

            await client.put_object(
                task.bucket,
                task.object_name,
                BytesIO(task.data),
                length=len(task.data),
                content_type=task.content_type or "application/octet-stream",
                metadata=metadata,
                tags=Tags(for_storage=True, **task.tags) if task.tags else None,
            )
            task.file_size = len(task.data)

        # Upload from async generator
        elif task.data_generator:
            # Collect into temp buffer for length calculation
            # For large streams, use multipart upload instead
            chunks: List[bytes] = []
            async for chunk in task.data_generator():
                chunks.append(chunk)
            data = b"".join(chunks)
            from io import BytesIO

            await client.put_object(
                task.bucket,
                task.object_name,
                BytesIO(data),
                length=len(data),
                content_type=task.content_type or "application/octet-stream",
                metadata=metadata,
            )
            task.file_size = len(data)

        else:
            raise UploadFailedError(f"Task {task.task_id}: no data source provided")

        task.status = TaskStatus.COMPLETED
        async with self._metrics_lock:
            self.metrics.tasks_completed += 1
            if task.file_size:
                self.metrics.bytes_uploaded += task.file_size

        logger.info(
            "Uploaded %s/%s (%d bytes)",
            task.bucket,
            task.object_name,
            task.file_size or 0,
            extra=_log_extra(task_id=task.task_id),
        )

    @retry_on_s3_error(max_retries=3)
    async def _process_download(self, task: MinioTask) -> None:
        """Process a single download task."""
        task.status = TaskStatus.PROCESSING
        task.attempts += 1

        client = self.client_mgr.client

        # Download to file
        if task.destination_path:
            task.destination_path.parent.mkdir(parents=True, exist_ok=True)
            await client.fget_object(
                task.bucket, task.object_name, str(task.destination_path)
            )
            downloaded_size = task.destination_path.stat().st_size

        # Download to stream callback
        elif task.stream_callback:
            response = await client.get_object(task.bucket, task.object_name)
            downloaded_size = 0
            try:
                async for chunk in response.content.iter_chunked(64 * 1024):
                    await task.stream_callback(chunk)
                    downloaded_size += len(chunk)
            finally:
                await response.close()

        else:
            raise DownloadFailedError(
                f"Task {task.task_id}: no destination provided"
            )

        task.status = TaskStatus.COMPLETED
        async with self._metrics_lock:
            self.metrics.tasks_completed += 1
            self.metrics.bytes_downloaded += downloaded_size

        logger.info(
            "Downloaded %s/%s (%d bytes)",
            task.bucket,
            task.object_name,
            downloaded_size,
            extra=_log_extra(task_id=task.task_id),
        )

    @retry_on_s3_error(max_retries=3)
    async def _process_delete(self, task: MinioTask) -> None:
        """Process a single delete task."""
        task.status = TaskStatus.PROCESSING
        task.attempts += 1

        client = self.client_mgr.client
        await client.remove_object(task.bucket, task.object_name)

        task.status = TaskStatus.COMPLETED
        async with self._metrics_lock:
            self.metrics.tasks_completed += 1
            self.metrics.objects_deleted += 1

        logger.info(
            "Deleted %s/%s",
            task.bucket,
            task.object_name,
            extra=_log_extra(task_id=task.task_id),
        )

    # ------------------------------------------------------------------
    # Batch Operations
    # ------------------------------------------------------------------

    async def upload_batch(
        self,
        tasks: List[MinioTask],
        *,
        concurrency: Optional[int] = None,
    ) -> List[MinioTask]:
        """
        Upload a batch of objects with controlled concurrency.

        Uses gather with semaphore instead of queue for lower latency
        on known-size batches.
        """
        sem = asyncio.Semaphore(concurrency or self.config.upload_workers)

        async def _upload_one(task: MinioTask) -> MinioTask:
            async with sem:
                await self._process_upload(task)
                return task

        results = await asyncio.gather(
            *[_upload_one(t) for t in tasks], return_exceptions=True
        )

        for task, result in zip(tasks, results):
            if isinstance(result, Exception):
                task.status = TaskStatus.FAILED
                task.error = str(result)

        return tasks

    async def delete_batch(
        self,
        bucket: str,
        object_names: List[str],
    ) -> Dict[str, bool]:
        """
        Delete multiple objects efficiently.

        Uses remove_objects for bulk deletion.
        """
        client = self.client_mgr.client
        from miniopy_async.deleteobjects import DeleteObject

        delete_objects = [DeleteObject(name) for name in object_names]
        errors = await client.remove_objects(bucket, delete_objects)

        results: Dict[str, bool] = {name: True for name in object_names}
        for error in errors:
            results[error.name] = False
            logger.error(
                "Failed to delete %s/%s: %s",
                bucket,
                error.name,
                error.code,
                extra=_log_extra(),
            )

        async with self._metrics_lock:
            self.metrics.objects_deleted += sum(1 for v in results.values() if v)

        return results

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @retry_on_s3_error(max_retries=3)
    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        recursive: bool = True,
    ) -> AsyncIterator[Dict[str, Any]]:
        """List objects in bucket with optional prefix."""
        client = self.client_mgr.client
        objects = await client.list_objects(
            bucket, prefix=prefix, recursive=recursive
        )
        async for obj in objects:
            yield {
                "name": obj.object_name,
                "size": obj.size,
                "etag": obj.etag,
                "last_modified": obj.last_modified,
            }

    @retry_on_s3_error(max_retries=3)
    async def generate_presigned_url(
        self,
        bucket: str,
        object_name: str,
        expiry: int = 3600,
        method: str = "GET",
    ) -> str:
        """Generate presigned URL for object access."""
        client = self.client_mgr.client
        if method.upper() == "GET":
            return await client.presigned_get_object(bucket, object_name, expiry)
        elif method.upper() == "PUT":
            return await client.presigned_put_object(bucket, object_name, expiry)
        else:
            raise ValueError(f"Unsupported method: {method}")

    @retry_on_s3_error(max_retries=3)
    async def copy_object(
        self,
        source_bucket: str,
        source_object: str,
        dest_bucket: str,
        dest_object: str,
    ) -> None:
        """Server-side copy between buckets."""
        client = self.client_mgr.client
        await client.copy_object(
            dest_bucket,
            dest_object,
            f"{source_bucket}/{source_object}",
        )
        logger.info(
            "Copied %s/%s -> %s/%s",
            source_bucket,
            source_object,
            dest_bucket,
            dest_object,
            extra=_log_extra(),
        )

    def get_metrics(self) -> PipelineMetrics:
        """Get current pipeline metrics snapshot."""
        return self.metrics


# ---------------------------------------------------------------------------
# Higher-Level API: File Watcher / Directory Sync
# ---------------------------------------------------------------------------

class DirectorySync:
    """
    Sync a local directory to MinIO bucket with pipeline.

    Usage:
        sync = DirectorySync(pipeline, local_dir="/data", bucket="backups")
        await sync.sync_up(delete_extraneous=False)
    """

    def __init__(
        self,
        pipeline: MinioPipeline,
        local_dir: Path | str,
        bucket: str,
        prefix: str = "",
    ):
        self.pipeline = pipeline
        self.local_dir = Path(local_dir)
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")

    async def sync_up(
        self,
        *,
        delete_extraneous: bool = False,
        pattern: str = "*",
    ) -> Dict[str, int]:
        """Upload all files matching pattern to bucket."""
        files = list(self.local_dir.rglob(pattern))
        stats = {"uploaded": 0, "skipped": 0, "failed": 0}

        for file_path in files:
            if not file_path.is_file():
                continue

            relative = file_path.relative_to(self.local_dir)
            object_name = f"{self.prefix}/{relative}".lstrip("/") if self.prefix else str(relative)

            # Check if already exists with same size
            info = await self.pipeline.client_mgr.get_object_info(
                self.bucket, object_name
            )
            if info and info["size"] == file_path.stat().st_size:
                stats["skipped"] += 1
                continue

            task = MinioTask(
                task_id=hashlib.sha256(str(file_path).encode()).hexdigest()[:16],
                operation=OperationType.UPLOAD,
                bucket=self.bucket,
                object_name=object_name,
                file_path=file_path,
                content_type=self._guess_content_type(file_path),
            )
            await self.pipeline.submit_upload(task)
            stats["uploaded"] += 1

        if delete_extraneous:
            # TODO: List remote objects and delete those not in local set
            pass

        return stats

    @staticmethod
    def _guess_content_type(path: Path) -> str:
        import mimetypes

        ctype, _ = mimetypes.guess_type(str(path))
        return ctype or "application/octet-stream"


 
 