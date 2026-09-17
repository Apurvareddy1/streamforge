from prometheus_client import Counter, Gauge, start_http_server
import time


# Total number of events processed by the worker
events_processed = Counter(
    "streamforge_events_processed_total",
    "Total number of events processed"
)

# Current processing rate
events_per_second = Gauge(
    "streamforge_events_per_second",
    "Events processed per second"
)

# Current processing lag
processing_lag = Gauge(
    "streamforge_processing_lag_seconds",
    "Processing lag in seconds"
)

# Worker health status
worker_status = Gauge(
    "streamforge_worker_status",
    "Worker status: 1 = running, 0 = stopped"
)


# Store the previous count and time
_previous_count = 0
_previous_time = time.time()


def start_metrics_server(port=8000):
    """Start the Prometheus metrics HTTP server."""
    start_http_server(port)
    worker_status.set(1)
    print(f"Prometheus metrics server started on port {port}")


def record_event(event_timestamp=None):
    """Record one processed event and update metrics."""
    global _previous_count, _previous_time

    events_processed.inc()

    current_time = time.time()
    current_count = events_processed._value.get()

    elapsed_time = current_time - _previous_time

    if elapsed_time > 0:
        rate = (current_count - _previous_count) / elapsed_time
        events_per_second.set(rate)

    _previous_count = current_count
    _previous_time = current_time

    if event_timestamp is not None:
        lag = current_time - event_timestamp
        if lag >= 0:
            processing_lag.set(lag)


def set_worker_status(status):
    """Update worker health status."""
    worker_status.set(status)