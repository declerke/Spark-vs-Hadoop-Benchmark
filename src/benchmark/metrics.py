import time
import threading
import psutil
import os
import tracemalloc
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class BenchmarkMetrics:
    engine: str = ""
    workload: str = ""
    dataset: str = ""
    data_size_mb: float = 0.0
    wall_time_sec: float = 0.0
    cpu_time_sec: float = 0.0
    peak_memory_mb: float = 0.0
    avg_memory_mb: float = 0.0
    peak_cpu_pct: float = 0.0
    avg_cpu_pct: float = 0.0
    tracemalloc_peak_mb: float = 0.0
    output_rows: int = 0
    exit_code: int = 0
    error_msg: str = ""
    sample_fraction: float = 1.0
    notes: str = ""


class ResourceMonitor:
    def __init__(self, pid: Optional[int] = None, interval: float = 0.5):
        self.pid = pid or os.getpid()
        self.interval = interval
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self.memory_samples = []
        self.cpu_samples = []
        self._process = psutil.Process(self.pid)

    def _monitor_loop(self):
        while not self._stop_event.is_set():
            try:
                mem_info = self._process.memory_info()
                mem_mb = mem_info.rss / (1024 * 1024)
                cpu_pct = self._process.cpu_percent(interval=None)
                self.memory_samples.append(mem_mb)
                self.cpu_samples.append(cpu_pct)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            time.sleep(self.interval)

    def start(self):
        self._process.cpu_percent(interval=None)
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    @property
    def peak_memory_mb(self) -> float:
        return max(self.memory_samples, default=0.0)

    @property
    def avg_memory_mb(self) -> float:
        if not self.memory_samples:
            return 0.0
        return sum(self.memory_samples) / len(self.memory_samples)

    @property
    def peak_cpu_pct(self) -> float:
        return max(self.cpu_samples, default=0.0)

    @property
    def avg_cpu_pct(self) -> float:
        if not self.cpu_samples:
            return 0.0
        return sum(self.cpu_samples) / len(self.cpu_samples)


class BenchmarkTimer:
    def __init__(self):
        self._wall_start: float = 0.0
        self._cpu_start: float = 0.0
        self.wall_elapsed: float = 0.0
        self.cpu_elapsed: float = 0.0

    def __enter__(self):
        self._wall_start = time.perf_counter()
        self._cpu_start = time.process_time()
        return self

    def __exit__(self, *args):
        self.wall_elapsed = time.perf_counter() - self._wall_start
        self.cpu_elapsed = time.process_time() - self._cpu_start


def measure_data_size(paths) -> float:
    if isinstance(paths, str):
        paths = [paths]
    total_bytes = 0
    for p in paths:
        if os.path.isfile(p):
            total_bytes += os.path.getsize(p)
        elif os.path.isdir(p):
            for root, _, files in os.walk(p):
                for f in files:
                    total_bytes += os.path.getsize(os.path.join(root, f))
    return total_bytes / (1024 * 1024)


def collect_metrics(
    engine: str,
    workload: str,
    dataset: str,
    data_paths,
    runner_fn,
    sample_fraction: float = 1.0,
    notes: str = ""
) -> BenchmarkMetrics:
    data_size_mb = measure_data_size(data_paths)
    monitor = ResourceMonitor()
    tracemalloc.start()
    output_rows = 0
    exit_code = 0
    error_msg = ""

    monitor.start()
    with BenchmarkTimer() as timer:
        try:
            result = runner_fn()
            if isinstance(result, int):
                output_rows = result
            elif isinstance(result, dict):
                output_rows = result.get("rows", 0)
                exit_code = result.get("exit_code", 0)
        except Exception as exc:
            exit_code = 1
            error_msg = str(exc)
    monitor.stop()

    _, tm_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return BenchmarkMetrics(
        engine=engine,
        workload=workload,
        dataset=dataset,
        data_size_mb=data_size_mb * sample_fraction,
        wall_time_sec=timer.wall_elapsed,
        cpu_time_sec=timer.cpu_elapsed,
        peak_memory_mb=monitor.peak_memory_mb,
        avg_memory_mb=monitor.avg_memory_mb,
        peak_cpu_pct=monitor.peak_cpu_pct,
        avg_cpu_pct=monitor.avg_cpu_pct,
        tracemalloc_peak_mb=tm_peak / (1024 * 1024),
        output_rows=output_rows,
        exit_code=exit_code,
        error_msg=error_msg,
        sample_fraction=sample_fraction,
        notes=notes
    )


def metrics_to_dict(m: BenchmarkMetrics) -> dict:
    return asdict(m)