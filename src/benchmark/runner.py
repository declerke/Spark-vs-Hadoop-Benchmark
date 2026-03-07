import sys
import os
import csv
import subprocess
import shutil
import tempfile
import logging
from typing import List, Dict, Optional
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from benchmark.metrics import collect_metrics, BenchmarkMetrics, metrics_to_dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

SAMPLE_FRACTIONS = [0.1, 0.25, 0.5, 1.0]

BASE_DIR = os.environ.get("BENCHMARK_BASE_DIR", "/benchmark")
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
SRC_DIR = os.path.join(BASE_DIR, "src")

PAYSIM_FILE = os.path.join(DATA_DIR, "paysim_transactions.csv")
BANKSIM_TX_FILE = os.path.join(DATA_DIR, "banksim_transactions.csv")
BANKSIM_NET_FILE = os.path.join(DATA_DIR, "banksim_network.csv")

HADOOP_STREAMING_JAR = "/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
HDFS_INPUT_BASE = "/benchmark_input"
HDFS_OUTPUT_BASE = "/benchmark_output"

def get_env():
    env = os.environ.copy()
    env["JAVA_HOME"] = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
    env["HADOOP_HOME"] = os.environ.get("HADOOP_HOME", "/opt/hadoop-3.2.1")
    env["PATH"] = f"{env['HADOOP_HOME']}/bin:{env['JAVA_HOME']}/bin:{env['PATH']}"
    return env

def sample_csv(input_path: str, fraction: float, output_path: str):
    if fraction >= 1.0:
        shutil.copy(input_path, output_path)
        return

    import random
    random.seed(42)
    with open(input_path, "r", encoding="utf-8") as fin, \
         open(output_path, "w", encoding="utf-8") as fout:
        header = fin.readline()
        fout.write(header)
        for line in fin:
            if random.random() < fraction:
                fout.write(line)

def upload_to_hdfs(local_path: str, hdfs_path: str):
    env = get_env()
    hdfs_bin = shutil.which("hdfs", path=env["PATH"]) or f"{env['HADOOP_HOME']}/bin/hdfs"
    
    subprocess.run(
        [hdfs_bin, "dfs", "-test", "-e", hdfs_path],
        capture_output=True, env=env
    )
    if True: # Always attempt removal to be safe given previous failures
        subprocess.run([hdfs_bin, "dfs", "-rm", "-r", "-f", hdfs_path], capture_output=True, env=env)

    upload = subprocess.run(
        [hdfs_bin, "dfs", "-put", local_path, hdfs_path],
        capture_output=True, text=True, env=env
    )
    if upload.returncode != 0:
        raise RuntimeError(f"HDFS upload failed: {upload.stderr}")

def run_hadoop_streaming(
    mapper_path: str,
    reducer_path: str,
    input_hdfs: str,
    output_hdfs: str,
    num_reducers: int = 4
) -> Dict:
    env = get_env()
    hdfs_bin = shutil.which("hdfs", path=env["PATH"]) or f"{env['HADOOP_HOME']}/bin/hdfs"
    hadoop_bin = shutil.which("hadoop", path=env["PATH"]) or f"{env['HADOOP_HOME']}/bin/hadoop"

    subprocess.run(
        [hdfs_bin, "dfs", "-rm", "-r", "-f", output_hdfs],
        capture_output=True, env=env
    )

    cmd = [
        hadoop_bin, "jar", HADOOP_STREAMING_JAR,
        "-D", f"mapreduce.job.reduces={num_reducers}",
        "-D", "mapreduce.map.memory.mb=2048",
        "-D", "mapreduce.reduce.memory.mb=4096",
        "-D", "mapreduce.job.name=benchmark_streaming",
        "-files", f"{mapper_path},{reducer_path}",
        "-mapper", f"python3 {os.path.basename(mapper_path)}",
        "-reducer", f"python3 {os.path.basename(reducer_path)}",
        "-input", input_hdfs,
        "-output", output_hdfs,
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=1800, env=env)

    output_rows = 0
    if proc.returncode == 0:
        count_proc = subprocess.run(
            [hdfs_bin, "dfs", "-cat", f"{output_hdfs}/part-*"],
            capture_output=True, text=True, env=env
        )
        output_rows = len(count_proc.stdout.strip().split("\n")) if count_proc.stdout.strip() else 0

    return {
        "exit_code": proc.returncode,
        "rows": output_rows,
        "stderr": proc.stderr[-2000:] if proc.stderr else ""
    }

def run_spark_submit(
    script_path: str,
    data_dir: str,
    output_dir: str,
    spark_master: str = "local[*]",
    driver_memory: str = "2g",
    executor_memory: str = "2g"
) -> Dict:
    env = get_env()
    
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    cmd = [
        "spark-submit",
        "--master", spark_master,
        "--driver-memory", driver_memory,
        "--executor-memory", executor_memory,
        "--conf", "spark.sql.shuffle.partitions=8",
        "--conf", "spark.driver.extraJavaOptions=-Xss4m",
        script_path,
        data_dir,
        output_dir
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=1800, env=env)

    output_rows = 0
    if proc.returncode == 0:
        for line in proc.stdout.split("\n"):
            if line.startswith("WORKLOAD") and "_ROWS=" in line:
                try:
                    output_rows = int(line.split("=")[1].strip())
                except (ValueError, IndexError):
                    pass

    return {
        "exit_code": proc.returncode,
        "rows": output_rows,
        "stderr": proc.stderr[-2000:] if proc.stderr else ""
    }

def run_hadoop_workload(
    workload_num: int,
    workload_name: str,
    input_files: List[str],
    sample_fraction: float,
    tmp_dir: str
) -> BenchmarkMetrics:
    sampled_files = []
    for f in input_files:
        if not os.path.exists(f):
            continue
        sampled_path = os.path.join(tmp_dir, f"sample_{os.path.basename(f)}")
        sample_csv(f, sample_fraction, sampled_path)
        sampled_files.append(sampled_path)

    if not sampled_files:
        log.warning(f"No input files for Hadoop WL{workload_num}")
        m = BenchmarkMetrics(
            engine="Hadoop_MapReduce",
            workload=f"WL{workload_num}_{workload_name}",
            dataset="ALL",
            exit_code=1,
            error_msg="No input files",
            sample_fraction=sample_fraction
        )
        return m

    hdfs_input = f"{HDFS_INPUT_BASE}/wl{workload_num}_{int(sample_fraction*100)}"
    hdfs_output = f"{HDFS_OUTPUT_BASE}/wl{workload_num}_{int(sample_fraction*100)}"

    try:
        for f in sampled_files:
            upload_to_hdfs(f, f"{hdfs_input}/{os.path.basename(f)}")
    except RuntimeError as e:
        log.error(f"HDFS upload failed: {e}")
        m = BenchmarkMetrics(
            engine="Hadoop_MapReduce",
            workload=f"WL{workload_num}_{workload_name}",
            dataset="ALL",
            exit_code=1,
            error_msg=str(e),
            sample_fraction=sample_fraction
        )
        return m

    wl_dir = os.path.join(SRC_DIR, "hadoop", f"workload{workload_num}_{workload_name}")
    mapper = os.path.join(wl_dir, "mapper.py")
    reducer = os.path.join(wl_dir, "reducer.py")

    def runner():
        return run_hadoop_streaming(mapper, reducer, hdfs_input, hdfs_output)

    metrics = collect_metrics(
        engine="Hadoop_MapReduce",
        workload=f"WL{workload_num}_{workload_name}",
        dataset="PAYSIM+BANKSIM",
        data_paths=sampled_files,
        runner_fn=runner,
        sample_fraction=sample_fraction,
        notes=f"hadoop_streaming_reducers=4"
    )
    return metrics

def run_spark_workload(
    workload_num: int,
    workload_name: str,
    input_files: List[str],
    sample_fraction: float,
    tmp_dir: str,
    spark_master: str = "local[*]"
) -> BenchmarkMetrics:
    sampled_data_dir = os.path.join(tmp_dir, f"spark_data_wl{workload_num}_{int(sample_fraction*100)}")
    os.makedirs(sampled_data_dir, exist_ok=True)

    for f in input_files:
        if not os.path.exists(f):
            continue
        sampled_path = os.path.join(sampled_data_dir, os.path.basename(f))
        sample_csv(f, sample_fraction, sampled_path)

    script_path = os.path.join(SRC_DIR, "spark", f"workload{workload_num}_{workload_name}.py")
    if not os.path.exists(script_path):
        m = BenchmarkMetrics(
            engine="Spark_PySpark",
            workload=f"WL{workload_num}_{workload_name}",
            dataset="ALL",
            exit_code=1,
            error_msg=f"Script not found: {script_path}",
            sample_fraction=sample_fraction
        )
        return m

    spark_output_dir = os.path.join(LOGS_DIR, f"spark_output_wl{workload_num}_{int(sample_fraction*100)}")

    def runner():
        return run_spark_submit(
            script_path=script_path,
            data_dir=sampled_data_dir,
            output_dir=spark_output_dir,
            spark_master=spark_master
        )

    all_sampled = [
        os.path.join(sampled_data_dir, os.path.basename(f))
        for f in input_files
        if os.path.exists(os.path.join(sampled_data_dir, os.path.basename(f)))
    ]

    metrics = collect_metrics(
        engine="Spark_PySpark",
        workload=f"WL{workload_num}_{workload_name}",
        dataset="PAYSIM+BANKSIM",
        data_paths=all_sampled,
        runner_fn=runner,
        sample_fraction=sample_fraction,
        notes=f"spark_master={spark_master}"
    )
    return metrics

def save_results(results: List[BenchmarkMetrics], output_csv: str):
    if not results:
        log.warning("No results to save")
        return

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    fieldnames = list(metrics_to_dict(results[0]).keys())

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for m in results:
            writer.writerow(metrics_to_dict(m))

    log.info(f"Results saved to {output_csv}")

def run_full_benchmark(
    sample_fractions: Optional[List[float]] = None,
    engines: Optional[List[str]] = None,
    workloads: Optional[List[int]] = None,
    spark_master: str = "local[*]",
    output_csv: Optional[str] = None
) -> List[BenchmarkMetrics]:
    if sample_fractions is None:
        sample_fractions = SAMPLE_FRACTIONS
    if engines is None:
        engines = ["spark", "hadoop"]
    if workloads is None:
        workloads = [1, 2, 3]
    if output_csv is None:
        output_csv = os.path.join(LOGS_DIR, "benchmark_results.csv")

    workload_map = {
        1: ("aggregation", [PAYSIM_FILE, BANKSIM_TX_FILE]),
        2: ("fraud", [PAYSIM_FILE, BANKSIM_TX_FILE]),
        3: ("join_window", [PAYSIM_FILE, BANKSIM_TX_FILE, BANKSIM_NET_FILE]),
    }

    all_results: List[BenchmarkMetrics] = []
    tmp_dir = tempfile.mkdtemp(prefix="benchmark_")

    try:
        for fraction in sample_fractions:
            log.info(f"=== Sample fraction: {fraction:.0%} ===")

            for wl_num in workloads:
                if wl_num not in workload_map:
                    continue
                wl_name, wl_files = workload_map[wl_num]
                log.info(f"  Workload {wl_num}: {wl_name}")

                if "spark" in engines:
                    log.info(f"    Running Spark (fraction={fraction})")
                    m = run_spark_workload(
                        wl_num, wl_name, wl_files, fraction, tmp_dir, spark_master
                    )
                    all_results.append(m)
                    log.info(f"    Spark done: {m.wall_time_sec:.1f}s, {m.peak_memory_mb:.0f}MB")

                if "hadoop" in engines:
                    log.info(f"    Running Hadoop MapReduce (fraction={fraction})")
                    m = run_hadoop_workload(wl_num, wl_name, wl_files, fraction, tmp_dir)
                    all_results.append(m)
                    log.info(f"    Hadoop done: {m.wall_time_sec:.1f}s, {m.peak_memory_mb:.0f}MB")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    save_results(all_results, output_csv)
    log.info(f"Benchmark complete. {len(all_results)} runs recorded.")
    return all_results

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Spark vs Hadoop Benchmark Runner")
    parser.add_argument("--fractions", nargs="+", type=float, default=[0.1, 0.25, 0.5, 1.0])
    parser.add_argument("--engines", nargs="+", choices=["spark", "hadoop"], default=["spark", "hadoop"])
    parser.add_argument("--workloads", nargs="+", type=int, choices=[1, 2, 3], default=[1, 2, 3])
    parser.add_argument("--spark-master", default="local[*]")
    parser.add_argument("--output-csv", default=os.path.join(LOGS_DIR, "benchmark_results.csv"))
    args = parser.parse_args()

    results = run_full_benchmark(
        sample_fractions=args.fractions,
        engines=args.engines,
        workloads=args.workloads,
        spark_master=args.spark_master,
        output_csv=args.output_csv
    )
    print(f"\nCompleted {len(results)} benchmark runs")
