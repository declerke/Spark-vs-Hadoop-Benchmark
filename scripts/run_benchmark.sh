#!/usr/bin/env bash
export HADOOP_HOME=/opt/hadoop-3.2.1
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH

set -euo pipefail

BENCHMARK_BASE_DIR="${BENCHMARK_BASE_DIR:-/benchmark}"
DATA_DIR="${BENCHMARK_BASE_DIR}/data"
LOGS_DIR="${BENCHMARK_BASE_DIR}/logs"
PLOTS_DIR="${BENCHMARK_BASE_DIR}/plots"
SRC_DIR="${BENCHMARK_BASE_DIR}/src"

SPARK_MASTER="${SPARK_MASTER:-local[*]}"
ENGINES="${ENGINES:-spark hadoop}"
FRACTIONS="${FRACTIONS:-0.1 0.25 0.5 1.0}"
WORKLOADS="${WORKLOADS:-1 2 3}"

LOG_FILE="${LOGS_DIR}/benchmark_run_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "${LOGS_DIR}" "${PLOTS_DIR}"

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "${msg}"
    echo "${msg}" >> "${LOG_FILE}"
}

check_prerequisites() {
    log "Checking prerequisites..."

    if [ -f /usr/local/bin/hadoop ] && [ ! -L /usr/local/bin/hadoop ]; then
        rm -f /usr/local/bin/hadoop
        ln -s /opt/hadoop-3.2.1/bin/hadoop /usr/local/bin/hadoop
    fi

    if [ -f "${HADOOP_HOME}/etc/hadoop/hadoop-env.sh" ]; then
        sed -i '/export JAVA_HOME=/d' "${HADOOP_HOME}/etc/hadoop/hadoop-env.sh"
        echo "export JAVA_HOME=${JAVA_HOME}" >> "${HADOOP_HOME}/etc/hadoop/hadoop-env.sh"
    fi

    export HADOOP_HOME=/opt/hadoop-3.2.1
    export PATH=$HADOOP_HOME/bin:$PATH

    python3 -c "import psutil, pandas, matplotlib, seaborn" 2>/dev/null || {
        log "Installing Python dependencies..."
        pip install psutil pandas matplotlib seaborn --quiet
    }

    if [[ " ${ENGINES} " =~ " spark " ]]; then
        if ! command -v spark-submit &>/dev/null; then
            log "WARNING: spark-submit not found. Spark benchmarks will fail."
        else
            log "Spark found: $(spark-submit --version 2>&1 | head -1)"
        fi
    fi

    if [[ " ${ENGINES} " =~ " hadoop " ]]; then
        if ! command -v hadoop &>/dev/null; then
            log "Hadoop not found. Attempting auto-installation..."
            
            local HADOOP_VER="3.2.1"
            local HADOOP_URL="https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VER}/hadoop-${HADOOP_VER}.tar.gz"
            
            mkdir -p /opt
            wget -qO- "${HADOOP_URL}" | tar -xzC /opt/
            
            export HADOOP_HOME="/opt/hadoop-${HADOOP_VER}"
            export PATH="${PATH}:${HADOOP_HOME}/bin"
            log "Hadoop ${HADOOP_VER} installed successfully to ${HADOOP_HOME}"
        else
            log "Hadoop found: $(${HADOOP_HOME}/bin/hadoop version 2>/dev/null | head -1)"
        fi
    fi
}

validate_data() {
    log "Validating input datasets..."

    local found=0
    for f in paysim_transactions.csv banksim_transactions.csv banksim_network.csv; do
        if [ -f "${DATA_DIR}/${f}" ]; then
            local size_mb=$(du -m "${DATA_DIR}/${f}" | cut -f1)
            log "    Found: ${f} (${size_mb} MB)"
            found=$((found + 1))
        else
            log "    Missing: ${DATA_DIR}/${f}"
        fi
    done

    if [ "${found}" -eq 0 ]; then
        log "ERROR: No dataset files found in ${DATA_DIR}"
        log "Please copy PaySim and BankSim CSV files to ${DATA_DIR}/"
    fi

    python3 "${BENCHMARK_BASE_DIR}/scripts/prepare_data.py" \
        --data-dir "${DATA_DIR}" \
        --validate \
        --rename 2>&1 | tee -a "${LOG_FILE}"
}

setup_hdfs() {
    if [[ ! " ${ENGINES} " =~ " hadoop " ]]; then
        return
    fi

    log "Setting up HDFS directories..."
    export HADOOP_CONF_DIR="${BENCHMARK_BASE_DIR}/configs/hadoop"
    export HADOOP_SSH_OPTS="-o BatchMode=yes -o ConnectTimeout=5"

    hdfs dfs -rm -r /benchmark_input /benchmark_output 2>/dev/null || true

    hdfs dfs -mkdir -p /benchmark_input 2>/dev/null || true
    hdfs dfs -mkdir -p /benchmark_output 2>/dev/null || true
    hdfs dfs -chmod 777 /benchmark_input 2>/dev/null || true
    hdfs dfs -chmod 777 /benchmark_output 2>/dev/null || true
    log "HDFS directories ready."
}

make_executables() {
    find "${SRC_DIR}/hadoop" -name "*.py" -exec chmod +x {} \;
    log "Hadoop scripts made executable."
}

run_benchmark() {
    log "Starting benchmark..."
    log "    Engines: ${ENGINES}"
    log "    Fractions: ${FRACTIONS}"
    log "    Workloads: ${WORKLOADS}"
    log "    Spark master: ${SPARK_MASTER}"

    export HADOOP_CONF_DIR="${BENCHMARK_BASE_DIR}/configs/hadoop"

    BENCHMARK_BASE_DIR="${BENCHMARK_BASE_DIR}" \
    HADOOP_HOME="${HADOOP_HOME}" \
    JAVA_HOME="${JAVA_HOME}" \
    python3 "${SRC_DIR}/benchmark/runner.py" \
        --fractions ${FRACTIONS} \
        --engines ${ENGINES} \
        --workloads ${WORKLOADS} \
        --spark-master "${SPARK_MASTER}" \
        --output-csv "${LOGS_DIR}/benchmark_results.csv" \
        2>&1 | tee -a "${LOG_FILE}"
}

visualize_results() {
    local results_csv="${LOGS_DIR}/benchmark_results.csv"
    if [ ! -f "${results_csv}" ]; then
        log "WARNING: No results CSV found at ${results_csv}. Skipping visualization."
        return
    fi

    log "Generating visualizations..."
    python3 "${BENCHMARK_BASE_DIR}/scripts/visualize.py" \
        --input "${results_csv}" \
        --output-dir "${PLOTS_DIR}" \
        2>&1 | tee -a "${LOG_FILE}"
    log "Plots saved to ${PLOTS_DIR}"
}

print_summary() {
    local results_csv="${LOGS_DIR}/benchmark_results.csv"
    if [ ! -f "${results_csv}" ]; then
        return
    fi

    log "=== BENCHMARK SUMMARY ==="
    LOGS_DIR="${LOGS_DIR}" python3 - << 'PYEOF'
import csv, os, sys

results_csv = os.path.join(os.environ.get("LOGS_DIR", "/benchmark/logs"), "benchmark_results.csv")
if not os.path.exists(results_csv):
    sys.exit(0)

rows = []
with open(results_csv) as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f"\n{'Engine':<25} {'Workload':<30} {'Fraction':<10} {'Time(s)':<12} {'PeakMem(MB)':<15} {'ExitCode'}")
print("-" * 105)
for row in rows:
    try:
        print(
            f"{row.get('engine',''):<25} "
            f"{row.get('workload',''):<30} "
            f"{float(row.get('sample_fraction',0)):<10.2f} "
            f"{float(row.get('wall_time_sec',0)):<12.1f} "
            f"{float(row.get('peak_memory_mb',0)):<15.0f} "
            f"{row.get('exit_code','')}"
        )
    except:
        continue
PYEOF
}

main() {
    log "=== Spark vs Hadoop Benchmark ==="
    log "Base dir: ${BENCHMARK_BASE_DIR}"
    
    export HADOOP_CONF_DIR="${BENCHMARK_BASE_DIR}/configs/hadoop"

    check_prerequisites
    validate_data
    setup_hdfs
    make_executables
    run_benchmark
    visualize_results
    print_summary

    log "=== Benchmark complete. Results: ${LOGS_DIR}/benchmark_results.csv ==="
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    echo "Usage: ENGINES='spark hadoop' FRACTIONS='0.1 0.5 1.0' ./run_benchmark.sh"
    exit 0
fi

main "$@"
