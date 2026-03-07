#!/usr/bin/env python3
import os
import sys
import argparse
import logging
import csv
import math
from typing import List, Dict, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
import numpy as np

try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

ENGINE_COLORS = {
    "spark": "#E25000",
    "hadoop": "#0176D3",
}

ENGINE_MARKERS = {
    "spark": "o",
    "hadoop": "s",
}

WORKLOAD_LABELS = {
    "WL1_aggregation": "WL1: Daily Aggregation",
    "WL2_fraud": "WL2: Fraud Proxy",
    "WL3_join_window": "WL3: Join + Window",
}

FIGURE_DPI = 150
FIGURE_SIZE_STANDARD = (10, 6)
FIGURE_SIZE_WIDE = (14, 6)
FIGURE_SIZE_SQUARE = (8, 8)

def load_results(csv_path: str) -> List[Dict]:
    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            
            if row["engine"] == "Spark_PySpark": row["engine"] = "spark"
            if row["engine"] == "Hadoop_MapReduce": row["engine"] = "hadoop"
            
            for col in ["wall_time_sec", "cpu_time_sec", "peak_memory_mb", "avg_memory_mb",
                        "data_size_mb", "output_rows", "sample_fraction", "tracemalloc_peak_mb",
                        "peak_cpu_pct", "avg_cpu_pct"]:
                try:
                    row[col] = float(row.get(col, 0) or 0)
                except ValueError:
                    row[col] = 0.0
            try:
                row["exit_code"] = int(row.get("exit_code", 0) or 0)
            except ValueError:
                row["exit_code"] = 1
            rows.append(row)
    return rows

def filter_successful(rows: List[Dict]) -> List[Dict]:
    return [r for r in rows if r["exit_code"] <= 1 and r["wall_time_sec"] > 0]

def style_axis(ax, title: str, xlabel: str, ylabel: str):
    ax.set_title(title, fontsize=13, fontweight="bold", pad=12)
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.tick_params(labelsize=9)

def add_engine_legend(ax, engines):
    patches = [
        mpatches.Patch(color=ENGINE_COLORS.get(e, "gray"), label=e.replace("_", " "))
        for e in engines
    ]
    ax.legend(handles=patches, loc="upper left", framealpha=0.9, fontsize=9)

def plot_scaling_curves(rows: List[Dict], output_dir: str):
    successful = filter_successful(rows)
    if not successful:
        log.warning("No successful rows for scaling curves")
        return

    workloads = sorted(set(r["workload"] for r in successful))
    engines = sorted(set(r["engine"] for r in successful))

    fig, axes = plt.subplots(1, len(workloads), figsize=(5 * len(workloads), 6), squeeze=False)
    axes = axes[0]

    for ax_idx, wl in enumerate(workloads):
        ax = axes[ax_idx]
        wl_rows = [r for r in successful if r["workload"] == wl]

        for engine in engines:
            eng_rows = sorted(
                [r for r in wl_rows if r["engine"] == engine],
                key=lambda r: r["data_size_mb"]
            )
            if not eng_rows:
                continue

            x = [r["data_size_mb"] for r in eng_rows]
            y = [r["wall_time_sec"] for r in eng_rows]

            ax.plot(
                x, y,
                color=ENGINE_COLORS.get(engine, "gray"),
                marker=ENGINE_MARKERS.get(engine, "o"),
                linewidth=2.0,
                markersize=7,
                label=engine.replace("_", " ")
            )

            for xi, yi in zip(x, y):
                ax.annotate(
                    f"{yi:.0f}s",
                    (xi, yi),
                    textcoords="offset points",
                    xytext=(4, 4),
                    fontsize=7,
                    color=ENGINE_COLORS.get(engine, "gray")
                )

        wl_label = WORKLOAD_LABELS.get(wl, wl)
        style_axis(ax, wl_label, "Input Data Size (MB)", "Wall-Clock Time (s)")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.legend(fontsize=8, framealpha=0.9)

    fig.suptitle("Execution Time Scaling Curves: Spark vs Hadoop MapReduce",
                 fontsize=14, fontweight="bold", y=1.02)
    fig.tight_layout()
    out = os.path.join(output_dir, "01_scaling_curves.png")
    fig.savefig(out, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved: {out}")

def plot_time_comparison_bars(rows: List[Dict], output_dir: str):
    successful = filter_successful(rows)
    if not successful:
        return

    full_rows = [r for r in successful if r["sample_fraction"] >= 0.99]
    if not full_rows:
        max_frac = max(r["sample_fraction"] for r in successful)
        full_rows = [r for r in successful if r["sample_fraction"] >= max_frac - 0.01]

    workloads = sorted(set(r["workload"] for r in full_rows))
    engines = sorted(set(r["engine"] for r in full_rows))

    n_wl = len(workloads)
    x = np.arange(n_wl)
    width = 0.35

    fig, ax = plt.subplots(figsize=FIGURE_SIZE_STANDARD)

    for eng_idx, engine in enumerate(engines):
        times = []
        for wl in workloads:
            matching = [r for r in full_rows if r["engine"] == engine and r["workload"] == wl]
            times.append(matching[0]["wall_time_sec"] if matching else 0)

        offset = (eng_idx - (len(engines) - 1) / 2) * width
        bars = ax.bar(
            x + offset, times, width,
            label=engine.replace("_", " "),
            color=ENGINE_COLORS.get(engine, "gray"),
            alpha=0.85,
            edgecolor="white",
            linewidth=0.5
        )
        for bar, t in zip(bars, times):
            if t > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max(times) * 0.01,
                    f"{t:.0f}s",
                    ha="center", va="bottom", fontsize=8, fontweight="bold"
                )

    ax.set_xticks(x)
    ax.set_xticklabels([WORKLOAD_LABELS.get(wl, wl) for wl in workloads], fontsize=9)
    style_axis(ax, "Wall-Clock Execution Time by Workload (Full Dataset)",
               "Workload", "Time (seconds)")
    ax.legend(fontsize=9)

    spark_rows = [r for r in full_rows if r["engine"] == "spark"]
    hadoop_rows = [r for r in full_rows if r["engine"] == "hadoop"]
    if spark_rows and hadoop_rows:
        for wl in workloads:
            s = next((r["wall_time_sec"] for r in spark_rows if r["workload"] == wl), None)
            h = next((r["wall_time_sec"] for r in hadoop_rows if r["workload"] == wl), None)
            if s and h and s > 0:
                speedup = h / s
                wl_idx = workloads.index(wl)
                ax.annotate(
                    f"{speedup:.1f}×\nspeedup",
                    xy=(wl_idx, max(s, h) * 1.08),
                    ha="center", fontsize=8, color="#2E7D32",
                    fontweight="bold"
                )

    fig.tight_layout()
    out = os.path.join(output_dir, "02_time_comparison_bars.png")
    fig.savefig(out, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved: {out}")

def plot_memory_vs_datasize(rows: List[Dict], output_dir: str):
    successful = filter_successful(rows)
    if not successful:
        return

    engines = sorted(set(r["engine"] for r in successful))
    fig, ax = plt.subplots(figsize=FIGURE_SIZE_STANDARD)

    for engine in engines:
        eng_rows = [r for r in successful if r["engine"] == engine]
        x = [r["data_size_mb"] for r in eng_rows]
        y = [r["peak_memory_mb"] for r in eng_rows]

        ax.scatter(
            x, y,
            color=ENGINE_COLORS.get(engine, "gray"),
            marker=ENGINE_MARKERS.get(engine, "o"),
            s=60,
            alpha=0.7,
            label=engine.replace("_", " ")
        )

        if len(x) >= 2:
            try:
                coeffs = np.polyfit(np.log(np.maximum(x, 1e-6)), y, 1)
                x_fit = np.linspace(min(x), max(x), 100)
                y_fit = coeffs[0] * np.log(np.maximum(x_fit, 1e-6)) + coeffs[1]
                ax.plot(x_fit, y_fit, color=ENGINE_COLORS.get(engine, "gray"),
                        linestyle="--", alpha=0.5, linewidth=1.5)
            except (np.linalg.LinAlgError, ValueError):
                pass

    style_axis(ax, "Peak Memory Usage vs Input Data Size",
               "Input Data Size (MB)", "Peak Memory (MB)")
    ax.legend(fontsize=9)

    fig.tight_layout()
    out = os.path.join(output_dir, "03_memory_vs_datasize.png")
    fig.savefig(out, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved: {out}")

def plot_pareto_frontier(rows: List[Dict], output_dir: str):
    successful = filter_successful(rows)
    if not successful:
        return

    full_rows = [r for r in successful if r["sample_fraction"] >= 0.99]
    if not full_rows:
        max_frac = max(r["sample_fraction"] for r in successful)
        full_rows = [r for r in successful if r["sample_fraction"] >= max_frac - 0.01]

    if not full_rows:
        return

    fig, ax = plt.subplots(figsize=FIGURE_SIZE_SQUARE)

    workloads = sorted(set(r["workload"] for r in full_rows))
    wl_markers = ["o", "s", "^", "D", "v"]

    engines = sorted(set(r["engine"] for r in full_rows))

    for r in full_rows:
        wl_idx = workloads.index(r["workload"]) if r["workload"] in workloads else 0
        ax.scatter(
            r["wall_time_sec"],
            r["peak_memory_mb"],
            color=ENGINE_COLORS.get(r["engine"], "gray"),
            marker=wl_markers[wl_idx % len(wl_markers)],
            s=200,
            alpha=0.85,
            edgecolors="white",
            linewidth=1.0,
            zorder=5
        )
        ax.annotate(
            f"{WORKLOAD_LABELS.get(r['workload'], r['workload']).split(':')[0]}\n{r['engine'].split('_')[0]}",
            (r["wall_time_sec"], r["peak_memory_mb"]),
            textcoords="offset points",
            xytext=(8, 4),
            fontsize=7,
            color=ENGINE_COLORS.get(r["engine"], "gray")
        )

    style_axis(ax, "Efficiency Pareto Frontier: Time vs Memory (Full Dataset)",
               "Wall-Clock Time (s)", "Peak Memory (MB)")

    engine_patches = [
        mpatches.Patch(color=ENGINE_COLORS.get(e, "gray"), label=e.replace("_", " "))
        for e in engines
    ]
    wl_legend = [
        plt.scatter([], [], marker=wl_markers[i % len(wl_markers)], color="gray", label=WORKLOAD_LABELS.get(wl, wl))
        for i, wl in enumerate(workloads)
    ]
    ax.legend(handles=engine_patches + wl_legend, loc="upper right", fontsize=8, framealpha=0.9)

    ax.annotate("← Faster", xy=(0.02, 0.08), xycoords="axes fraction", fontsize=9,
                color="#555555", style="italic")
    ax.annotate("↓ Less Memory", xy=(0.75, 0.02), xycoords="axes fraction", fontsize=9,
                color="#555555", style="italic")

    fig.tight_layout()
    out = os.path.join(output_dir, "04_pareto_frontier.png")
    fig.savefig(out, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved: {out}")

def plot_resource_heatmap(rows: List[Dict], output_dir: str):
    successful = filter_successful(rows)
    if not successful:
        return

    engines = sorted(set(r["engine"] for r in successful))
    workloads = sorted(set(r["workload"] for r in successful))

    metrics_to_plot = [
        ("wall_time_sec", "Exec Time (s)"),
        ("peak_memory_mb", "Peak RAM (MB)"),
        ("cpu_time_sec", "CPU Time (s)"),
    ]

    fig, axes = plt.subplots(
        len(metrics_to_plot), 1,
        figsize=(max(8, len(workloads) * 2), 4 * len(metrics_to_plot))
    )

    if len(metrics_to_plot) == 1:
        axes = [axes]

    fractions = sorted(set(r["sample_fraction"] for r in successful))

    for ax_idx, (metric_col, metric_label) in enumerate(metrics_to_plot):
        ax = axes[ax_idx]

        matrix_data = []
        row_labels = []

        for engine in engines:
            for frac in fractions:
                row = []
                row_labels.append(f"{engine.split('_')[0]}\n{frac:.0%}")
                for wl in workloads:
                    matching = [
                        r for r in successful
                        if r["engine"] == engine
                        and r["workload"] == wl
                        and abs(r["sample_fraction"] - frac) < 0.01
                    ]
                    if matching:
                        row.append(matching[0][metric_col])
                    else:
                        row.append(float("nan"))
                matrix_data.append(row)

        matrix = np.array(matrix_data, dtype=float)

        if HAS_SEABORN:
            cmap = "YlOrRd"
            im = sns.heatmap(
                matrix,
                ax=ax,
                annot=True,
                fmt=".0f",
                cmap=cmap,
                xticklabels=[WORKLOAD_LABELS.get(w, w).split(":")[0] for w in workloads],
                yticklabels=row_labels,
                linewidths=0.5,
                linecolor="white"
            )
        else:
            valid = matrix[~np.isnan(matrix)]
            vmin = valid.min() if len(valid) > 0 else 0
            vmax = valid.max() if len(valid) > 0 else 1
            im = ax.imshow(matrix, cmap="YlOrRd", vmin=vmin, vmax=vmax, aspect="auto")
            ax.set_xticks(range(len(workloads)))
            ax.set_xticklabels([WORKLOAD_LABELS.get(w, w).split(":")[0] for w in workloads], fontsize=8)
            ax.set_yticks(range(len(row_labels)))
            ax.set_yticklabels(row_labels, fontsize=7)
            for i in range(matrix.shape[0]):
                for j in range(matrix.shape[1]):
                    if not np.isnan(matrix[i, j]):
                        ax.text(j, i, f"{matrix[i, j]:.0f}", ha="center", va="center", fontsize=8)
            plt.colorbar(im, ax=ax)

        ax.set_title(f"{metric_label} Heatmap", fontsize=11, fontweight="bold")

    fig.suptitle("Resource Usage Heatmap: Engine × Dataset Size × Workload",
                 fontsize=13, fontweight="bold")
    fig.tight_layout()
    out = os.path.join(output_dir, "05_resource_heatmap.png")
    fig.savefig(out, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved: {out}")

def plot_speedup_ratios(rows: List[Dict], output_dir: str):
    successful = filter_successful(rows)
    if not successful:
        return

    fractions = sorted(set(r["sample_fraction"] for r in successful))
    workloads = sorted(set(r["workload"] for r in successful))

    speedup_data = {wl: {"fractions": [], "speedups": []} for wl in workloads}

    for frac in fractions:
        for wl in workloads:
            spark_t = next(
                (r["wall_time_sec"] for r in successful
                 if r["engine"] == "spark"
                 and r["workload"] == wl
                 and abs(r["sample_fraction"] - frac) < 0.01
                 and r["wall_time_sec"] > 0),
                None
            )
            hadoop_t = next(
                (r["wall_time_sec"] for r in successful
                 if r["engine"] == "hadoop"
                 and r["workload"] == wl
                 and abs(r["sample_fraction"] - frac) < 0.01
                 and r["wall_time_sec"] > 0),
                None
            )
            if spark_t and hadoop_t:
                speedup_data[wl]["fractions"].append(frac * 100)
                speedup_data[wl]["speedups"].append(hadoop_t / spark_t)

    fig, ax = plt.subplots(figsize=FIGURE_SIZE_STANDARD)

    wl_colors = ["#1976D2", "#388E3C", "#F57C00"]
    for idx, wl in enumerate(workloads):
        d = speedup_data[wl]
        if d["fractions"]:
            ax.plot(
                d["fractions"], d["speedups"],
                marker="o", linewidth=2,
                color=wl_colors[idx % len(wl_colors)],
                label=WORKLOAD_LABELS.get(wl, wl),
                markersize=7
            )
            for fx, sy in zip(d["fractions"], d["speedups"]):
                ax.annotate(
                    f"{sy:.1f}×",
                    (fx, sy),
                    textcoords="offset points",
                    xytext=(5, 3),
                    fontsize=8,
                    color=wl_colors[idx % len(wl_colors)]
                )

    ax.axhline(y=1.0, color="black", linestyle="--", linewidth=1, alpha=0.5)
    ax.fill_between([0, 105], [1, 1], [0, 0], alpha=0.05, color="blue",
                    label="Hadoop faster zone")
    ax.fill_between([0, 105], [1, 1], [50, 50], alpha=0.05, color="red",
                    label="Spark faster zone")

    style_axis(
        ax,
        "Spark Speedup over Hadoop MapReduce (higher = Spark faster)",
        "Dataset Size (% of full data)",
        "Speedup Ratio (Hadoop time / Spark time)"
    )
    ax.legend(fontsize=8, framealpha=0.9)
    ax.set_xlim(0, 110)
    ax.set_ylim(bottom=0)

    fig.tight_layout()
    out = os.path.join(output_dir, "06_speedup_ratios.png")
    fig.savefig(out, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Saved: {out}")

def generate_all_plots(csv_path: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    rows = load_results(csv_path)
    log.info(f"Loaded {len(rows)} benchmark records from {csv_path}")

    if HAS_SEABORN:
        sns.set_theme(style="whitegrid", palette="muted")
    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "axes.titlesize": 13,
        "axes.labelsize": 11,
        "figure.facecolor": "white",
    })

    plot_scaling_curves(rows, output_dir)
    plot_time_comparison_bars(rows, output_dir)
    plot_memory_vs_datasize(rows, output_dir)
    plot_pareto_frontier(rows, output_dir)
    plot_resource_heatmap(rows, output_dir)
    plot_speedup_ratios(rows, output_dir)

    log.info(f"All plots saved to {output_dir}")

def main():
    parser = argparse.ArgumentParser(description="Visualize benchmark results")
    parser.add_argument("--input", required=True, help="Path to benchmark_results.csv")
    parser.add_argument("--output-dir", default="/benchmark/plots", help="Output directory for plots")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        log.error(f"Input file not found: {args.input}")
        sys.exit(1)

    generate_all_plots(args.input, args.output_dir)

if __name__ == "__main__":
    main()