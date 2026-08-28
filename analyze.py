"""
Reads the exported CSV dataset and produces the result charts + a written
summary (RESULTS.md) for the project.
"""
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "font.size": 11,
})
COLORS = {
    "success": "#2E7D32",
    "dead_letter": "#C62828",
    "retrying": "#F9A825",
    "processing": "#1565C0",
}

deliveries = pd.read_csv("data/deliveries_dataset.csv")
attempts = pd.read_csv("data/delivery_attempts.csv")

deliveries["partner"] = deliveries["endpoint_url"].str.extract(r"/webhook/([\w-]+)")

# ---------------------------------------------------------------- Chart 1
# Final delivery status breakdown (overall)
status_counts = deliveries["status"].value_counts()
fig, ax = plt.subplots(figsize=(6, 5))
colors = [COLORS.get(s, "#999999") for s in status_counts.index]
ax.pie(status_counts.values, labels=status_counts.index, autopct="%1.1f%%",
       colors=colors, startangle=90, wedgeprops={"edgecolor": "white", "linewidth": 1.5})
ax.set_title("Overall Delivery Outcome (all endpoints)")
plt.tight_layout()
plt.savefig("charts/01_overall_outcome.png", dpi=150)
plt.close()

# ---------------------------------------------------------------- Chart 2
# Success rate by downstream reliability profile (this is the headline
# result: it shows the retry mechanism recovering deliveries even against
# increasingly unreliable partners)
by_partner = deliveries.groupby(["partner", "status"]).size().unstack(fill_value=0)
by_partner = by_partner.reindex(["reliable-partner", "mostly-stable-partner",
                                  "flaky-partner", "very-flaky-partner"])
fig, ax = plt.subplots(figsize=(8, 5.5))
bottom = None
for status in ["success", "dead_letter", "retrying", "processing"]:
    if status not in by_partner.columns:
        continue
    ax.bar(by_partner.index, by_partner[status], bottom=bottom,
           label=status, color=COLORS.get(status, "#999999"))
    bottom = by_partner[status] if bottom is None else bottom + by_partner[status]
ax.set_ylabel("Number of deliveries")
ax.set_title("Delivery Outcome by Downstream Reliability Profile")
ax.legend(title="final status")
plt.xticks(rotation=15)
plt.tight_layout()
plt.savefig("charts/02_outcome_by_partner.png", dpi=150)
plt.close()

# ---------------------------------------------------------------- Chart 3
# How many attempts it took successful deliveries to land
succ = deliveries[deliveries.status == "success"]
attempt_hist = succ["attempt_count"].value_counts().sort_index()
fig, ax = plt.subplots(figsize=(7, 5))
ax.bar(attempt_hist.index.astype(str), attempt_hist.values, color="#1565C0")
ax.set_xlabel("Attempts needed to succeed")
ax.set_ylabel("Number of deliveries")
ax.set_title("Retry Effectiveness: Attempts Needed for Successful Delivery")
for i, v in enumerate(attempt_hist.values):
    ax.text(i, v + max(attempt_hist.values) * 0.01, str(v), ha="center")
plt.tight_layout()
plt.savefig("charts/03_attempts_to_success.png", dpi=150)
plt.close()

# ---------------------------------------------------------------- Chart 4
# Exponential backoff curve (theoretical, matches worker.compute_backoff)
BASE, CAP = 0.3, 5.0
attempt_nums = list(range(1, 9))
backoff_vals = [min(CAP, BASE * (2 ** (a - 1))) for a in attempt_nums]
fig, ax = plt.subplots(figsize=(7, 5))
ax.plot(attempt_nums, backoff_vals, marker="o", color="#6A1B9A", linewidth=2)
ax.set_xlabel("Attempt number")
ax.set_ylabel("Backoff delay before next retry (s)")
ax.set_title("Exponential Backoff Schedule (base=0.3s, cap=5s)")
ax.grid(alpha=0.3)
plt.tight_layout()
plt.savefig("charts/04_backoff_schedule.png", dpi=150)
plt.close()

# ---------------------------------------------------------------- Chart 5
# Latency distribution: success vs failed attempts
fig, ax = plt.subplots(figsize=(7, 5))
succ_latency = attempts[attempts.status == "success"]["latency_ms"].dropna()
fail_latency = attempts[attempts.status == "failed"]["latency_ms"].dropna()
ax.hist(succ_latency, bins=30, alpha=0.7, label="success", color=COLORS["success"])
ax.hist(fail_latency, bins=30, alpha=0.7, label="failed", color=COLORS["dead_letter"])
ax.set_xlabel("Attempt latency (ms)")
ax.set_ylabel("Count")
ax.set_title("Delivery Attempt Latency: Success vs Failure")
ax.legend()
plt.tight_layout()
plt.savefig("charts/05_latency_distribution.png", dpi=150)
plt.close()

# ---------------------------------------------------------------- Chart 6
# Event-type fan-out success rate (sanity check the system treats all
# event types equally -- delivery reliability should be endpoint-driven,
# not event-type-driven)
by_type = deliveries.groupby(["event_type", "status"]).size().unstack(fill_value=0)
by_type_pct = by_type.div(by_type.sum(axis=1), axis=0) * 100
fig, ax = plt.subplots(figsize=(8, 5))
bottom = None
for status in ["success", "dead_letter", "retrying", "processing"]:
    if status not in by_type_pct.columns:
        continue
    ax.bar(by_type_pct.index, by_type_pct[status], bottom=bottom,
           label=status, color=COLORS.get(status, "#999999"))
    bottom = by_type_pct[status] if bottom is None else bottom + by_type_pct[status]
ax.set_ylabel("% of deliveries")
ax.set_title("Delivery Outcome % by Event Type")
ax.legend(title="final status", loc="upper right")
plt.xticks(rotation=20)
plt.tight_layout()
plt.savefig("charts/06_outcome_by_event_type.png", dpi=150)
plt.close()

# ---------------------------------------------------------------- Summary
summary = {
    "total_deliveries": int(len(deliveries)),
    "overall_success_rate_pct": round(100 * (deliveries.status == "success").mean(), 2),
    "dead_letter_rate_pct": round(100 * (deliveries.status == "dead_letter").mean(), 2),
    "avg_attempts_for_success": round(succ["attempt_count"].mean(), 2),
    "median_success_latency_ms": round(float(succ_latency.median()), 2) if len(succ_latency) else None,
    "by_partner_success_rate_pct": (
        deliveries.groupby("partner")["status"]
        .apply(lambda s: round(100 * (s == "success").mean(), 2))
        .to_dict()
    ),
    "total_attempts_made": int(len(attempts)),
}

with open("data/summary_metrics.json", "w") as f:
    json.dump(summary, f, indent=2)

print(json.dumps(summary, indent=2))
