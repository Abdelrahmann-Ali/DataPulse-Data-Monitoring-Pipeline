"""
DataPulse - Data Pipeline Monitor
A desktop application for data engineers to monitor ETL pipelines,
test data source connections, and run data quality checks.
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
import os
import csv
import random
import threading
import time
from datetime import datetime, timedelta


# THEME & STYLING

COLORS = {
    "bg": "#0b0e14",
    "surface": "#111620",
    "card": "#161b27",
    "border": "#1e2636",
    "accent": "#00d4aa",
    "accent_hover": "#00f0c0",
    "success": "#00e676",
    "warning": "#ffab40",
    "danger": "#ff5252",
    "info": "#40c4ff",
    "text": "#d0d8e8",
    "text_dim": "#5a6580",
    "text_bright": "#ffffff",
    "bar_bg": "#1a2030",
}

FONTS = {
    "title": ("Segoe UI", 22, "bold"),
    "heading": ("Segoe UI", 14, "bold"),
    "subheading": ("Segoe UI", 11, "bold"),
    "body": ("Segoe UI", 10),
    "small": ("Segoe UI", 9),
    "mono": ("Consolas", 10),
    "mono_small": ("Consolas", 9),
    "metric_big": ("Segoe UI", 28, "bold"),
    "metric_label": ("Segoe UI", 9),
}


# SIMULATED DATA ENGINE

PIPELINE_NAMES = [
    "ETL_Sales_Daily", "Ingest_Clickstream", "Transform_Users",
    "Load_Warehouse_DIM", "Spark_Aggregation", "Kafka_Consumer_Orders",
    "CDC_Postgres_Sync", "dbt_Staging_Models", "Airflow_DAG_Reports",
    "S3_to_Snowflake",
]

DATA_SOURCES = [
    {"name": "PostgreSQL Prod", "type": "PostgreSQL", "host": "db-prod.internal:5432", "db": "analytics"},
    {"name": "MySQL Staging", "type": "MySQL", "host": "db-staging.internal:3306", "db": "staging_dw"},
    {"name": "MongoDB Atlas", "type": "MongoDB", "host": "cluster0.abc.mongodb.net:27017", "db": "events"},
    {"name": "Redis Cache", "type": "Redis", "host": "redis.internal:6379", "db": "0"},
    {"name": "Kafka Broker", "type": "Kafka", "host": "kafka.internal:9092", "db": "—"},
    {"name": "S3 Data Lake", "type": "AWS S3", "host": "s3.amazonaws.com", "db": "data-lake-prod"},
    {"name": "Snowflake DW", "type": "Snowflake", "host": "acme.snowflakecomputing.com", "db": "PROD_DW"},
    {"name": "Elasticsearch", "type": "Elasticsearch", "host": "es.internal:9200", "db": "logs-*"},
]

DQ_CHECKS = [
    "Null check on primary keys",
    "Row count threshold (>1000)",
    "Schema drift detection",
    "Duplicate record check",
    "Freshness check (< 1 hour)",
    "Referential integrity (FK)",
    "Value range validation",
    "Data type consistency",
]

SCHEMA_DATA = {
    "dim_users": ["user_id (PK, INT)", "username (VARCHAR)", "email (VARCHAR)", "created_at (TIMESTAMP)", "is_active (BOOL)"],
    "fact_orders": ["order_id (PK, INT)", "user_id (FK, INT)", "product_id (FK, INT)", "amount (DECIMAL)", "order_date (DATE)", "status (VARCHAR)"],
    "dim_products": ["product_id (PK, INT)", "name (VARCHAR)", "category (VARCHAR)", "price (DECIMAL)", "stock (INT)"],
    "stg_events": ["event_id (PK, BIGINT)", "event_type (VARCHAR)", "user_id (INT)", "payload (JSON)", "timestamp (TIMESTAMP)"],
    "fact_sessions": ["session_id (PK, UUID)", "user_id (FK, INT)", "start_time (TIMESTAMP)", "end_time (TIMESTAMP)", "page_views (INT)"],
}

def generate_pipelines(n=10):
    statuses = ["Success", "Success", "Success", "Running", "Failed", "Queued"]
    pipelines = []
    for i in range(n):
        name = PIPELINE_NAMES[i % len(PIPELINE_NAMES)]
        status = random.choice(statuses)
        dur = random.randint(5, 600) if status in ("Success", "Failed") else 0
        rows = random.randint(1000, 5000000) if status == "Success" else 0
        ago = random.randint(1, 180)
        pipelines.append({
            "id": f"PL-{1000+i}",
            "name": name,
            "status": status,
            "duration_sec": dur,
            "rows_processed": rows,
            "last_run": (datetime.now() - timedelta(minutes=ago)).strftime("%Y-%m-%d %H:%M"),
            "schedule": random.choice(["@hourly", "@daily", "*/15 * * * *", "@weekly"]),
        })
    return pipelines


def generate_dq_results():
    results = []
    for check in DQ_CHECKS:
        passed = random.random() > 0.2
        results.append({
            "check": check,
            "status": "Passed" if passed else "Failed",
            "table": random.choice(["dim_users", "fact_orders", "stg_events", "dim_products"]),
            "details": "OK" if passed else f"{random.randint(1,500)} issues found",
            "run_time": f"{random.uniform(0.1, 5.0):.2f}s",
        })
    return results


# MAIN APPLICATION

class DataPulseApp:
    """Main application class for DataPulse pipeline monitor."""

    def __init__(self, root):
        self.root = root
        self.root.title("DataPulse — Pipeline Monitor")
        self.root.geometry("1150x750")
        self.root.configure(bg=COLORS["bg"])
        self.root.minsize(900, 600)

        self.pipelines = generate_pipelines(10)
        self.dq_results = generate_dq_results()

        self._build_ui()

    # UI Construction 

    def _build_ui(self):
        # Top bar
        self._build_topbar()

        # Main content area with notebook tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        self._configure_notebook_style()

        # Tab 1 — Dashboard
        self.tab_dash = tk.Frame(self.notebook, bg=COLORS["bg"])
        self.notebook.add(self.tab_dash, text="  📊 Dashboard  ")
        self._build_dashboard(self.tab_dash)

        # Tab 2 — Pipelines
        self.tab_pipes = tk.Frame(self.notebook, bg=COLORS["bg"])
        self.notebook.add(self.tab_pipes, text="  ⚙️ Pipelines  ")
        self._build_pipelines_tab(self.tab_pipes)

        # Tab 3 — Connections
        self.tab_conn = tk.Frame(self.notebook, bg=COLORS["bg"])
        self.notebook.add(self.tab_conn, text="  🔌 Connections  ")
        self._build_connections_tab(self.tab_conn)

        # Tab 4 — Data Quality
        self.tab_dq = tk.Frame(self.notebook, bg=COLORS["bg"])
        self.notebook.add(self.tab_dq, text="  ✅ Data Quality  ")
        self._build_dq_tab(self.tab_dq)

        # Tab 5 — Schema Explorer
        self.tab_schema = tk.Frame(self.notebook, bg=COLORS["bg"])
        self.notebook.add(self.tab_schema, text="  🗂️ Schema  ")
        self._build_schema_tab(self.tab_schema)

    def _configure_notebook_style(self):
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TNotebook", background=COLORS["bg"], borderwidth=0)
        style.configure("TNotebook.Tab",
                        background=COLORS["border"], foreground=COLORS["text_dim"],
                        font=FONTS["subheading"], padding=[18, 10])
        style.map("TNotebook.Tab",
                   background=[("selected", COLORS["card"])],
                   foreground=[("selected", COLORS["accent"])])

        style.configure("Treeview",
                        background=COLORS["surface"], foreground=COLORS["text"],
                        fieldbackground=COLORS["surface"], font=FONTS["body"], rowheight=34,
                        borderwidth=0)
        style.configure("Treeview.Heading",
                        background=COLORS["bg"], foreground=COLORS["accent"],
                        font=FONTS["subheading"], padding=[0, 6])
        style.map("Treeview",
                  background=[("selected", "#d1fae5")],
                  foreground=[("selected", COLORS["text"])])

    # Top Bar 

    def _build_topbar(self):
        bar = tk.Frame(self.root, bg=COLORS["surface"], height=62,
                       highlightbackground=COLORS["border"], highlightthickness=1)
        bar.pack(fill="x", padx=14, pady=(14, 8))
        bar.pack_propagate(False)

        tk.Label(bar, text="⚡ DataPulse", font=FONTS["title"],
                 bg=COLORS["surface"], fg=COLORS["accent"]).pack(side="left", padx=16)

        tk.Label(bar, text="Pipeline Monitor for Data Engineers", font=FONTS["body"],
                 bg=COLORS["surface"], fg=COLORS["text_dim"]).pack(side="left", padx=8)

        # Refresh button
        refresh_btn = tk.Button(bar, text="🔄 Refresh", font=FONTS["subheading"],
                                bg=COLORS["accent"], fg=COLORS["text_bright"],
                                activebackground=COLORS["accent_hover"], bd=0, padx=16, pady=6,
                                cursor="hand2", command=self._refresh_all)
        refresh_btn.pack(side="right", padx=16)

        # Export button
        export_btn = tk.Button(bar, text="📤 Export CSV", font=FONTS["subheading"],
                               bg=COLORS["bg"], fg=COLORS["text"],
                               activebackground=COLORS["border"], bd=1, relief="solid",
                               padx=16, pady=6, cursor="hand2", command=self._export_report)
        export_btn.pack(side="right", padx=4)

    # Dashboard Tab 

    def _build_dashboard(self, parent):
        # Metrics row
        metrics_frame = tk.Frame(parent, bg=COLORS["bg"])
        metrics_frame.pack(fill="x", padx=8, pady=12)

        total = len(self.pipelines)
        success = sum(1 for p in self.pipelines if p["status"] == "Success")
        failed = sum(1 for p in self.pipelines if p["status"] == "Failed")
        running = sum(1 for p in self.pipelines if p["status"] == "Running")
        total_rows = sum(p["rows_processed"] for p in self.pipelines)
        dq_passed = sum(1 for d in self.dq_results if d["status"] == "Passed")

        metrics = [
            ("Total Pipelines", str(total), COLORS["info"]),
            ("Successful", str(success), COLORS["success"]),
            ("Failed", str(failed), COLORS["danger"]),
            ("Running", str(running), COLORS["warning"]),
            ("Rows Processed", f"{total_rows:,}", COLORS["accent"]),
            ("DQ Checks Passed", f"{dq_passed}/{len(self.dq_results)}", COLORS["success"]),
        ]

        for i, (label, value, color) in enumerate(metrics):
            metrics_frame.columnconfigure(i, weight=1)
            card = tk.Frame(metrics_frame, bg=COLORS["card"], highlightbackground=COLORS["border"],
                            highlightthickness=1)
            card.grid(row=0, column=i, padx=6, sticky="nsew")

            tk.Label(card, text=value, font=FONTS["metric_big"],
                     bg=COLORS["card"], fg=color).pack(pady=(16, 2))
            tk.Label(card, text=label, font=FONTS["metric_label"],
                     bg=COLORS["card"], fg=COLORS["text_dim"]).pack(pady=(0, 16))

        # Recent activity log
        log_frame = tk.Frame(parent, bg=COLORS["card"], highlightbackground=COLORS["border"],
                             highlightthickness=1)
        log_frame.pack(fill="both", expand=True, padx=14, pady=(4, 12))

        tk.Label(log_frame, text="📋 Recent Activity", font=FONTS["heading"],
                 bg=COLORS["card"], fg=COLORS["text"]).pack(anchor="w", padx=12, pady=(10, 6))

        self.log_text = tk.Text(log_frame, bg=COLORS["bg"], fg=COLORS["text"],
                                font=FONTS["mono_small"], bd=0, wrap="word", height=8,
                                insertbackground=COLORS["text"])
        self.log_text.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        self._populate_log()

        # System health bars
        self._build_health_section(parent)

    def _populate_log(self):
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", "end")
        for p in sorted(self.pipelines, key=lambda x: x["last_run"], reverse=True):
            icon = {"Success": "✅", "Failed": "❌", "Running": "🔄", "Queued": "⏳"}.get(p["status"], "•")
            line = f"  {icon}  [{p['last_run']}]  {p['name']}  →  {p['status']}"
            if p["rows_processed"]:
                line += f"  ({p['rows_processed']:,} rows)"
            self.log_text.insert("end", line + "\n")
        self.log_text.config(state="disabled")

    # Pipelines Tab 

    def _build_pipelines_tab(self, parent):
        toolbar = tk.Frame(parent, bg=COLORS["bg"])
        toolbar.pack(fill="x", padx=14, pady=(12, 6))

        tk.Label(toolbar, text="Pipeline Jobs", font=FONTS["heading"],
                 bg=COLORS["bg"], fg=COLORS["text"]).pack(side="left")

        tk.Button(toolbar, text="▶ Trigger Run", font=FONTS["body"],
                  bg=COLORS["accent"], fg=COLORS["text_bright"], bd=0,
                  padx=14, pady=5, cursor="hand2",
                  command=self._trigger_pipeline).pack(side="right", padx=4)

        # Treeview
        cols = ("id", "name", "status", "duration", "rows", "last_run", "schedule")
        tree_frame = tk.Frame(parent, bg=COLORS["bg"])
        tree_frame.pack(fill="both", expand=True, padx=14, pady=6)

        self.pipe_tree = ttk.Treeview(tree_frame, columns=cols, show="headings")
        headers = {"id": "ID", "name": "Pipeline", "status": "Status",
                   "duration": "Duration", "rows": "Rows", "last_run": "Last Run",
                   "schedule": "Schedule"}
        widths = {"id": 70, "name": 180, "status": 80, "duration": 80,
                  "rows": 110, "last_run": 140, "schedule": 120}
        for c in cols:
            self.pipe_tree.heading(c, text=headers[c])
            self.pipe_tree.column(c, width=widths[c], anchor="center" if c != "name" else "w")

        scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.pipe_tree.yview)
        self.pipe_tree.configure(yscrollcommand=scroll.set)
        self.pipe_tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        self._refresh_pipeline_tree()

    def _refresh_pipeline_tree(self):
        for item in self.pipe_tree.get_children():
            self.pipe_tree.delete(item)
        for p in self.pipelines:
            dur = f"{p['duration_sec']}s" if p["duration_sec"] else "—"
            rows = f"{p['rows_processed']:,}" if p["rows_processed"] else "—"
            self.pipe_tree.insert("", "end", values=(
                p["id"], p["name"], p["status"], dur, rows, p["last_run"], p["schedule"]
            ))

    def _trigger_pipeline(self):
        sel = self.pipe_tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Select a pipeline to trigger.")
            return
        idx = self.pipe_tree.index(sel[0])
        name = self.pipelines[idx]["name"]
        self.pipelines[idx]["status"] = "Running"
        self.pipelines[idx]["duration_sec"] = 0
        self.pipelines[idx]["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        self._refresh_pipeline_tree()
        messagebox.showinfo("Triggered", f"Pipeline '{name}' is now running.")

    # Connections Tab 

    def _build_connections_tab(self, parent):
        toolbar = tk.Frame(parent, bg=COLORS["bg"])
        toolbar.pack(fill="x", padx=14, pady=(12, 6))

        tk.Label(toolbar, text="Data Source Connections", font=FONTS["heading"],
                 bg=COLORS["bg"], fg=COLORS["text"]).pack(side="left")

        tk.Button(toolbar, text="🔍 Test All Connections", font=FONTS["body"],
                  bg=COLORS["info"], fg=COLORS["text_bright"], bd=0,
                  padx=14, pady=5, cursor="hand2",
                  command=self._test_all_connections).pack(side="right", padx=4)

        # Connection cards
        self.conn_frame = tk.Frame(parent, bg=COLORS["bg"])
        self.conn_frame.pack(fill="both", expand=True, padx=14, pady=6)

        self.conn_statuses = {}
        self._build_connection_cards()

    def _build_connection_cards(self):
        for widget in self.conn_frame.winfo_children():
            widget.destroy()

        canvas = tk.Canvas(self.conn_frame, bg=COLORS["bg"], highlightthickness=0)
        scrollbar = ttk.Scrollbar(self.conn_frame, orient="vertical", command=canvas.yview)
        inner = tk.Frame(canvas, bg=COLORS["bg"])

        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        for i, src in enumerate(DATA_SOURCES):
            status = self.conn_statuses.get(src["name"], "Unknown")
            color = {"Connected": COLORS["success"], "Failed": COLORS["danger"]}.get(status, COLORS["text_dim"])

            card = tk.Frame(inner, bg=COLORS["card"], highlightbackground=COLORS["border"],
                            highlightthickness=1)
            card.pack(fill="x", pady=4, padx=4)

            row = tk.Frame(card, bg=COLORS["card"])
            row.pack(fill="x", padx=14, pady=10)

            tk.Label(row, text=f"● {src['name']}", font=FONTS["subheading"],
                     bg=COLORS["card"], fg=color).pack(side="left")
            tk.Label(row, text=f"  {src['type']}  |  {src['host']}  |  DB: {src['db']}",
                     font=FONTS["small"], bg=COLORS["card"], fg=COLORS["text_dim"]).pack(side="left", padx=10)
            tk.Label(row, text=status, font=FONTS["body"],
                     bg=COLORS["card"], fg=color).pack(side="right")

    def _test_all_connections(self):
        """Simulate testing all connections with a progress effect."""
        for src in DATA_SOURCES:
            self.conn_statuses[src["name"]] = random.choice(["Connected", "Connected", "Connected", "Failed"])
        self._build_connection_cards()
        connected = sum(1 for v in self.conn_statuses.values() if v == "Connected")
        messagebox.showinfo("Connection Test",
                            f"Results: {connected}/{len(DATA_SOURCES)} connected successfully.")

    # Data Quality Tab 

    def _build_dq_tab(self, parent):
        toolbar = tk.Frame(parent, bg=COLORS["bg"])
        toolbar.pack(fill="x", padx=14, pady=(12, 6))

        tk.Label(toolbar, text="Data Quality Checks", font=FONTS["heading"],
                 bg=COLORS["bg"], fg=COLORS["text"]).pack(side="left")

        tk.Button(toolbar, text="▶ Run All Checks", font=FONTS["body"],
                  bg=COLORS["success"], fg=COLORS["text_bright"], bd=0,
                  padx=14, pady=5, cursor="hand2",
                  command=self._run_dq_checks).pack(side="right", padx=4)

        # Treeview for DQ results
        cols = ("check", "table", "status", "details", "run_time")
        tree_frame = tk.Frame(parent, bg=COLORS["bg"])
        tree_frame.pack(fill="both", expand=True, padx=14, pady=6)

        self.dq_tree = ttk.Treeview(tree_frame, columns=cols, show="headings")
        headers = {"check": "Check", "table": "Table", "status": "Status",
                   "details": "Details", "run_time": "Run Time"}
        widths = {"check": 250, "table": 130, "status": 80, "details": 200, "run_time": 80}
        for c in cols:
            self.dq_tree.heading(c, text=headers[c])
            self.dq_tree.column(c, width=widths[c], anchor="center" if c != "check" else "w")

        scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.dq_tree.yview)
        self.dq_tree.configure(yscrollcommand=scroll.set)
        self.dq_tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        self._refresh_dq_tree()

    def _refresh_dq_tree(self):
        for item in self.dq_tree.get_children():
            self.dq_tree.delete(item)
        for d in self.dq_results:
            self.dq_tree.insert("", "end", values=(
                d["check"], d["table"], d["status"], d["details"], d["run_time"]
            ))

    def _run_dq_checks(self):
        self.dq_results = generate_dq_results()
        self._refresh_dq_tree()
        passed = sum(1 for d in self.dq_results if d["status"] == "Passed")
        messagebox.showinfo("Data Quality", f"Checks complete: {passed}/{len(self.dq_results)} passed.")

    # Actions 

    def _refresh_all(self):
        self.pipelines = generate_pipelines(10)
        self.dq_results = generate_dq_results()
        self.conn_statuses = {}

        # Rebuild dashboard
        for w in self.tab_dash.winfo_children():
            w.destroy()
        self._build_dashboard(self.tab_dash)

        # Refresh pipelines tree
        self._refresh_pipeline_tree()

        # Rebuild connections
        self._build_connection_cards()

        # Refresh DQ tree
        self._refresh_dq_tree()

    def _export_report(self):
        """Export pipeline data to CSV."""
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=f"datapulse_report_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        )
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["ID", "Pipeline", "Status", "Duration(s)", "Rows", "Last Run", "Schedule"])
                for p in self.pipelines:
                    writer.writerow([p["id"], p["name"], p["status"], p["duration_sec"],
                                     p["rows_processed"], p["last_run"], p["schedule"]])
            messagebox.showinfo("Export", f"Report saved to:\n{path}")
        except IOError as e:
            messagebox.showerror("Export Error", str(e))

    # Schema Explorer Tab

    def _build_schema_tab(self, parent):
        toolbar = tk.Frame(parent, bg=COLORS["bg"])
        toolbar.pack(fill="x", padx=14, pady=(12, 6))
        tk.Label(toolbar, text="Schema Explorer", font=FONTS["heading"],
                 bg=COLORS["bg"], fg=COLORS["text"]).pack(side="left")

        paned = tk.PanedWindow(parent, orient="horizontal", bg=COLORS["border"],
                               sashwidth=4, bd=0)
        paned.pack(fill="both", expand=True, padx=14, pady=6)

        # Table list
        left = tk.Frame(paned, bg=COLORS["card"])
        paned.add(left, width=220)
        tk.Label(left, text="Tables", font=FONTS["subheading"],
                 bg=COLORS["card"], fg=COLORS["accent"]).pack(anchor="w", padx=12, pady=(10, 6))
        self.table_listbox = tk.Listbox(left, bg=COLORS["surface"], fg=COLORS["text"],
                                         font=FONTS["body"], selectbackground=COLORS["accent"],
                                         selectforeground=COLORS["bg"], bd=0, highlightthickness=0)
        self.table_listbox.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        for tbl in SCHEMA_DATA:
            self.table_listbox.insert("end", f"  📋 {tbl}")
        self.table_listbox.bind("<<ListboxSelect>>", self._on_table_select)

        # Column details
        right = tk.Frame(paned, bg=COLORS["card"])
        paned.add(right)
        tk.Label(right, text="Columns", font=FONTS["subheading"],
                 bg=COLORS["card"], fg=COLORS["accent"]).pack(anchor="w", padx=12, pady=(10, 6))
        self.col_text = tk.Text(right, bg=COLORS["surface"], fg=COLORS["text"],
                                font=FONTS["mono"], bd=0, wrap="word",
                                insertbackground=COLORS["text"])
        self.col_text.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        self.col_text.insert("1.0", "  Select a table to view its columns.")
        self.col_text.config(state="disabled")

    def _on_table_select(self, event):
        sel = self.table_listbox.curselection()
        if not sel:
            return
        tbl_name = list(SCHEMA_DATA.keys())[sel[0]]
        cols = SCHEMA_DATA[tbl_name]
        self.col_text.config(state="normal")
        self.col_text.delete("1.0", "end")
        self.col_text.insert("end", f"  Table: {tbl_name}\n")
        self.col_text.insert("end", f"  Columns: {len(cols)}\n\n")
        for i, col in enumerate(cols, 1):
            marker = "🔑" if "PK" in col else ("🔗" if "FK" in col else "  ")
            self.col_text.insert("end", f"  {marker} {i}. {col}\n")
        self.col_text.config(state="disabled")

    # System Health (Dashboard) 

    def _build_health_section(self, parent):
        frame = tk.Frame(parent, bg=COLORS["card"], highlightbackground=COLORS["border"],
                         highlightthickness=1)
        frame.pack(fill="x", padx=14, pady=(4, 12))
        tk.Label(frame, text="🖥️ System Health", font=FONTS["heading"],
                 bg=COLORS["card"], fg=COLORS["text"]).pack(anchor="w", padx=12, pady=(10, 8))

        metrics = [("CPU Usage", random.randint(15, 85), COLORS["info"]),
                   ("Memory", random.randint(30, 90), COLORS["warning"]),
                   ("Disk I/O", random.randint(10, 70), COLORS["accent"])]

        for label, pct, color in metrics:
            row = tk.Frame(frame, bg=COLORS["card"])
            row.pack(fill="x", padx=12, pady=3)
            tk.Label(row, text=label, font=FONTS["small"], width=12, anchor="w",
                     bg=COLORS["card"], fg=COLORS["text_dim"]).pack(side="left")
            bar_bg = tk.Frame(row, bg=COLORS["bar_bg"], height=14)
            bar_bg.pack(side="left", fill="x", expand=True, padx=(4, 8))
            bar_bg.update_idletasks()
            bar_fill = tk.Frame(bar_bg, bg=color, height=14)
            bar_fill.place(relwidth=pct / 100, relheight=1.0)
            tk.Label(row, text=f"{pct}%", font=FONTS["small"], width=5,
                     bg=COLORS["card"], fg=color).pack(side="right")

        tk.Frame(frame, height=8, bg=COLORS["card"]).pack()

    # Public Accessors (for testing) 

    def get_pipeline_count(self):
        return len(self.pipelines)

    def get_success_count(self):
        return sum(1 for p in self.pipelines if p["status"] == "Success")

    def get_failed_count(self):
        return sum(1 for p in self.pipelines if p["status"] == "Failed")

    def get_dq_pass_rate(self):
        if not self.dq_results:
            return 0.0
        passed = sum(1 for d in self.dq_results if d["status"] == "Passed")
        return passed / len(self.dq_results)


def main():
    """Entry point for DataPulse."""
    root = tk.Tk()
    DataPulseApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
