import json
import threading
import uvicorn
import streamlit as st
from streamlit_mermaid import st_mermaid
from workflow_backend import engine
from api import app  # FastAPI app

# --- Start FastAPI in background thread ---
def run_api():
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")

if "api_started" not in st.session_state:
    threading.Thread(target=run_api, daemon=True).start()
    st.session_state.api_started = True

# --- Auto-refresh every 5 seconds ---
#st_autorefresh = st.experimental_autorefresh(interval=5000, key="datarefresh")

st.set_page_config(page_title="Claim Workflow Manager", layout="wide")
st.title("📋 Claim Workflow Manager")
# --- Manual refresh button ---
if st.button("🔄 Refresh Workflows"):
    st.rerun()

# --- Mermaid diagram helper ---
def workflow_mermaid(meta: dict) -> str:
    node_ids = {
        "Validate Request": "A",
        "Gather Claim Info": "B",
        "Identify Accounts & Process Decision": "D",
        "Cancel CWD Request": "C",
        "Hold Request": "E",
        "Apply Temporary Suppression": "F",
        "Fulfill Case and Detect": "G",
        "END": "H",
    }
    visited_styles = []
    for step in meta.get("steps_history", []):
        node = step.get("node")
        if node in node_ids:
            visited_styles.append(f"style {node_ids[node]} fill:#e3f2fd,stroke:#1565c0,stroke-width:2px;")
    current_node = meta.get("last_node") or (
        meta["steps_history"][0]["node"] if meta.get("steps_history") else "Validate Request"
    )
    if current_node in node_ids:
        visited_styles.append(f"style {node_ids[current_node]} fill:#ffecb3,stroke:#ff6f00,stroke-width:4px;")
    styles = "\n    ".join(visited_styles)
    return f"""
graph TD
    A[Validate Request] -->|yes| B[Gather Claim Info]
    A -->|no| C[Cancel CWD Request]
    B --> D[Identify Accounts & Process Decision]
    D -->|cancel| C
    D -->|hold| E[Hold Request]
    D -->|suppress| F[Apply Temporary Suppression]
    E -->|resume| F
    E -->|abort| C
    F -->|yes| G[Fulfill Case and Detect]
    F -->|no| C
    G --> H[END]
    C --> H
    {styles}
"""



# --- Start workflow ---
st.header("🚀 Start a New Workflow")
with st.form("start_form"):
    customer_id = st.text_input("Customer ID")
    started_by = st.text_input("Started by (user)")
    start_btn = st.form_submit_button("Start Workflow")
    if start_btn:
        if not customer_id or not started_by:
            st.error("Please provide both Customer ID and Started by.")
        else:
            inst_id, out = engine.start(customer_id, started_by)
            st.success(f"Workflow {inst_id} started")
            st.json(out)

# --- Resume workflow (UI) ---
st.header("⏩ Resume a Workflow")
with st.form("resume_form"):
    resume_id = st.text_input("Instance ID to resume")
    actor = st.text_input("Actor resuming")
    updates_str = st.text_area("Updates (JSON)", '{"validate": "yes"}')
    resume_btn = st.form_submit_button("Resume Workflow")
    if resume_btn:
        try:
            updates = json.loads(updates_str or "{}")
            out = engine.resume(resume_id, actor, updates)
            st.success(f"Workflow {resume_id} resumed by {actor}")
            st.json(out)
        except json.JSONDecodeError:
            st.error("Invalid JSON in updates field.")

# --- All workflows ---
st.header("📋 All Workflows")
instances = engine.list_instances()
if instances:
    for m in instances:
        meta = m.to_dict()
        if not meta.get("last_node"):
            meta["last_node"] = "Validate Request"
        with st.expander(f"Workflow {meta['instance_id']} — {meta['status']}"):
            st.write("**Customer ID:**", meta["customer_id"])
            st.write("**Workflow Name:**", meta["workflow_name"])
            st.write("**Last Node:**", meta["last_node"])
            st.write("**Status:**", meta["status"])
            st.markdown("**Steps History:**")
            if meta["steps_history"]:
                for i, step in enumerate(meta["steps_history"], start=1):
                    st.markdown(f"**Step {i}:**")
                    st.json(step)
            else:
                st.write("_No steps recorded yet_")
            st.markdown("**Workflow Diagram:**")
            st_mermaid(workflow_mermaid(meta))
else:
    st.info("No workflows yet.")