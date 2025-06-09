import streamlit as st
from pyvis.network import Network
import streamlit.components.v1 as components
import tempfile
import os

# ETL Job definitions
jobs = [
    ("PL_FULLLOAD_OMS_TABLES", "Full Load", "Run time",4),
    ("PL_BSEG", "Incremental Load", 30),
    ("PL_DYNAMIC_FULLLOAD_JOB_4", "Full Load", 55),
    ("PL_COBK, PL_COSS, PL_COEP, PL_COSP, PL_JCDS", "Incremental Load", 90),
    ("PL_DYNAMIC_FULLLOAD_JOB_6", "Full Load", 200),
    ("PL_DYNAMIC_FULLLOAD_JOB_1", "Full Load", 10),
    ("PL_MSEG, PL_AFKO, PL_MKPF, PL_AUFK, PL_MARA, PL_BKPF", "Incremental Load", 40),
    ("PL_SAP_DYNAMIC_FULLLOAD_JOB_5", "Full Load", 3),
    ("PL_DYNAMIC_FULLLOAD_JOB_7", "Full Load", 115),
    ("PL_DYNAMIC_FULLLOAD_JOB_3", "Full Load", 115),
    ("PL_FI_SQL_Views", "Full Load", 1),
    ("PL_DYNAMIC_FULLLOAD_JOB_2", "Full Load", 25),
]

# Streamlit UI setup
st.set_page_config(page_title="ADF Lineage Flow", layout="wide")
st.title("Interactive ETL Lineage Flow -  ADF Jobs")

# Initialize PyVis Network
net = Network(height='750px', width='100%', bgcolor='#1e1e1e', font_color='white', directed=True)

# Apply corrected JSON-based hierarchical layout options
net.set_options("""
{
  "layout": {
    "hierarchical": {
      "enabled": true,
      "direction": "LR",
      "sortMethod": "directed"
    }
  },
  "edges": {
    "arrows": {
      "to": {"enabled": true}
    },
    "smooth": {
      "enabled": true,
      "type": "curvedCW",
      "roundness": 0.5
    }
  },
  "nodes": {
    "shape": "dot",
    "size": 18,
    "font": {
      "size": 14,
      "color": "white"
    }
  },
  "physics": {
    "enabled": false
  }
}
""")

# Add root node
net.add_node("Asarco ADF Jobs", shape='dot', size=30, color='skyblue', title="ADF Root Job")

# Add job nodes and edges
for job, job_type, runtime in jobs:
    color = "green" if job_type == "Incremental Load" else "orange"
    label = f"{job} ({job_type})"
    title = f"{job}<br>Type: {job_type}<br>Runtime: {runtime} mins"
    net.add_node(job, label=label, title=title, color=color)
    net.add_edge("Asarco ADF Jobs", job)

# Display the graph in Streamlit using HTML component
with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
    tmp_path = tmp_file.name
    net.write_html(tmp_path)
    with open(tmp_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    components.html(html_content, height=800)
    os.remove(tmp_path)
