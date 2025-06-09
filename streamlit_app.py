import streamlit as st
from pyvis.network import Network
import streamlit.components.v1 as components
import tempfile
import os

# ETL Job definitions
jobs = [
    ("PL_FULLLOAD_OMS_TABLES", "Full Load", 4),
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

# Page setup
st.set_page_config(page_title="ADF Lineage Flow", layout="wide")
st.title("Interactive ETL Lineage Flow - Asarco ADF Jobs")

# Create network
net = Network(height='700px', width='100%', bgcolor='#1e1e1e', font_color='white')
net.add_node("Asarco ADF Jobs", shape='dot', size=25, color='skyblue', title="Central ADF Job Hub")

# Add job nodes
for job, job_type, runtime in jobs:
    color = "green" if job_type == "Incremental Load" else "orange"
    label = f"{job}\n({job_type})"
    title = f"{job}<br>Type: {job_type}<br>Runtime: {runtime} mins"
    net.add_node(job, label=label, title=title, color=color)
    net.add_edge("Asarco ADF Jobs", job)

# Save and render using write_html (FIX for Streamlit Cloud)
with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
    tmp_path = tmp_file.name
    net.write_html(tmp_path)
    with open(tmp_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    components.html(html_content, height=750)
    os.remove(tmp_path)
