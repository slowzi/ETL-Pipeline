#!/bin/bash

# Virtual Environment Path
VENV_PATH="/home/dims/directory_env/ETLenv/venv/bin/activate"

# Activate Virtual Environment
source "$VENV_PATH"

# Set Python script
PYTHON_SCRIPT="/home/dims/pipeline_project/pipeline.py"

# Run Python Script and insert log
python "$PYTHON_SCRIPT" >> /home/dims/pipeline_project/log/luigi_process.log 2>&1

# Luigi info simple log
dt=$(date '+%d/%m/%Y %H:%M:%S');
echo "Luigi started at ${dt}" >> /home/dims/pipeline_project/log/luigi_info.log