"""Publish the fabric/ items (Lakehouse, Notebooks, SemanticModel) to a Fabric workspace with fabric-cicd.

NOT run by this project: no Fabric tenant was available. Usage after `az login` (or service principal env vars):
    pip install -r requirements-deploy.txt
    FABRIC_WORKSPACE_ID=<guid> python scripts/deploy_fabric.py
"""
import os
from pathlib import Path

from azure.identity import DefaultAzureCredential
from fabric_cicd import FabricWorkspace, publish_all_items

ROOT = Path(__file__).resolve().parents[1]

ws = FabricWorkspace(
    workspace_id=os.environ["FABRIC_WORKSPACE_ID"],
    repository_directory=str(ROOT / "fabric"),
    environment=os.environ.get("FABRIC_ENV", "DEV"),
    item_type_in_scope=["Lakehouse", "Notebook", "SemanticModel"],
    token_credential=DefaultAzureCredential(),
)
publish_all_items(ws)
