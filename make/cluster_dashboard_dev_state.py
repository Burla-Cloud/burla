import json
import os
import sys
from pathlib import Path

from platformdirs import user_config_dir

STATE_PATH = Path(".burla_cluster_dashboard_url_before_dev")
CREDENTIALS_PATH = Path(user_config_dir("burla", "burla")) / "burla_credentials.json"
LOCAL_CLUSTER_DASHBOARD_URL = "http://localhost:5001"


def point_at_local_dashboard():
    CREDENTIALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    config = json.loads(CREDENTIALS_PATH.read_text()) if CREDENTIALS_PATH.exists() else {}
    config["cluster_dashboard_url"] = LOCAL_CLUSTER_DASHBOARD_URL
    CREDENTIALS_PATH.write_text(json.dumps(config))


def save():
    if not CREDENTIALS_PATH.exists():
        STATE_PATH.write_text("__NO_FILE__")
    else:
        data = json.loads(CREDENTIALS_PATH.read_text())
        if "cluster_dashboard_url" not in data:
            STATE_PATH.write_text("__NO_KEY__")
        else:
            STATE_PATH.write_text(data["cluster_dashboard_url"])


def restore():
    if not STATE_PATH.exists():
        return
    previous = STATE_PATH.read_text()
    STATE_PATH.unlink()
    if previous == "__NO_FILE__":
        if CREDENTIALS_PATH.exists():
            CREDENTIALS_PATH.unlink()
        return
    if previous == "__NO_KEY__":
        if CREDENTIALS_PATH.exists():
            data = json.loads(CREDENTIALS_PATH.read_text())
            data.pop("cluster_dashboard_url", None)
            CREDENTIALS_PATH.write_text(json.dumps(data))
        return
    if CREDENTIALS_PATH.exists():
        data = json.loads(CREDENTIALS_PATH.read_text())
        data["cluster_dashboard_url"] = previous
        CREDENTIALS_PATH.write_text(json.dumps(data))


def delete_booting_nodes():
    from google.cloud import firestore
    from google.cloud.firestore_v1 import FieldFilter

    database = firestore.Client(project=os.environ["PROJECT_ID"], database="burla")
    booting_filter = FieldFilter("status", "==", "BOOTING")
    booting_nodes = database.collection("nodes").where(filter=booting_filter).get()
    if not booting_nodes:
        print("No booting nodes found")
    else:
        for document in booting_nodes:
            document.reference.delete()
            print(f"Deleted node doc: {document.id}")


if __name__ == "__main__":
    command = sys.argv[1]
    if command == "save":
        save()
    elif command == "restore":
        restore()
    elif command == "point":
        point_at_local_dashboard()
    elif command == "delete_booting_nodes":
        delete_booting_nodes()
