import os
import sys


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
    if command == "delete_booting_nodes":
        delete_booting_nodes()
