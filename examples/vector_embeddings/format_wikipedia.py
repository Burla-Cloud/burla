import bz2
import subprocess
import xml.etree.ElementTree as xml_tree
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from google.cloud import storage
from burla import remote_parallel_map


def download_and_unzip(bucket, blob_name):
    xml_file_path = blob_name.removesuffix(".bz2")
    if Path(xml_file_path).exists():
        return

    if not Path(blob_name).exists():
        print("Downloading compressed XML from GCS...")
        bucket.blob(blob_name).download_to_filename(blob_name)

    print("Unzipping...")
    command = f"pbzip2 -d -p80 -c {blob_name} > {xml_file_path}"
    process = subprocess.run(command, shell=True, stderr=subprocess.PIPE, text=True)
    if process.returncode != 0:
        raise RuntimeError(f"pbzip2 failed with error: {process.stderr}")
    print("Download and unzip complete.")


def articles_to_blobs(blob_name):
    bucket = storage.Client().bucket("wikipedia-vectordb-demo")
    download_and_unzip(bucket, blob_name)
    executor = ThreadPoolExecutor(max_workers=80)
    xml_file_path = blob_name.removesuffix(".bz2")
    context = xml_tree.iterparse(xml_file_path, events=("end",))

    # MUST cycle sequentially through xml file
    for event, element in context:
        if element.tag.endswith("page"):
            title_element = element.find("./{*}title")
            id_element = element.find("./{*}id")
            text_element = element.find(".//{*}text")
            if title_element is not None and id_element is not None and text_element is not None:
                article_id = id_element.text or "noid"
                text = text_element.text or ""
                blob = bucket.blob(f"articles/{article_id}.txt")
                executor.submit(blob.upload_from_string, text.encode("utf-8"))
            element.clear()
    executor.shutdown(wait=True)
    print("All articles uploaded.")


remote_parallel_map(articles_to_blobs, ["enwiki-latest-pages-articles.xml.bz2"])
