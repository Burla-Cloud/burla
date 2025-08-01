import xml.etree.ElementTree as xml_tree
from subprocess import Popen, PIPE, STDOUT
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from google.cloud import storage
from burla import remote_parallel_map


def execute_cmd_live_output(cmd: str):
    process = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT, text=True)
    for line in process.stdout:
        print(line.rstrip("\n"), end="", flush=True)
    process.wait()
    if process.returncode != 0:
        msg = f"pbzip2 failed with exit code {process.returncode}"
        raise RuntimeError(msg=f"{msg}, stderr: {process.stderr.read()}")


def download_and_unzip(bucket, blob_name):
    if not Path(blob_name).exists():
        print("Downloading compressed XML from GCS...")
        bucket.blob(blob_name).download_to_filename(blob_name)

    if not Path(blob_name.removesuffix(".bz2")).exists():
        print("Unzipping...")
        execute_cmd_live_output(f"pbzip2 -d -v -p80 {blob_name}")


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
