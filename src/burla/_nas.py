import requests
import json
from typing import Optional
from pathlib import Path

from yaspin import yaspin
from appdirs import user_config_dir

from burla import _BURLA_SERVICE_URL
from burla._auth import login_required, auth_headers_from_local_config, current_user

BYTES_HEADER = {"Content-Type": "application/octet-stream"}
CONFIG_PATH = Path(user_config_dir(appname="burla", appauthor="burla"))
CURRENT_DIR_PATH = CONFIG_PATH / Path("current_dir.json")


def _get_current_dir():
    CONFIG_PATH.mkdir(parents=True, exist_ok=True)
    if not CURRENT_DIR_PATH.exists():
        CURRENT_DIR_PATH.touch()
        CURRENT_DIR_PATH.write_text(json.dumps({"current_dir": "/"}))
    return json.loads(CURRENT_DIR_PATH.read_text())["current_dir"]


@login_required
def rm(recurse: bool = False):
    pass


@login_required
def cd(folder: str):
    current_dir = _get_current_dir()
    current_dir = current_dir[:-1] if current_dir.endswith("/") else current_dir

    if folder == ".":
        print(f"{current_user()} {current_dir} % ")
        return
    elif folder == ".." and current_dir == "/":
        return
    elif folder == "..":
        target_dir = "/".join(current_dir.split("/")[:-1])
        target_dir = "/" if target_dir == "" else target_dir
    elif folder.startswith("/"):
        target_dir = folder
    else:
        target_dir = current_dir + f"/{folder}"

    url = f"{_BURLA_SERVICE_URL}/v1/bcs/object_info"
    headers = auth_headers_from_local_config()
    response = requests.post(url, json={"remote_path": target_dir}, headers=headers)
    response.raise_for_status()
    remote_object_info = response.json()
    if remote_object_info.get("type") == "folder":
        CURRENT_DIR_PATH.write_text(json.dumps({"current_dir": target_dir}))
        print(f"{current_user()} {target_dir} % ")
    elif remote_object_info.get("type") != "folder":
        print(f"burla nas cd: no such file or directory: {target_dir}")


@login_required
def ls():
    current_dir = _get_current_dir()
    current_dir = "/" if current_dir == "" else current_dir
    print(f"{current_user()} {current_dir} % ")

    url = f"{_BURLA_SERVICE_URL}/v1/bcs/object_info"
    headers = auth_headers_from_local_config()
    response = requests.post(url, json={"remote_path": current_dir}, headers=headers)
    response.raise_for_status()
    remote_object_info = response.json()
    if not remote_object_info.get("type") == "folder":
        raise Exception("Current directory is not a folder??")

    print("\t".join(remote_object_info["contents_relative_paths"]))


@login_required
def upload(local_path: str, remote_folder: Optional[str] = None, recurse: bool = False):
    local_path = Path(local_path)
    if remote_folder is None:
        remote_folder = Path(_get_current_dir())
    elif remote_folder.startswith("/"):
        remote_folder = Path(remote_folder)
    else:
        remote_folder = Path(_get_current_dir()) / Path(remote_folder)

    if not local_path.exists():
        print(f"Path {local_path} does not exist.")
        return
    elif local_path.is_dir() and (not recurse):
        msg = f"Path {local_path} is a directory, "
        msg += "please use `--recurse` to recursively upload all files in a directory."
        print(msg)
        return
    elif local_path.is_file() and recurse:
        print(f"`--recurse` option was used but path {local_path} is not a directory.")
        return
    elif not (local_path.is_dir() or local_path.is_file()):
        msg = f"Cannot upload {local_path}, is not file or folder (may be symlink or block device)"
        print(msg)
        return

    url = f"{_BURLA_SERVICE_URL}/v1/bcs/object_info"
    headers = auth_headers_from_local_config()
    response = requests.post(url, json={"remote_path": str(remote_folder)}, headers=headers)
    response.raise_for_status()
    remote_object_info = response.json()
    if remote_object_info.get("type") == "file":
        print(f"Destination must be a folder but {remote_folder} is a file.")
        return

    spinner = yaspin()
    spinner.text = f"Uploading {local_path} ..."
    spinner.start()

    try:
        if local_path.is_dir():
            filepaths = []
            local_filepaths = [subpath for subpath in local_path.rglob("*") if subpath.is_file()]
            for local_filepath in local_filepaths:
                remote_filepath = remote_folder / local_filepath
                fp = dict(remote_filepath=str(remote_filepath), local_filepath=str(local_filepath))
                filepaths.append(fp)
        elif local_path.is_file():
            remote_filepath = remote_folder / local_path.name
            filepaths = [dict(remote_filepath=str(remote_filepath), local_filepath=str(local_path))]

        headers = auth_headers_from_local_config()
        url = f"{_BURLA_SERVICE_URL}/v1/bcs/upload_urls"
        payload = {"remote_filepaths": [fp["remote_filepath"] for fp in filepaths]}
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

        def read_in_chunks(file, chunk_size=8192):
            while True:
                data = file.read(chunk_size)
                if not data:
                    break
                yield data

        for remote_filepath, url in response.json()["paths_to_urls"]:
            filepath = [fp for fp in filepaths if fp["remote_filepath"] == remote_filepath][0]
            spinner.text = f"Uploading {filepath['local_filepath']} ..."
            with Path(filepath["local_filepath"]).open("rb") as file:
                file_content_iterator = read_in_chunks(file)
                response = requests.put(url, headers=BYTES_HEADER, data=file_content_iterator)
                response.raise_for_status()
    except Exception as e:
        spinner.stop()
        raise e

    spinner.text = "Done!"
    spinner.ok("✔")


@login_required
def download(remote_path: str, local_folder: Optional[str] = None, recurse: bool = False):
    headers = auth_headers_from_local_config()

    current_dir = _get_current_dir()
    current_dir = current_dir[:-1] if current_dir.endswith("/") else current_dir
    current_dir = "/" if current_dir == "" else current_dir
    is_relative_path = not remote_path.startswith("/")
    remote_path = Path(current_dir) / Path(remote_path) if is_relative_path else Path(remote_path)
    local_folder = Path(local_folder) if local_folder else Path.cwd()

    url = f"{_BURLA_SERVICE_URL}/v1/bcs/object_info"
    response = requests.post(url, json={"remote_path": str(remote_path)}, headers=headers)
    response.raise_for_status()
    remote_object_info = response.json()
    is_folder = remote_object_info.get("type") == "folder"
    is_file = remote_object_info.get("type") == "file"

    if not local_folder.is_dir():
        print(f"local_folder `{local_folder}` is not a folder.")
        return
    elif not remote_object_info.get("exists"):
        print(f"Path `{remote_path}` not found on remote server.")
        return
    elif not (is_folder or is_file):
        raise Exception("Unexpected response from server, please email jake@burla.dev")
    elif is_folder and (not recurse):
        msg = f"Path `{remote_path}` is a directory, "
        msg += "please use `--recurse` to recursively download all files in a directory."
        print(msg)
        return
    elif is_file and recurse:
        msg = f"`--recurse` option was used but remote path `{remote_path}` is not a directory."
        print(msg)
        return
    elif is_folder and remote_object_info["contents_relative_paths"] == []:
        print(f"Remote folder `{remote_path}` is empty!")
        return

    spinner = yaspin()
    spinner.text = f"Downloading {remote_path} ..."
    spinner.start()

    try:
        if is_file:
            blob_paths_to_download = [remote_path]
        elif is_folder:
            folder_contents = remote_object_info["contents_relative_paths"]
            blob_paths_to_download = [remote_path / Path(path) for path in folder_contents]

        url = f"{_BURLA_SERVICE_URL}/v1/bcs/download_urls"
        payload = {"remote_paths": [str(path) for path in blob_paths_to_download]}
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        paths_to_urls = response.json()["paths_to_urls"]

        for remote_filepath, download_url in paths_to_urls:
            local_path = local_folder / Path(remote_filepath).relative_to(remote_path.parent)
            local_path.parent.mkdir(parents=True, exist_ok=True)

            spinner.text = f"Downloading {remote_path} to {local_path}"
            with requests.get(download_url, headers=BYTES_HEADER, stream=True) as response:
                response.raise_for_status()
                with local_path.open("wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
    except Exception as e:
        spinner.stop()
        raise e

    spinner.text = "Done!"
    spinner.ok("✔")
