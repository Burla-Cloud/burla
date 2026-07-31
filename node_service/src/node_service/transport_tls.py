import asyncio
import os
import subprocess
from pathlib import Path

from node_service import INSTANCE_NAME, head_client

TLS_KEY_PATH = Path(os.environ["NODE_TLS_KEY_PATH"])
TLS_CERT_PATH = Path(os.environ["NODE_TLS_CERT_PATH"])
RENEWAL_INTERVAL_SEC = 3 * 24 * 60 * 60


async def renew_certificate():
    csr_path = TLS_CERT_PATH.with_suffix(".csr")
    await asyncio.to_thread(
        subprocess.run,
        [
            "openssl",
            "req",
            "-new",
            "-key",
            str(TLS_KEY_PATH),
            "-subj",
            f"/CN={INSTANCE_NAME}",
            "-out",
            str(csr_path),
        ],
        check=True,
    )
    certificate = await head_client.issue_certificate(csr_path.read_text())
    next_cert_path = TLS_CERT_PATH.with_suffix(".next")
    next_cert_path.write_text(certificate)
    os.replace(next_cert_path, TLS_CERT_PATH)
    await asyncio.to_thread(
        subprocess.run,
        ["docker", "restart", "burla-node-caddy"],
        check=True,
    )


async def certificate_renewal_loop():
    while True:
        await asyncio.sleep(RENEWAL_INTERVAL_SEC)
        renewed = False
        while not renewed:
            try:
                await renew_certificate()
                renewed = True
            except Exception as error:
                print(f"Node certificate renewal failed: {error}")
                await asyncio.sleep(60 * 60)
