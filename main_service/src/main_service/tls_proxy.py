"""
TLS terminator for client-hosted mode.

On a head VM, Caddy terminates cluster-CA TLS on the internal port. When
main_service runs inside the burla pip package there is no Caddy, so this
tiny asyncio proxy does the same job: accept TLS (head cert, signed by the
cluster CA) on the internal port and pipe plaintext to the local uvicorn
port. Node traffic arrives here through the frp relay tunnel.
"""

import asyncio
import ssl

from main_service.transport_tls import HEAD_CERT_PATH, HEAD_KEY_PATH

# Loopback (host, port) addresses of this proxy's live connections to uvicorn.
# Relay traffic and the owner's own browser both reach uvicorn from 127.0.0.1;
# the auth middleware uses this set to tell them apart (relay traffic must
# still log in, the owner's local traffic must not have to).
RELAY_CLIENT_ADDRESSES: set[tuple[str, int]] = set()


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        while True:
            data = await reader.read(64 * 1024)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError, ssl.SSLError):
        pass
    finally:
        writer.close()


async def start_tls_proxy(listen_port: int, forward_port: int):
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(certfile=HEAD_CERT_PATH, keyfile=HEAD_KEY_PATH)

    async def handle(client_reader, client_writer):
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                "127.0.0.1", forward_port
            )
        except OSError:
            client_writer.close()
            return

        sockname = upstream_writer.get_extra_info("sockname")
        proxy_address = (sockname[0], sockname[1])
        RELAY_CLIENT_ADDRESSES.add(proxy_address)

        async def pipe_both_directions():
            try:
                await asyncio.gather(
                    _pipe(client_reader, upstream_writer),
                    _pipe(upstream_reader, client_writer),
                )
            finally:
                RELAY_CLIENT_ADDRESSES.discard(proxy_address)

        asyncio.create_task(pipe_both_directions())

    return await asyncio.start_server(
        handle, host="0.0.0.0", port=listen_port, ssl=ssl_context
    )
