import argparse
import ipaddress
from collections.abc import Sequence
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class NoStoreStaticRequestHandler(SimpleHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()


def create_server(
    *,
    host: str,
    port: int,
    directory: Path,
) -> ThreadingHTTPServer:
    try:
        is_loopback = ipaddress.ip_address(host).is_loopback
    except ValueError as error:
        raise ValueError("static server host must be a loopback IP address") from error
    if not is_loopback:
        raise ValueError("static server host must be a loopback IP address")

    handler = partial(
        NoStoreStaticRequestHandler,
        directory=str(directory),
    )
    return ThreadingHTTPServer((host, port), handler)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Serve Stock EVA static files locally.")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--directory", required=True, type=Path)
    args = parser.parse_args(argv)

    with create_server(
        host=args.host,
        port=args.port,
        directory=args.directory,
    ) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
