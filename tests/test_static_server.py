import http.client
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Self

import pytest


@contextmanager
def running_static_server(
    directory: Path,
) -> Iterator[tuple[str, int]]:
    static_server = import_module("backend.app.static_server")
    server = static_server.create_server(
        host="127.0.0.1",
        port=0,
        directory=directory,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield host, port
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def request(
    address: tuple[str, int],
    method: str,
    path: str,
) -> tuple[int, dict[str, str], bytes]:
    connection = http.client.HTTPConnection(*address, timeout=5)
    try:
        connection.request(method, path)
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        connection.close()


def test_workspace_get_responses_are_never_cached(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    assets = workspace / "assets"
    assets.mkdir(parents=True)
    (workspace / "index.html").write_text("<main>new release</main>")
    (workspace / "app.js").write_text("window.release = 'new';")
    (assets / "app.css").write_text("body { color: black; }")

    with running_static_server(tmp_path) as address:
        for path in (
            "/workspace/index.html",
            "/workspace/app.js",
            "/workspace/assets/app.css",
        ):
            status, headers, body = request(address, "GET", path)

            assert status == 200
            assert headers["Cache-Control"] == "no-store"
            assert body


def test_head_preserves_static_metadata_without_a_body(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    payload = b"console.log('release');"
    (workspace / "app.js").write_bytes(payload)

    with running_static_server(tmp_path) as address:
        status, headers, body = request(address, "HEAD", "/workspace/app.js")

    assert status == 200
    assert headers["Cache-Control"] == "no-store"
    assert headers["Content-Length"] == str(len(payload))
    assert body == b""


def test_missing_static_resource_keeps_404_and_no_store(tmp_path: Path) -> None:
    with running_static_server(tmp_path) as address:
        status, headers, body = request(
            address,
            "GET",
            "/workspace/missing.js",
        )

    assert status == 404
    assert headers["Cache-Control"] == "no-store"
    assert body


def test_server_binds_requested_host_and_serves_only_given_directory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    public_root = tmp_path / "release" / "public"
    public_root.mkdir(parents=True)
    (public_root / "sentinel.txt").write_text("selected release")
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    (unrelated / "sentinel.txt").write_text("wrong directory")
    monkeypatch.chdir(unrelated)

    with running_static_server(public_root) as address:
        status, _, body = request(address, "GET", "/sentinel.txt")

    assert address[0] == "127.0.0.1"
    assert status == 200
    assert body == b"selected release"


def test_server_rejects_non_loopback_bind_address(tmp_path: Path) -> None:
    static_server = import_module("backend.app.static_server")

    with pytest.raises(ValueError, match="loopback"):
        static_server.create_server(
            host="0.0.0.0",
            port=0,
            directory=tmp_path,
        )


def test_cli_passes_host_port_and_directory_to_server(
    tmp_path: Path,
    monkeypatch,
) -> None:
    static_server = import_module("backend.app.static_server")
    received: dict[str, object] = {}

    class FakeServer:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *args: object) -> None:
            received["closed"] = True

        def serve_forever(self) -> None:
            received["served"] = True

    def fake_create_server(
        *,
        host: str,
        port: int,
        directory: Path,
    ) -> FakeServer:
        received.update(host=host, port=port, directory=directory)
        return FakeServer()

    monkeypatch.setattr(static_server, "create_server", fake_create_server)

    static_server.main(
        [
            "--host",
            "127.0.0.1",
            "--port",
            "18080",
            "--directory",
            str(tmp_path),
        ]
    )

    assert received == {
        "host": "127.0.0.1",
        "port": 18080,
        "directory": tmp_path,
        "served": True,
        "closed": True,
    }
