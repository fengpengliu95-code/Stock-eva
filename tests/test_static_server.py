import http.client
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Self

import pytest

ROOT = Path(__file__).parents[1]


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
) -> tuple[int, list[tuple[str, str]], bytes]:
    connection = http.client.HTTPConnection(*address, timeout=5)
    try:
        connection.request(method, path)
        response = connection.getresponse()
        return response.status, response.getheaders(), response.read()
    finally:
        connection.close()


def header_values(
    headers: list[tuple[str, str]],
    name: str,
) -> list[str]:
    return [value for header_name, value in headers if header_name.lower() == name.lower()]


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
            assert header_values(headers, "Cache-Control") == ["no-store"]
            assert body


def test_head_preserves_static_metadata_without_a_body(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    payload = b"console.log('release');"
    (workspace / "app.js").write_bytes(payload)

    with running_static_server(tmp_path) as address:
        status, headers, body = request(address, "HEAD", "/workspace/app.js")

    assert status == 200
    assert header_values(headers, "Cache-Control") == ["no-store"]
    assert header_values(headers, "Content-Length") == [str(len(payload))]
    assert body == b""


def test_missing_static_resource_keeps_404_and_no_store(tmp_path: Path) -> None:
    with running_static_server(tmp_path) as address:
        status, headers, body = request(
            address,
            "GET",
            "/workspace/missing.js",
        )

    assert status == 404
    assert header_values(headers, "Cache-Control") == ["no-store"]
    assert body


def test_workspace_directory_redirect_and_index_have_one_no_store_header(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "index.html").write_text("<main>workspace</main>")

    with running_static_server(tmp_path) as address:
        redirect_status, redirect_headers, _ = request(
            address,
            "GET",
            "/workspace",
        )
        index_status, index_headers, index_body = request(
            address,
            "GET",
            "/workspace/",
        )

    assert redirect_status == 301
    assert header_values(redirect_headers, "Location") == ["/workspace/"]
    assert header_values(redirect_headers, "Cache-Control") == ["no-store"]
    assert index_status == 200
    assert header_values(index_headers, "Cache-Control") == ["no-store"]
    assert index_body == b"<main>workspace</main>"


def test_unsupported_method_keeps_501_and_one_no_store_header(
    tmp_path: Path,
) -> None:
    with running_static_server(tmp_path) as address:
        status, headers, body = request(address, "POST", "/workspace/")

    assert status == 501
    assert header_values(headers, "Cache-Control") == ["no-store"]
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


def test_server_pins_symlink_to_release_present_at_startup(
    tmp_path: Path,
) -> None:
    release_a = tmp_path / "releases" / "a"
    release_b = tmp_path / "releases" / "b"
    for release, content in (
        (release_a, "release-a"),
        (release_b, "release-b"),
    ):
        public = release / "public"
        public.mkdir(parents=True)
        (public / "sentinel.txt").write_text(content)
    current = tmp_path / "current"
    current.symlink_to(release_a, target_is_directory=True)

    with running_static_server(current / "public") as old_address:
        _, old_headers, old_body = request(
            old_address,
            "GET",
            "/sentinel.txt",
        )
        next_current = tmp_path / ".current-next"
        next_current.symlink_to(release_b, target_is_directory=True)
        next_current.replace(current)
        _, pinned_headers, pinned_body = request(
            old_address,
            "GET",
            "/sentinel.txt",
        )
        with running_static_server(current / "public") as new_address:
            _, new_headers, new_body = request(
                new_address,
                "GET",
                "/sentinel.txt",
            )

    assert old_body == b"release-a"
    assert pinned_body == b"release-a"
    assert new_body == b"release-b"
    for headers in (old_headers, pinned_headers, new_headers):
        assert header_values(headers, "Cache-Control") == ["no-store"]


def test_server_requires_existing_directory_at_startup(tmp_path: Path) -> None:
    static_server = import_module("backend.app.static_server")

    with pytest.raises(FileNotFoundError):
        static_server.create_server(
            host="127.0.0.1",
            port=0,
            directory=tmp_path / "missing",
        )


@pytest.mark.parametrize(
    "host",
    [
        "::1",
        "localhost",
        "0.0.0.0",
        "192.0.2.10",
    ],
)
def test_server_rejects_non_ipv4_loopback_bind_address(
    tmp_path: Path,
    host: str,
) -> None:
    static_server = import_module("backend.app.static_server")

    with pytest.raises(ValueError, match="IPv4 loopback"):
        static_server.create_server(
            host=host,
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


def test_readme_uses_the_loopback_no_store_static_server() -> None:
    readme = (ROOT / "README.md").read_text()

    assert "python3 -m http.server 8080" not in readme
    assert "python -m backend.app.static_server" in readme
    assert "--host 127.0.0.1" in readme
    assert "Cache-Control: no-store" in readme
