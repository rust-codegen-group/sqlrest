#!/usr/bin/env python3
"""Exercise checked-in examples in a fresh process/container and after restart.

Only owns its TemporaryDirectory and its child process/container. PostgreSQL
mode requires an explicitly disposable database URL; it leaves sample tables
there for caller-owned teardown. Never point it at a database containing data.
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def request(base, method, path, body=None, expected=200):
    data = None if body is None else json.dumps(body).encode()
    headers = {} if data is None else {"Content-Type": "application/json"}
    req = urllib.request.Request(base + path, data, headers, method=method)
    try:
        response = HTTP.open(req, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        value = json.load(response)
        if response.status != expected:
            raise AssertionError(f"{method} {path}: {response.status}: {value}")
        return value


def operation(management, name, action, success=True):
    accepted = request(management, "POST", f"/databases/{name}/{action}", expected=202)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        result = request(management, "GET", f"/databases/{name}/operations/{accepted['operation_id']}")
        if result["outcome"] != "running":
            assert (result["outcome"] == "succeeded") == success, result
            return result
        time.sleep(0.02)
    raise TimeoutError(f"{action}: polling deadline (not proof of failure or rollback)")


@contextlib.contextmanager
def server(binary, image, workspace):
    child = None
    container = None
    try:
        if image:
            container = subprocess.check_output([
                "docker", "run", "-d", "--user", f"{os.getuid()}:{os.getgid()}",
                "--mount", f"type=bind,src={workspace},dst=/workspace",
                "-p", "127.0.0.1::8080", "-p", "127.0.0.1::8081", image,
                "--data-listen", "0.0.0.0:8080", "--management-listen", "0.0.0.0:8081",
            ], text=True, timeout=30).strip()
            ports = json.loads(subprocess.check_output([
                "docker", "inspect", "--format", "{{json .NetworkSettings.Ports}}", container,
            ], text=True, timeout=10))
            data = f"http://127.0.0.1:{ports['8080/tcp'][0]['HostPort']}"
            management = f"http://127.0.0.1:{ports['8081/tcp'][0]['HostPort']}"
        else:
            child = subprocess.Popen([
                binary, "--data-listen", "127.0.0.1:0", "--management-listen", "127.0.0.1:0",
            ], stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
            lines = queue.Queue()
            threading.Thread(target=lambda: lines.put(child.stderr.readline()), daemon=True).start()
            line = lines.get(timeout=15).strip()
            assert line.startswith("data="), line
            addresses = dict(part.split("=", 1) for part in line.split())
            data, management = "http://" + addresses["data"], "http://" + addresses["management"]
        deadline = time.monotonic() + 15
        while True:
            try:
                request(management, "GET", "/databases/notregistered", expected=404)
                break
            except (urllib.error.URLError, ConnectionError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        yield data, management
    finally:
        if child:
            child.terminate()
            try:
                assert child.wait(timeout=20) == 0, "child did not exit gracefully"
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
                raise
            finally:
                child.stderr.close()
        if container:
            try:
                subprocess.run(["docker", "stop", "-t", "20", container], check=True,
                               stdout=subprocess.DEVNULL, timeout=30)
                code = subprocess.check_output([
                    "docker", "inspect", "--format", "{{.State.ExitCode}}", container,
                ], text=True, timeout=10).strip()
                assert code == "0", f"container exit={code}"
            finally:
                subprocess.run(["docker", "rm", "-f", container], check=True,
                               stdout=subprocess.DEVNULL, timeout=15)


def configure(workspace, app, backend, postgres, image):
    root = workspace / app
    server_root = Path("/workspace") / app if image else root
    target = ({"kind": "turso", "path": str(server_root / "data.db")}
              if backend == "turso" else {"kind": "postgres_unencrypted", "connection": postgres})
    return {
        "database": target,
        "interfaces": str(server_root / "interfaces"),
        "migrations": str(server_root / "migrations"),
        "limits": {"timeout_ms": 5000, "max_rows": 100},
    }


def deploy(management, app, config):
    status = request(management, "PUT", f"/databases/{app}", config)
    assert status["phase"] == "unloaded", status
    operation(management, app, "migrate")
    operation(management, app, "reload")
    assert request(management, "GET", f"/databases/{app}")["phase"] == "ready"


def records(data, app, method, path, body=None):
    return request(data, method, f"/db/{app}{path}", body)["records"]


def crud(data):
    todo = {"id": 41, "title": "Read the contract", "completed": False}
    entry = {"id": "expense-coffee-001", "amount_minor": -425, "note": "Coffee"}
    for app, route, item, change in [
        ("todolist", "/todos", todo, {"title": "Checked", "completed": True}),
        ("ledger", "/entries", entry, {"amount_minor": -450, "note": "Coffee + tip"}),
    ]:
        assert records(data, app, "GET", route) == []
        assert records(data, app, "POST", route, item) == [item]
        # Same stable ID: no second insertion. Changed payload cannot silently overwrite.
        assert records(data, app, "POST", route, item) == [item]
        altered = dict(item, **change)
        assert records(data, app, "POST", route, altered) == [item]
        assert records(data, app, "GET", route) == [item]
        key = urllib.parse.quote(str(item["id"]), safe="")
        assert records(data, app, "GET", route + "/" + key) == [item]
        assert records(data, app, "POST", route + "/lookup", {"ids": [item["id"], item["id"]]}) == [item]
        assert records(data, app, "POST", route + "/lookup", {"ids": []}) == []
        assert records(data, app, "PATCH", route + "/" + key, change) == [altered]
        assert records(data, app, "DELETE", route + "/" + key) == [altered]
        assert records(data, app, "DELETE", route + "/" + key) == []
        assert records(data, app, "POST", route, item) == [item]
    assert records(data, "ledger", "GET", "/balance") == [{"balance_minor": -425}]
    request(data, "POST", "/db/todolist/todos/lookup", {"ids": ["41"]}, expected=400)
    request(data, "POST", "/db/todolist/todos", dict(todo, id=42, completed=1), expected=400)
    request(data, "POST", "/db/ledger/entries", dict(entry, id="bad", amount_minor=1.5), expected=400)
    assert records(data, "ledger", "GET", "/balance") == [{"balance_minor": -425}]


def recovery(data, management, workspace):
    # Change an applied migration, preserve edit, restore the exact exported original.
    migration = next((workspace / "todolist/migrations").glob("*.sql"))
    original = migration.read_bytes()
    migration.write_bytes(original + b"\n-- accidental history edit\n")
    operation(management, "todolist", "migrate", success=False)
    assert records(data, "todolist", "GET", "/todos")[0]["id"] == 41
    history = request(management, "GET", "/databases/todolist/migrations")["migrations"]
    assert len(history) == 1
    record = history[0]
    assert record["filename"] == migration.name
    shutil.copyfile(migration, workspace / "saved-local-edit.sql")
    migration.write_text(record["source"])
    assert migration.read_bytes() == original
    # New migration is a higher version. Successful migration auto-reloads/resumes.
    (workspace / "todolist/migrations/0002_index.sql").write_text("CREATE INDEX todos_title ON todos(title);\n")
    probe = workspace / "todolist/interfaces/version"
    probe.mkdir()
    (probe / "get.sql").write_text("SELECT CAST(2 AS BIGINT) AS version;\n")
    (probe / "get.response.yaml").write_text(json.dumps({
        "type": "object", "properties": {"version": {"type": "integer"}},
        "required": ["version"], "additionalProperties": False,
    }))
    request(data, "GET", "/db/todolist/version", expected=404)
    operation(management, "todolist", "migrate")
    assert records(data, "todolist", "GET", "/version") == [{"version": 2}]
    assert records(data, "todolist", "GET", "/todos")[0]["title"] == "Read the contract"


def sdk(management, workspace):
    generator = os.environ["OPENAPI_NEXUS_BIN"]
    for app in ["todolist", "ledger"]:
        target = workspace / f"sdk-{app}"
        target.mkdir()
        specification = request(management, "GET", f"/databases/{app}/openapi")
        (target / "openapi.json").write_text(json.dumps(specification))
        subprocess.run([generator, "generate", "-i", str(target / "openapi.json"),
                        "-o", str(target / "sdk"), "--generators", "typescript-fetch"],
                       check=True, timeout=60)
        shutil.copyfile(ROOT / f"examples/sdk/{app}.ts", target / "client.ts")
        sources = sorted(str(path) for path in target.rglob("*.ts"))
        subprocess.run([
            "tsc", "--strict", "--target", "ES2022", "--module", "commonjs",
            "--moduleResolution", "node", "--ignoreDeprecations", "6.0",
            "--lib", "ES2022,DOM", "--outDir", str(target / "compiled"),
            *sources,
        ], check=True, timeout=60)
        yield target / "compiled/client.js"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default=str(ROOT / "target/debug/sqlrest"))
    parser.add_argument("--image", help="Use an already-built local image instead of the binary")
    parser.add_argument("--backend", choices=["turso", "postgres"], default="turso")
    parser.add_argument("--sdk", action="store_true", help="Generate, compile and call TypeScript SDKs")
    args = parser.parse_args()
    if args.image and args.backend != "turso":
        parser.error("image smoke uses Turso; PostgreSQL is exercised by native E2E")
    postgres = os.environ.get("SQLREST_TEST_POSTGRES")
    if args.backend == "postgres" and not postgres:
        parser.error("SQLREST_TEST_POSTGRES must identify a disposable empty database")
    with tempfile.TemporaryDirectory(prefix="sqlrest-e2e-") as temporary:
        workspace = Path(temporary)
        configs = {}
        for app in ["todolist", "ledger"]:
            shutil.copytree(ROOT / "examples" / app / args.backend, workspace / app)
            configs[app] = configure(workspace, app, args.backend, postgres, args.image)
        # PG uses one database with both applications' tables. Its aliases share
        # one migration directory and coordinated ownership, not separate histories.
        if args.backend == "postgres":
            for source in (workspace / "ledger/interfaces").iterdir():
                shutil.copytree(source, workspace / "todolist/interfaces" / source.name)
            (workspace / "todolist/migrations/0002_entries.sql").write_bytes(
                (workspace / "ledger/migrations/0001_entries.sql").read_bytes())
        with server(args.binary, args.image, workspace) as (data, management):
            deploy(management, "todolist", configs["todolist"])
            if args.backend == "postgres":
                # Same explicit combined migration set for aliases of a shared DB;
                # calls below remain sequential (one migrator).
                configs["ledger"]["interfaces"] = configs["todolist"]["interfaces"]
                configs["ledger"]["migrations"] = configs["todolist"]["migrations"]
            deploy(management, "ledger", configs["ledger"])
            crud(data)
            if args.backend == "turso":
                recovery(data, management, workspace)
            if args.sdk:
                for client in sdk(management, workspace):
                    subprocess.run(["node", str(client), data], check=True, timeout=30)
        with server(args.binary, args.image, workspace) as (data, management):
            for app in ["todolist", "ledger"]:
                request(management, "GET", f"/databases/{app}", expected=404)
                deploy(management, app, configs[app])
            assert records(data, "todolist", "GET", "/todos") == [
                {"id": 41, "title": "Read the contract", "completed": False}]
            assert records(data, "ledger", "GET", "/balance") == [{"balance_minor": -425}]
        print(f"PASS {args.backend}: two examples, CRUD, retries, arrays, restart"
              + (", generated SDK calls" if args.sdk else "")
              + (", container" if args.image else ""))


if __name__ == "__main__":
    main()
