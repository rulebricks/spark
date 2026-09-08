import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import httpx
import pytest
from rulebricks import Rulebricks

from rulebricks_spark._config import RunConfig

ROOT_ID = "068d229a-f8a8-4a74-b789-62ce785e13ba"
HEADERS = {
    "X-Rulebricks-Flow-Execution-Id": ROOT_ID,
    "X-Rulebricks-Trace-Id": "trace-123",
}


def echo(request):
    return httpx.Response(
        200,
        json=[
            json.loads(item["response"]) if "response" in item else item
            for item in json.loads(request.content)
        ],
    )


def respond(state, request):
    state.requests.append(request)
    response = state.handler(request)
    for key, value in HEADERS.items():
        response.headers.setdefault(key, value)
    return response


@pytest.fixture
def config():
    return RunConfig(
        asset_type="flow",
        slug="decision",
        version="3",
        api_key="test-key",
        base_url="https://rules.example/api/v1",
        max_retries=0,
    )


@pytest.fixture
def sdk(config):
    state = SimpleNamespace(handler=echo, requests=[])
    with httpx.Client(
        transport=httpx.MockTransport(lambda request: respond(state, request))
    ) as client:
        state.http = client
        state.client = Rulebricks(
            api_key=config.api_key,
            base_url=config.base_url,
            httpx_client=client,
            max_retries=4,
        )
        yield state


@pytest.fixture
def api():
    state = SimpleNamespace(handler=echo, requests=[])

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.reply()

        def do_GET(self):
            self.reply()

        def reply(self):
            request = httpx.Request(
                self.command,
                f"http://{self.headers['Host']}{self.path}",
                headers=dict(self.headers),
                content=self.rfile.read(int(self.headers.get("Content-Length", 0))),
            )
            response = respond(state, request)
            self.send_response(response.status_code)
            for key, value in response.headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(response.content)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.options = dict(
        api_key="test-key", base_url=f"http://127.0.0.1:{server.server_port}/api/v1"
    )
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[2]")
        .appName("rulebricks-spark-tests")
        .config("spark.speculation", "false")
        .config("spark.task.maxFailures", "1")
        .config("spark.stage.maxConsecutiveAttempts", "1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setCheckpointDir(str(tmp_path_factory.mktemp("checkpoints")))
    yield session
    session.stop()
