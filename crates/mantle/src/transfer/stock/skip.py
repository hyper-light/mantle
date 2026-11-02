"""Test only: real AWS CLI receives 412 during multipart upload, then HEAD sees no object."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import threading
from urllib.parse import urlsplit

counts = {"refused": 0, "aborted": 0, "completed": 0, "heads": 0}
lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def body(self):
        # CLI parts here are 8 MiB; read pieces, including HTTP chunk framing, with no payload kept.
        total = 0
        chunked = self.headers.get("Transfer-Encoding", "").lower() == "chunked"
        left = None if chunked else int(self.headers.get("Content-Length", "0"))
        while left is None or left:
            if chunked:
                left = int(self.rfile.readline(8192).split(b";")[0], 16)
                if left == 0:
                    while self.rfile.readline(8192) not in (b"\r\n", b""):
                        pass
                    return
            total += left
            if total > 16 * 1024**2:
                raise ValueError("test body exceeds its bound")
            while left:
                data = self.rfile.read(min(left, 65536))
                if not data:
                    raise ValueError("short test body")
                left -= len(data)
            if chunked:
                if self.rfile.read(2) != b"\r\n":
                    raise ValueError("bad chunk terminator")
                left = None

    def reply(self, code, body=b""):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", "application/xml")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_POST(self):
        self.body()
        if urlsplit(self.path).query == "uploads":
            self.reply(200, b'<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>transfer</Bucket><Key>l0</Key><UploadId>test</UploadId></InitiateMultipartUploadResult>')
        else:
            with lock:
                counts["completed"] += 1
            self.reply(400, b"<Error><Code>InvalidRequest</Code></Error>")

    def do_PUT(self):
        self.body()
        with lock:
            counts["refused"] += 1
        self.reply(412, b"<Error><Code>PreconditionFailed</Code><Message>Injected multipart refusal</Message></Error>")

    def do_DELETE(self):
        self.body()
        with lock:
            counts["aborted"] += 1
        self.reply(204)

    def do_HEAD(self):
        with lock:
            counts["heads"] += 1
        self.reply(404)


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
server.daemon_threads = True

def stop():
    sys.stdin.buffer.read(1)
    server.shutdown()

threading.Thread(target=stop, daemon=True).start()
print(server.server_port, flush=True)
server.serve_forever()
server.server_close()
Path(sys.argv[1]).write_text(json.dumps(counts))
