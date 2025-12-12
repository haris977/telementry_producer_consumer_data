#!/usr/bin/env python3
import argparse
import socket
import struct
import json
import threading
import queue
from datetime import datetime
from typing import Dict, Any
from pymongo import MongoClient, errors
from flask import Flask, Response, stream_with_context

# ----------------- your original helpers & logic (kept) -----------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0", help="Host to bind the consumer server")
    p.add_argument("--port", type=int, default=5000, help="Port to bind the consumer server")
    p.add_argument("--mongo", default="mongodb://localhost:27017", help="MongoDB URI (use replicaSet=rs0 for change streams)")
    p.add_argument("--db", default="telemetryDB", help="MongoDB database name")
    p.add_argument("--coll", default="telemetryRecords", help="MongoDB collection name")
    p.add_argument("--http-host", default="0.0.0.0", help="Host for embedded HTTP server (SSE)")
    p.add_argument("--http-port", type=int, default=8000, help="Port for embedded HTTP server (SSE)")
    return p.parse_args()

def process_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    nums = [v for k, v in doc.items() if k.startswith("gen_") and isinstance(v, (int, float))]
    s = sum(nums) if nums else 0
    avg = (s / len(nums)) if nums else None
    return {"numeric_count": len(nums), "numeric_sum": s, "numeric_avg": avg}

def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed while reading")
        buf += chunk
    return buf

def send_response(conn: socket.socket, obj: dict):
    b = json.dumps(obj, default=str).encode("utf-8")
    header = struct.pack(">I", len(b))
    conn.sendall(header + b)

# ----------------- Broadcasting (SSE) primitives -----------------
# Each connected HTTP client gets a Queue that mongo_watcher will put messages into.
clients = set()
clients_lock = threading.Lock()

def broadcast_to_clients(data_str: str):
    """Put a message into each client's queue (non-blocking)."""
    with clients_lock:
        for q in list(clients):
            try:
                q.put_nowait(data_str)
            except Exception:
                # if queue full or error, ignore that client for now
                pass

# ----------------- MongoDB watcher thread -----------------
def mongo_watcher(coll):
    """
    Uses MongoDB Change Streams to watch for changes and broadcasts JSON events.
    NOTE: Requires MongoDB replica set (even single-node). See run instructions.
    """
    try:
        print("[watcher] starting change stream...")
        # full_document='updateLookup' gives the full updated doc for update events
        with coll.watch(full_document='updateLookup') as stream:
            for change in stream:
                # change contains operationType, fullDocument (if requested), ns, documentKey, etc.
                data = json.dumps(change, default=str)
                print("[watcher] change:", data)
                broadcast_to_clients(data)
    except Exception as e:
        print("[watcher] error (change-stream):", e)
        # In production you might want to retry with backoff here.

# ----------------- Embedded Flask SSE server -----------------
app = Flask(__name__)

@app.route("/events")
def sse_events():
    """SSE endpoint: frontend connects and receives Mongo change events."""
    q = queue.Queue(maxsize=200)
    with clients_lock:
        clients.add(q)

    def gen():
        try:
            # SSE initial comment to confirm connection
            yield ": connected\n\n"
            while True:
                # block until an event arrives
                data = q.get()
                # SSE requires lines starting with 'data: ' and double newline termination
                yield f"data: {data}\n\n"
        except GeneratorExit:
            # client disconnected
            return
        finally:
            with clients_lock:
                clients.discard(q)

    return Response(stream_with_context(gen()), mimetype="text/event-stream")

@app.route("/health")
def health():
    return {"status": "ok"}

def run_flask(host: str, port: int):
    # Simple builtin Flask server for dev/demo. For production run behind nginx/uwsgi/gunicorn.
    print(f"[http] starting Flask SSE server on {host}:{port}")
    app.run(host=host, port=port, threaded=True, use_reloader=False)

# ----------------- Your TCP handler (kept) -----------------
def handle_client(conn: socket.socket, addr, db_coll):
    print(f"[conn] Accepted from {addr}")
    try:
        while True:
            # read 4-byte big-endian length
            header = conn.recv(4)
            if not header:
                print(f"[conn] {addr} closed connection")
                break
            if len(header) < 4:
                header += recv_exact(conn, 4 - len(header))
            msglen = struct.unpack(">I", header)[0]
            # read payload
            payload = recv_exact(conn, msglen)
            try:
                doc = json.loads(payload.decode("utf-8"))
            except Exception as e:
                err = {"status": "error", "reason": f"invalid json: {e}"}
                send_response(conn, err)
                continue

            # add server-side metadata
            doc["_receivedAt"] = datetime.utcnow().isoformat()

            # insert into MongoDB (safe single insert)
            try:
                insert_result = db_coll.insert_one(doc)
                # optionally process and update the document with processing result
                proc_res = process_document(doc)
                db_coll.update_one(
                    {"_id": insert_result.inserted_id},
                    {"$set": {"_processed": True, "_processedAt": datetime.utcnow(), "_processingResult": proc_res}}
                )
                ack = {"status": "ok", "id": str(insert_result.inserted_id)}
            except Exception as e:
                ack = {"status": "error", "reason": str(e)}

            send_response(conn, ack)
    except ConnectionError as ce:
        print(f"[conn] connection error from {addr}: {ce}")
    except Exception as ex:
        print(f"[conn] unexpected error from {addr}: {ex}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ----------------- main: set up mongo, start watcher & http, then accept TCP clients -----------------
def main():
    args = parse_args()

    # Connect to MongoDB
    try:
        # NOTE: change streams require replica set. Use e.g. mongodb://localhost:27017/?replicaSet=rs0
        client = MongoClient(args.mongo)
        db = client[args.db]
        coll = db[args.coll]
        print(f"[mongo] connected to {args.mongo} DB:{args.db} Coll:{args.coll}")
    except errors.PyMongoError as e:
        print("[mongo] connection error:", e)
        return

    # Start Mongo watcher thread
    t_watch = threading.Thread(target=mongo_watcher, args=(coll,), daemon=True)
    t_watch.start()

    # Start embedded Flask SSE server in a background thread
    t_http = threading.Thread(target=run_flask, args=(args.http_host, args.http_port), daemon=True)
    t_http.start()

    # Start TCP server (your original accept loop)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.host, args.port))
    srv.listen(8)
    print(f"[tcp] consumer listening on {args.host}:{args.port} ...")

    try:
        while True:
            conn, addr = srv.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr, coll), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("[tcp] shutting down (KeyboardInterrupt)")
    finally:
        try:
            srv.close()
        except Exception:
            pass
        try:
            client.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
