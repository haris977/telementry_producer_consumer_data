import argparse
import socket
import struct
import json
import threading
from datetime import datetime
from typing import Dict, Any
from pymongo import MongoClient, errors

# ---- helper: parse args ----
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0", help="Host to bind the consumer server")
    p.add_argument("--port", type=int, default=5000, help="Port to bind the consumer server")
    p.add_argument("--mongo", default="mongodb://localhost:27017", help="MongoDB URI")
    p.add_argument("--db", default="telemetryDB", help="MongoDB database name")
    p.add_argument("--coll", default="telemetryRecords", help="MongoDB collection name")
    return p.parse_args()

# ---- simple processing function (placeholder) ----
def process_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    # Example: compute number and average of numeric generated fields
    nums = [v for k, v in doc.items() if k.startswith("gen_") and isinstance(v, (int, float))]
    s = sum(nums) if nums else 0
    avg = (s / len(nums)) if nums else None
    return {"numeric_count": len(nums), "numeric_sum": s, "numeric_avg": avg}

# ---- socket framing helpers ----
def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes from socket or raise."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed while reading")
        buf += chunk
    return buf

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
                # try to read exact 4 bytes
                header += recv_exact(conn, 4 - len(header))
            msglen = struct.unpack(">I", header)[0]
            # read payload
            payload = recv_exact(conn, msglen)
            try:
                doc = json.loads(payload.decode("utf-8"))
            except Exception as e:
                # send back an error response
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
                db_coll.update_one({"_id": insert_result.inserted_id}, {"$set": {"_processed": True, "_processedAt": datetime.utcnow(), "_processingResult": proc_res}})
                ack = {"status": "ok", "id": str(insert_result.inserted_id)}
            except Exception as e:
                ack = {"status": "error", "reason": str(e)}

            send_response(conn, ack)
    except ConnectionError as ce:
        print(f"[conn] connection error from {addr}: {ce}")
    except Exception as ex:
        print(f"[conn] unexpected error from {addr}: {ex}")
        conn.close()

def send_response(conn: socket.socket, obj: dict):
    b = json.dumps(obj, default=str).encode("utf-8")
    header = struct.pack(">I", len(b))
    conn.sendall(header + b)

def main():
    args = parse_args()
    try:
        client = MongoClient(args.mongo)
        db = client[args.db]
        coll = db[args.coll]
        print(f"[mongo] connected to {args.mongo} DB:{args.db} Coll:{args.coll}")
    except errors.PyMongoError as e:
        print("[mongo] connection error:", e)
        return

    # start TCP server
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.host, args.port))
    srv.listen(8)
    # srv.settimeout(10)
    print(f"[tcp] consumer listening on {args.host}:{args.port} ...")

    try:
        while True:
            conn, addr = srv.accept()
            # handle each client in a separate thread
            t = threading.Thread(target=handle_client, args=(conn, addr, coll), daemon=True)
            t.start()
            
    except KeyboardInterrupt:
        print("[tcp] shutting down (KeyboardInterrupt)")
        srv.close()
        client.close()
    except :
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((args.host, args.port))
        srv.listen(8)
        srv.settimeout(10)      

if __name__ == "__main__":
    main()
