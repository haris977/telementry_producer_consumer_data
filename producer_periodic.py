import argparse
import socket
import struct
import json
import random
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import pandas as pd
from faker import Faker

faker = Faker()

DEFAULT_COUNTRIES = {
    "India": ["Mumbai", "Delhi", "Bengaluru", "Kolkata", "Chennai"],
    "USA": ["New York", "Los Angeles", "Chicago", "Houston", "San Francisco"],
    "United Kingdom": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
    "Germany": ["Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt"],
    "France": ["Paris", "Marseille", "Lyon", "Toulouse", "Nice"],
    "Canada": ["Toronto", "Montreal", "Vancouver", "Calgary", "Ottawa"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    "Brazil": ["Sao Paulo", "Rio de Janeiro", "Brasilia", "Salvador", "Fortaleza"],
    "South Africa": ["Johannesburg", "Cape Town", "Durban", "Pretoria", "Port Elizabeth"],
    "Japan": ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"]
}

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--file", "-f", help="CSV file path (required for mode=csv)")
    p.add_argument("--host", default="127.0.0.1", help="Consumer host")
    p.add_argument("--port", type=int, default=5000, help="Consumer port")
    p.add_argument("--interval", type=float, default=5.0, help="Seconds between sends (float accepted). Use 0 to send all immediately once.")
    p.add_argument("--mode", choices=("csv", "generate"), default="csv", help="Mode: 'csv' to use rows from CSV, 'generate' to create synthetic rows")
    p.add_argument("--batch", type=int, default=1, help="How many documents to send each interval (1 = default)")
    p.add_argument("--loop", action="store_true", help="If mode=csv: loop rows when end reached")
    p.add_argument("--seed", type=int, default=None, help="Optional RNG seed")
    p.add_argument("--ack", action="store_true", help="Wait for ACK from server for each message (recommended)")
    p.add_argument("--reconnect-backoff", type=float, default=1.0, help="Initial reconnect backoff seconds (exponential)")
    return p.parse_args()

def random_value_for_col(col_index: int):
    t = random.random()
    if t < 0.25:
        return random.randint(0, 10000)
    elif t < 0.55:
        return round(random.uniform(-1000.0, 10000.0), 4)
    elif t < 0.75:
        return random.choice([True, False])
    else:
        return faker.word() if random.random() < 0.6 else faker.sentence(nb_words=random.randint(2, 5))

def build_generated_fields():
    return {f"gen_{i}": random_value_for_col(i) for i in range(26, 151)}

def pick_country_city(mapping=DEFAULT_COUNTRIES):
    country = random.choice(list(mapping.keys()))
    city = random.choice(mapping[country]) if mapping[country] else ""
    return country, city

def send_frame(sock: socket.socket, obj: dict):
    b = json.dumps(obj, default=str).encode("utf-8")
    header = struct.pack(">I", len(b))
    sock.sendall(header + b)

def recv_frame(sock: socket.socket) -> dict:
    header = sock.recv(4)
    if not header:
        raise ConnectionError("connection closed by server")
    if len(header) < 4:
        # read remaining bytes
        needed = 4 - len(header)
        header += sock.recv(needed)
    msglen = struct.unpack(">I", header)[0]
    data = b""
    while len(data) < msglen:
        chunk = sock.recv(msglen - len(data))
        if not chunk:
            raise ConnectionError("server closed mid-frame")
        data += chunk
    return json.loads(data.decode("utf-8"))

def load_csv_rows(file_path: str):
    df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
    if df.shape[1] < 25:
        raise SystemExit(f"CSV has {df.shape[1]} columns; at least 25 required")
    header = list(df.columns)
    # store only first-25 values per row to build doc at runtime
    rows = []
    for _, r in df.iterrows():
        rows.append([None if r[header[i]] == "" else r[header[i]] for i in range(25)])
    return header[:25], rows

def connect_with_backoff(host: str, port: int, initial_backoff: float = 1.0, max_backoff: float = 60.0):
    backoff = initial_backoff
    connectedd = False
    while not connectedd:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((host, port))
            connectedd = True
            sock.settimeout(None)  # switch to blocking mode
            return sock
        except Exception as e:
            print(f"[connect] failed: {e}. Retrying in {backoff:.1f}s...")
            time.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)

def main():
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    if args.mode == "csv" and not args.file:
        raise SystemExit("mode=csv requires --file path")

    if args.mode == "csv":
        header25, csv_rows = load_csv_rows(args.file)
        total_rows = len(csv_rows)
        print(f"[csv] loaded {total_rows} rows, using header columns: {header25}")
    else:
        header25 = [f"col{i+1}" for i in range(25)]
        csv_rows = []
        total_rows = 0

    print(f"[tcp] connecting to {args.host}:{args.port} ...")
    sock = connect_with_backoff(args.host, args.port, initial_backoff=args.reconnect_backoff)

    row_index = 0

    try:
        sent_total = 0
        while True:
            start_time = time.time()
            batch_to_send = []
            for _ in range(args.batch):
                if args.mode == "csv":
                    if total_rows == 0:
                        raise SystemExit("CSV has no rows")
                    row_values = csv_rows[row_index]
                    doc = {header25[i]: row_values[i] for i in range(25)}
                    row_index += 1
                    if row_index >= total_rows:
                        if args.loop:
                            row_index = 0
                        else:
                            pass
                else:
                    doc = {f"col{i+1}": random_value_for_col(i+1) for i in range(25)}

                doc.update(build_generated_fields())
                country, city = pick_country_city()
                doc["country"] = country
                doc["city"] = city
                doc["_ingestedAt"] = datetime.utcnow().isoformat()
                batch_to_send.append(doc)
            for doc in batch_to_send:
                try:
                    send_frame(sock, doc)
                    if args.ack:
                        ack = recv_frame(sock)
                        if ack.get("status") != "ok":
                            print(f"[warn] non-ok ack: {ack}")
                    sent_total += 1
                except (ConnectionError, OSError) as e:
                    print(f"[tcp] send failed: {e}. Attempting reconnect...")
                    sock.close()
                    sock = connect_with_backoff(args.host, args.port, initial_backoff=args.reconnect_backoff)
                    try:
                        send_frame(sock, doc)
                        if args.ack:
                            ack = recv_frame(sock)
                        sent_total += 1
                    except Exception as e2:
                        print(f"[tcp] resend failed: {e2}. Skipping this doc.")
                        continue

            print(f"[tcp] sent total {sent_total} documents (last batch size {len(batch_to_send)})")

            if args.interval <= 0:
                print("[run] interval <= 0, exiting after single run.")
                break

            elapsed = time.time() - start_time
            to_sleep = args.interval - elapsed
            if to_sleep > 0:
                time.sleep(to_sleep)
            if args.mode == "csv" and not args.loop and row_index >= total_rows:
                print("[run] CSV exhausted and --loop not set. Exiting.")
                break

    except KeyboardInterrupt:
        print("[run] interrupted by user (Ctrl+C). Exiting.")

    finally:
        try:
            sock.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
