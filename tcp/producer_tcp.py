import argparse
import socket
import struct
import json
import random
from datetime import datetime
from typing import Dict, List, Tuple
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
    p.add_argument("--file", "-f", required=True, help="CSV file path")
    p.add_argument("--host", default="127.0.0.1", help="Consumer host")
    p.add_argument("--port", type=int, default=5000, help="Consumer port")
    p.add_argument("--seed", type=int, default=None, help="Optional RNG seed")
    p.add_argument("--batch", type=int, default=1, help="Send N docs per ack (1 = ack per doc)")
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
        if random.random() < 0.6:
            return faker.word()
        return faker.sentence(nb_words=random.randint(2, 5))

def build_generated_fields():
    return {f"gen_{i}": random_value_for_col(i) for i in range(26, 151)}

def pick_country_city(mapping):
    country = random.choice(list(mapping.keys()))
    city = random.choice(mapping[country]) if mapping[country] else ""
    return country, city

def send_frame(sock: socket.socket, obj: dict):
    b = json.dumps(obj, default=str).encode("utf-8")
    header = struct.pack(">I", len(b))
    sock.sendall(header + b)

def recv_frame(sock: socket.socket):
    # read 4 bytes
    header = sock.recv(4)
    if not header:
        raise ConnectionError("server closed")
    if len(header) < 4:
        # get remaining
        header += sock.recv(4 - len(header))
    msglen = struct.unpack(">I", header)[0]
    data = b""
    while len(data) < msglen:
        chunk = sock.recv(msglen - len(data))
        if not chunk:
            raise ConnectionError("server closed mid-frame")
        data += chunk
    return json.loads(data.decode("utf-8"))

def main():
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    countries_map = DEFAULT_COUNTRIES

    df = pd.read_csv(args.file, dtype=str, keep_default_na=False)
    ncols = df.shape[1]
    if ncols < 25:
        raise SystemExit(f"CSV has {ncols} columns; at least 25 required")
    csv_header = list(df.columns)

    # connect to server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((args.host, args.port))
    print(f"[tcp] connected to {args.host}:{args.port}")

    sent = 0
    batch_buf = []
    try:
        for  row in df.iterrows():
            doc = {}
            for i in range(25):
                key = csv_header[i]
                doc[key] = None if row[key] == "" else row[key]

            doc.update(build_generated_fields())
            country, city = pick_country_city(countries_map)
            doc["country"] = country
            doc["city"] = city
            doc["_ingestedAt"] = datetime.utcnow().isoformat()

            batch_buf.append(doc)

            if len(batch_buf) >= args.batch:
                # send batch as single array or send one by one: here we send one-by-one
                for item in batch_buf:
                    send_frame(sock, item)
                    # wait for ack per message
                    ack = recv_frame(sock)
                    if ack.get("status") != "ok":
                        print(f"[warn] server ack error: {ack}")
                sent += len(batch_buf)
                print(f"[tcp] sent {sent} documents")
                batch_buf = []

        # send remainder
        for item in batch_buf:
            send_frame(sock, item)
            ack = recv_frame(sock)
            if ack.get("status") != "ok":
                print(f"[warn] server ack error: {ack}")
            sent += 1
        print(f"[tcp] finished. total sent: {sent}")
    finally:
        sock.close()

if __name__ == "__main__":
    main()
