import os
import random
import time
import argparse
from typing import Dict, List, Tuple
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from faker import Faker

Default_csv = os.getenv("CSV_FILE")
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

faker = Faker()

def parse_arg():
    p = argparse.ArgumentParser()
    p.add_argument('--file', '-f', default=os.getenv('CSV_FILE', Default_csv),
                   help=r'CSV file path')
    p.add_argument('--mongo', '-m', default=os.getenv('MONGO_URI', 'mongodb://localhost:27017'),
                   help='MongoDB URI')
    p.add_argument('--db', default=os.getenv('MONGO_DB', 'testdb'),
                   help='MongoDB database name')
    p.add_argument('--coll', default=os.getenv('MONGO_COLL', 'sensor_data'),
                   help='MongoDB collection name')
    p.add_argument('--batch', type=int, default=500,
                   help='Insert batch size (default 500)')
    p.add_argument('--seed', type=int, default=None,
                   help='Optional random seed for reproducibility (default none)')
    p.add_argument('--countries-file', type=str, default=None,
                   help='Optional JSON file with {"Country": ["City1","City2", ...], ...}')
    return p.parse_args()

def load_country(country_file: str = None) -> Dict[str, List[str]]:
    if country_file:
        import json
        with open(country_file, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
            if isinstance(data, dict):
                return data
            raise ValueError("file must contain a JSON object mapping country -> list of cities")
    return DEFAULT_COUNTRIES

def pick_country_city(mapping: Dict[str, List[str]]) -> Tuple[str, str]:
    country = random.choice(list(mapping.keys()))
    cities = mapping[country]
    city = random.choice(cities)
    return country, city

def random_value(col_index: int):
    t = random.random()
    if t < 0.25:
        return random.randint(0, 1000)
    elif t < 0.5:
        return round(random.uniform(-10000.0, 10000.0), 4)
    elif t < 0.65:
        return random.choice([True, False])
    else:
        if random.random() < .6:
            return faker.word()
        else:
            return faker.sentence(nb_words=random.randint(2, 5))

def build_generated_fields() -> dict:
    out = {}
    for i in range(26, 150):
        out[f"gen_{i}"] = random_value(i)
    return out

def main():
    args = parse_arg()
    if args.seed is not None:
        random.seed(args.seed)
        faker.seed_instance(args.seed)

    countries_map = load_country(args.countries_file)
    for k in list(countries_map.keys())[:10]:
        print(f"{k}: {countries_map[k][:5]}")

    csv_path = args.file
    if not csv_path or not os.path.exists(csv_path):
        raise SystemExit(f"CSV file not found: {csv_path}")
    print(f"reading the file {csv_path} ...")
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    ncols = df.shape[1]
    if ncols < 25:
        raise SystemExit(f"your data has only {ncols} columns. number of columns should be >= 25")
    csv_header = list(df.columns)
    print(f"csv detected {ncols} columns; first 25 columns (required): {csv_header[:25]}")

    client = MongoClient(args.mongo)
    db = client[args.db]
    coll = db[args.coll]
    print(f"connected to MongoDB {args.mongo}, DB: {args.db}, collection: {args.coll}")
    
    total = 0
    batch_doc = []
    batch_size = max(1, args.batch)
    for idx, row in df.iterrows():
        doc = {}
        for i in range(25):
            key = csv_header[i]
            val = row[key]
            doc[key] = None if val == "" else val
        doc.update(build_generated_fields())
        country, city = pick_country_city(countries_map)
        doc["country"] = country
        doc["city"] = city
        doc["_ingestedAt"] = datetime.utcnow()
        doc["_processed"] = False
        batch_doc.append(doc)
        total += 1

        if len(batch_doc) >= batch_size:
            coll.insert_many(batch_doc, ordered=False)
            print(f"Inserted batch of {len(batch_doc)} documents. Total inserted so far: {total}")
            batch_doc = []
    if batch_doc:
        coll.insert_many(batch_doc, ordered=False)
        print(f"Inserted final batch of {len(batch_doc)}. Total inserted: {total}")

    print("producer finished")
    client.close()

if __name__ == "__main__":
    main()
