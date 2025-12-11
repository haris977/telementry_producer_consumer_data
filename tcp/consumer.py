import os
import time
from datetime import datetime
import argparse
from typing import Any, Dict, List
from pymongo import MongoClient, UpdateOne

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--mongo', '-m', default=os.getenv('MONGO_URI', 'mongodb://localhost:27017'),
                   help='MongoDB URI')
    p.add_argument('--db', default=os.getenv('MONGO_DB', 'testdb'),
                   help='Database name')
    p.add_argument('--coll', default=os.getenv('MONGO_COLL', 'sensor_data'),
                   help='Collection name')
    p.add_argument('--batch', type=int, default=200,
                   help='Number of docs to process per loop (default 200)')
    p.add_argument('--poll', type=float, default=5.0,
                   help='Poll interval in seconds when no docs found (default 5s)')
    return p.parse_args()

def to_number(v):
    """Try to convert to int/float. Return None if not numeric."""
    if isinstance(v, (int, float)):
        return v
    try:
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return None
        if isinstance(v, str) and "." in v:
            return float(v)
        return int(v)
    except Exception:
        try:
            return float(v)
        except Exception:
            return None

def process_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    numeric_values = []
    for k, v in doc.items():
        if k.startswith("gen_"):
            num = to_number(v)
            if num is not None:
                numeric_values.append(num)
    if numeric_values:
        s = sum(numeric_values)
        avg = s / len(numeric_values)
    else:
        s = 0
        avg = None
    return {"numeric_count": len(numeric_values), "numeric_sum": s, "numeric_avg": avg}

def main():
    args = parse_args()
    client = MongoClient(args.mongo)
    db = client[args.db]
    coll = db[args.coll]
    print(f"consumer connected to {args.mongo} with database: {args.db} and collection: {args.coll}")
    try:
        while True:
            docs = list(coll.find({'_processed': False}).limit(args.batch))
            if not docs:
                time.sleep(args.poll)
                continue
            print(f"found {len(docs)} document(s) to process")
            updates = []
            for doc in docs:
                try:
                    result = process_document(doc)
                    updates.append(UpdateOne(
                        {"_id": doc["_id"]},
                        {"$set": {
                            "_processed": True,
                            "_updatedAt": datetime.utcnow(),
                            "_processingResult": result
                        }}
                    ))
                except Exception as e:
                    updates.append(UpdateOne(
                        {"_id": doc["_id"]},
                        {"$set": {
                            "_processingError": str(e),
                            "_processingErrorAt": datetime.utcnow()
                        }}
                    ))
            if updates:
                res = coll.bulk_write(updates, ordered=False)
                print(f"bulk write completed: matched {res.matched_count}, modified {res.modified_count}")
    except KeyboardInterrupt:
        print("consumer stopped by user")
    finally:
        client.close()

if __name__ == "__main__":
    main()
