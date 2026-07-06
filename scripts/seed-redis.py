"""Seed a Redis instance with sample data for testing KeyView."""

import random
import redis

r = redis.Redis(host="redis", port=6379, decode_responses=True)

NAMESPACES = ["user", "session", "cache", "order", "product", "analytics", "config"]
random.seed(42)

print("Seeding Redis with sample data...")

pipe = r.pipeline(transaction=False)

for i in range(5000):
    ns = random.choice(NAMESPACES)
    key = f"{ns}:{random.randint(1000, 9999)}:{random.choice(['data', 'meta', 'idx'])}"

    key_type = random.choices(
        ["string", "hash", "list", "set", "zset"],
        weights=[40, 25, 15, 10, 10],
    )[0]

    if key_type == "string":
        pipe.set(key, f"value_{i}")
    elif key_type == "hash":
        pipe.hset(key, mapping={"field1": "val1", "field2": "val2"})
    elif key_type == "list":
        pipe.rpush(key, "item1", "item2", "item3")
    elif key_type == "set":
        pipe.sadd(key, "member1", "member2", "member3")
    elif key_type == "zset":
        pipe.zadd(key, {"member1": 1.0, "member2": 2.0})

    ttl_choice = random.choices(
        ["none", "short", "medium", "long"],
        weights=[30, 20, 30, 20],
    )[0]

    if ttl_choice == "short":
        pipe.expire(key, random.randint(10, 60))
    elif ttl_choice == "medium":
        pipe.expire(key, random.randint(300, 3600))
    elif ttl_choice == "long":
        pipe.expire(key, random.randint(7200, 172800))

    if i % 500 == 0:
        pipe.execute()
        pipe = r.pipeline(transaction=False)

pipe.execute()
print(f"Done. Total keys: {r.dbsize()}")
