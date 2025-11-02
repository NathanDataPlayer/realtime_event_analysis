import os
import json
import time
import random
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer
from faker import Faker


def ms_since_epoch(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def produce_continuous(bootstrap_servers: str):
    topic_page_views = os.getenv("TOPIC_PAGE_VIEWS", "page_views")
    topic_orders = os.getenv("TOPIC_ORDERS", "orders")

    fake = Faker()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks=1,
    )

    pages = ["/", "/home", "/product/alpha", "/product/beta", "/search", "/cart", "/checkout"]
    devices = ["mobile", "desktop", "tablet"]
    oss = ["iOS", "Android", "MacOS", "Windows", "Linux"]
    countries = ["CN", "US", "JP", "DE", "FR", "IN"]
    currencies = ["CNY", "USD", "EUR"]
    channels = ["organic", "paid", "email", "social"]

    user_pool = [fake.uuid4() for _ in range(1000)]

    print(f"Producing (continuous) to {bootstrap_servers} | topics: {topic_page_views}, {topic_orders}")

    while True:
        now = datetime.now(timezone.utc)
        ts_ms = ms_since_epoch(now)

        pv = {
            "event_id": fake.uuid4(),
            "user_id": random.choice(user_pool),
            "page": random.choice(pages),
            "referrer": fake.uri(),
            "device": random.choice(devices),
            "os": random.choice(oss),
            "country": random.choice(countries),
            "ts_ms": ts_ms,
        }

        order_status = random.choice(["created", "paid", "cancelled", "completed"])
        order = {
            "order_id": fake.uuid4(),
            "user_id": random.choice(user_pool),
            "amount": round(random.uniform(10, 5000), 2),
            "currency": random.choice(currencies),
            "status": order_status,
            "ts_ms": ts_ms,
            "region": random.choice(["East", "North", "South", "West"]),
            "channel": random.choice(channels),
        }

        producer.send(topic_page_views, pv)
        producer.send(topic_orders, order)

        for _ in range(random.randint(1, 5)):
            ts_ms2 = ms_since_epoch(datetime.now(timezone.utc))
            pv2 = pv.copy()
            pv2["event_id"] = fake.uuid4()
            pv2["ts_ms"] = ts_ms2
            pv2["page"] = random.choice(pages)
            producer.send(topic_page_views, pv2)

        producer.flush()
        time.sleep(0.5)


def produce_user_login_range(bootstrap_servers: str, start_date: str, end_date: str, per_day: int):
    topic_user_login = os.getenv("TOPIC_USER_LOGIN", "user_login")
    fake = Faker()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks=1,
    )

    devices = ["mobile", "desktop", "tablet"]
    oss = ["iOS", "Android", "MacOS", "Windows", "Linux"]
    regions = ["East", "North", "South", "West"]
    methods = ["password", "otp", "oauth", "sso"]
    user_pool = [fake.uuid4() for _ in range(5000)]

    # Parse dates as UTC midnight boundaries
    start = datetime.strptime(start_date, "%Y.%m.%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(end_date, "%Y.%m.%d").replace(tzinfo=timezone.utc)

    if end < start:
        raise ValueError("end_date must be >= start_date")

    cur = start
    total_sent = 0
    print(f"Producing user_login events to {bootstrap_servers} | topic: {topic_user_login}\nRange: {start_date} ~ {end_date}, {per_day} per day")
    while cur <= end:
        day_start = cur
        day_end = cur + timedelta(days=1) - timedelta(milliseconds=1)
        for _ in range(int(per_day)):
            # Random timestamp within the day
            delta_ms = random.randint(0, int((day_end - day_start).total_seconds() * 1000))
            ts_dt = day_start + timedelta(milliseconds=delta_ms)
            ts_ms = ms_since_epoch(ts_dt)
            ev = {
                "login_id": fake.uuid4(),
                "user_id": random.choice(user_pool),
                "ip": fake.ipv4(),
                "user_agent": fake.user_agent(),
                "device": random.choice(devices),
                "os": random.choice(oss),
                "is_success": random.choice([True, False]),
                "ts_ms": ts_ms,
                "region": random.choice(regions),
                "method": random.choice(methods),
            }
            producer.send(topic_user_login, ev)
            total_sent += 1
            # throttle lightly to avoid overwhelming broker
            if total_sent % 1000 == 0:
                producer.flush()
                time.sleep(0.1)
        print(f"{cur.strftime('%Y-%m-%d')}: sent {per_day}")
        cur = cur + timedelta(days=1)

    producer.flush()
    print(f"Done. Total sent: {total_sent}")


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    mode = os.getenv("MODE", "continuous").lower()
    if mode == "user_login_range":
        start_date = os.getenv("START_DATE")
        end_date = os.getenv("END_DATE")
        per_day = int(os.getenv("PER_DAY_COUNT", "1000"))
        if not start_date or not end_date:
            raise ValueError("START_DATE and END_DATE are required for user_login_range mode (format: YYYY.MM.DD)")
        produce_user_login_range(bootstrap_servers, start_date, end_date, per_day)
    else:
        produce_continuous(bootstrap_servers)


if __name__ == "__main__":
    main()