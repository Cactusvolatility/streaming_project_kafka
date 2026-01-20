import asyncio
import json
from aiokafka import AIOKafkaConsumer
import asyncpg

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "raw_events"
DB_DSN = "postgresql://postgres:password@timescaledb:5432/crypto"
BATCH_SIZE = 500

async def main():
    # Async Postgres connection
    pool = await asyncpg.create_pool(DB_DSN)
    
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="timescale-storage",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    
    await consumer.start()
    print(f"[consumer] reading from {KAFKA_TOPIC}")
    
    try:
        rows = []
        
        async for msg in consumer:
            d = msg.value
            payload = d.get("payload", {})
            
            rows.append((
                d.get("event_time"),
                payload.get("product_id"),
                payload.get("price"),
                payload.get("volume"),
                d.get("sequence"),
                d.get("ingest_ts_ms"),
            ))
            
            if len(rows) >= BATCH_SIZE:
                try:
                    async with pool.acquire() as conn:
                        await conn.executemany("""
                            INSERT INTO prices (event_time, symbol, price, volume, sequence, ingest_ts)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            ON CONFLICT (sequence, symbol) DO NOTHING
                        """, rows)
                    
                    await consumer.commit()
                    print(f"[db] inserted {len(rows)} rows")
                    rows.clear()
                    
                except Exception as e:
                    print(f"[error] batch insert failed: {e}")
                    rows.clear()
                    
    finally:
        await consumer.stop()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())