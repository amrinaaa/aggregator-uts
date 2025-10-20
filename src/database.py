import aiosqlite
import json
import logging
from .models import Event
from typing import List, Dict, Any

DATABASE_PATH = "/app/data/aggregator.db"

async def init_db():
    """Inisialisasi database dan tabel."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute("PRAGMA synchronous = NORMAL")
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                topic TEXT NOT NULL,
                event_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                source TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (topic, event_id)
            )
        """)
        await db.commit()
        logging.info(f"Database initialized at {DATABASE_PATH}")

async def batch_mark_events_processed(db: aiosqlite.Connection, events: List[Event]) -> int:
    """
    Mencoba menyimpan batch event ke DB dalam satu transaksi.
    Menggunakan 'INSERT OR IGNORE' untuk idempotency atomik.
    
    Args:
        db: Koneksi aiosqlite yang sudah ada.
        events: Daftar event yang akan diproses.
        
    Return:
        int: Jumlah event baru (unik) yang berhasil disimpan.
    """
    # Siapkan data untuk executemany
    data_to_insert = [
        (
            event.topic,
            event.event_id,
            event.timestamp.isoformat(),
            event.source,
            json.dumps(event.payload)
        ) for event in events
    ]
    
    if not data_to_insert:
        return 0

    try:
        # Simpan total changes sebelumnya
        initial_changes = db.total_changes
        
        await db.executemany(
            """
            INSERT OR IGNORE INTO processed_events 
                (topic, event_id, timestamp, source, payload_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            data_to_insert
        )
        await db.commit()
        
        # 'total_changes' akan berisi jumlah baris yang *benar-benar* di-INSERT
        # Kita hitung selisihnya untuk mendapatkan jumlah baris yang baru ditambahkan
        newly_inserted_count = db.total_changes - initial_changes
        return newly_inserted_count
        
    except aiosqlite.Error as e:
        logging.error(f"Database error while batch marking events: {e}", exc_info=True)
        return 0 # Asumsikan gagal memproses

async def get_events_by_topic(topic: str) -> List[Dict[str, Any]]:
    """Mengambil semua event unik yang telah diproses untuk sebuah topic."""
    events = []
    try:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM processed_events WHERE topic = ? ORDER BY timestamp", 
                (topic,)
            ) as cursor:
                async for row in cursor:
                    events.append({
                        "topic": row["topic"],
                        "event_id": row["event_id"],
                        "timestamp": row["timestamp"],
                        "source": row["source"],
                        "payload": json.loads(row["payload_json"])
                    })
    except aiosqlite.Error as e:
        logging.error(f"Database error while getting events: {e}", exc_info=True)
    return events

async def get_topic_stats() -> Dict[str, int]:
    """Menghitung jumlah event unik per topic."""
    stats = {}
    try:
        async with aiosqlite.connect(DATABASE_PATH) as db:
            async with db.execute(
                "SELECT topic, COUNT(*) as count FROM processed_events GROUP BY topic"
            ) as cursor:
                async for row in cursor:
                    stats[row[0]] = row[1]
    except aiosqlite.Error as e:
        logging.error(f"Database error while getting topic stats: {e}", exc_info=True)
    return stats