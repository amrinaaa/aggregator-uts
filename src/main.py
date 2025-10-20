# main.py

import asyncio
import logging
import uvicorn
import time
import aiosqlite
from fastapi import FastAPI, Request, HTTPException, Query
from pydantic import ValidationError
from typing import List, Union

from . import database
from .models import Event
from .stats import StatsTracker

# Konfigurasi logging dasar
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Konfigurasi Consumer ---
CONSUMER_BATCH_SIZE = 100
CONSUMER_WORKERS = 2

# Fungsi factory untuk testing
def create_app() -> FastAPI:
    app = FastAPI(title="Idempotent Log Aggregator")
    
    # In-memory queue dan stats tracker
    app.state.event_queue = asyncio.Queue()
    app.state.stats_tracker = StatsTracker()

    @app.on_event("startup")
    async def startup_event():
        """Saat startup: inisialisasi DB dan jalankan consumer task(s)."""
        await database.init_db()
        
        # Buat task background untuk consumer
        app.state.consumer_tasks = []
        for i in range(CONSUMER_WORKERS):
            task = asyncio.create_task(
                consumer(f"consumer-{i}", app.state.event_queue, app.state.stats_tracker)
            )
            app.state.consumer_tasks.append(task)
            
        logging.info(f"Application startup complete with {CONSUMER_WORKERS} consumer(s).")

    @app.on_event("shutdown")
    async def shutdown_event():
        """Saat shutdown: batalkan semua consumer task."""
        logging.info(f"Shutting down {len(app.state.consumer_tasks)} consumer task(s)...")
        for task in app.state.consumer_tasks:
            task.cancel()
        
        await asyncio.gather(*app.state.consumer_tasks, return_exceptions=True)
        logging.info("Application shutdown complete.")

    # ===================================================================
    # IMPLEMENTASI CONSUMER BARU
    # ===================================================================
    async def consumer(worker_id: str, queue: asyncio.Queue, stats: StatsTracker):
        """Background task yang memproses event dari queue DALAM BATCH."""
        logging.info(f"[{worker_id}] Batch consumer task started...")
        
        # Buka koneksi DB sekali per worker dan tahan
        async with aiosqlite.connect(database.DATABASE_PATH) as db:
            logging.info(f"[{worker_id}] Consumer connected to DB.")
            
            while True:
                batch = []
                try:
                    # 1. Tunggu event pertama (ini adalah titik 'blocking' utama)
                    event = await queue.get()
                    batch.append(event)
                    
                    # 2. Kumpulkan event lain (jika ada) tanpa menunggu
                    # Tujuannya untuk mengosongkan queue secepat mungkin
                    while len(batch) < CONSUMER_BATCH_SIZE and not queue.empty():
                        batch.append(queue.get_nowait())
                        
                except asyncio.QueueEmpty:
                    pass
                except asyncio.CancelledError:
                    logging.info(f"[{worker_id}] Consumer task stopping.")
                    break # Keluar dari loop utama
                except Exception as e:
                    logging.error(f"[{worker_id}] Error getting from queue: {e}", exc_info=True)
                    continue # Coba lagi

                # 3. Proses batch yang sudah terkumpul
                if batch:
                    try:
                        start_batch_time = time.monotonic()
                        
                        # Panggil fungsi database BATCH
                        new_count = await database.batch_mark_events_processed(db, batch)
                        
                        end_batch_time = time.monotonic()
                        
                        duplicate_count = len(batch) - new_count
                        
                        # Update stats (ini thread-safe)
                        stats.inc_unique(new_count)
                        stats.inc_duplicate(duplicate_count)
                        
                        # Tandai semua task di queue sebagai 'done'
                        for _ in batch:
                            queue.task_done()
                        
                        logging.info(
                            f"[{worker_id}] Processed batch of {len(batch)}. "
                            f"New: {new_count}, Duplicates: {duplicate_count}. "
                            f"Time: {(end_batch_time - start_batch_time)*1000:.2f}ms"
                        )
                    
                    except aiosqlite.Error as e:
                        logging.error(f"[{worker_id}] Error processing batch: {e}", exc_info=True)
                    except asyncio.CancelledError:
                        logging.info(f"[{worker_id}] Consumer task stopping during batch process.")
                        break
                    except Exception as e:
                        logging.error(f"[{worker_id}] Unexpected error in consumer loop: {e}", exc_info=True)
                        
        logging.info(f"[{worker_id}] Consumer task finished.")
    # ===================================================================

    # --- API Endpoints ---

    @app.post("/publish")
    async def publish_events(
        events: Union[Event, List[Event]], 
        request: Request
    ):
        """
        Menerima satu atau batch event dan menambahkannya ke queue.
        """
        queue = request.app.state.event_queue
        stats = request.app.state.stats_tracker
        
        event_list = [events] if isinstance(events, Event) else events
        
        for event in event_list:
            await queue.put(event)
        
        stats.inc_received(len(event_list))
        return {"status": "queued", "received_count": len(event_list)}

    @app.get("/events", response_model=List[Event])
    async def get_processed_events(topic: str = Query(..., min_length=1)):
        """Mengembalikan daftar event unik yang telah diproses untuk topic tertentu."""
        events = await database.get_events_by_topic(topic)
        # Pydantic akan memvalidasi ulang output
        return events

    @app.get("/stats")
    async def get_aggregator_stats(request: Request):
        """Menampilkan statistik operasional."""
        stats_tracker = request.app.state.stats_tracker
        current_stats = stats_tracker.get_stats()
        
        # Ambil jumlah unik per topic dari DB
        topic_stats = await database.get_topic_stats()
        current_stats["topics"] = topic_stats
        
        return current_stats

    return app

# Buat aplikasi
app = create_app()

if __name__ == "__main__":
    # Ini akan dieksekusi oleh CMD ["python", "-m", "src.main"]
    uvicorn.run("src.main:app", host="0.0.0.0", port=8080)