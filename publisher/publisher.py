import requests
import time
import random
import uuid
from datetime import datetime, timezone
from requests.exceptions import RequestException

# ===========================
# Konfigurasi Publisher
# ===========================
AGGREGATOR_URL = "http://aggregator:8080/publish"
STATS_URL = "http://aggregator:8080/stats"
TOTAL_EVENTS = 5000
DUPLICATE_PERCENT = 0.20
BATCH_SIZE = 100
RETRY_LIMIT = 3
RETRY_DELAY = 1
POLL_INTERVAL = 2

# ===========================
# Fungsi Pembantu
# ===========================

def generate_event(event_id=None, topic="logs"):
    """Membuat satu event JSON sesuai spesifikasi UTS."""
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "compose-publisher",
        "payload": {"data": "performance_test_data"},
    }

def safe_post(url, json, retries=RETRY_LIMIT, delay=RETRY_DELAY):
    """Melakukan POST dengan mekanisme retry untuk keandalan."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(url, json=json, timeout=5)
            if r.status_code == 200:
                return True
            else:
                print(f"[WARN] Attempt {attempt}: status {r.status_code} - {r.text}")
        except RequestException as e:
            print(f"[ERROR] Attempt {attempt}: {e}")
        time.sleep(delay)
    return False

def wait_for_processing(total_events):
    """Polling /stats sampai semua event terproses (unik + duplikat)."""
    print("\nMenunggu semua event diproses oleh consumer...")
    while True:
        try:
            stats = requests.get(STATS_URL, timeout=5).json()
            processed_count = stats.get("unique_processed", 0) + stats.get("duplicate_dropped", 0)
            if processed_count >= total_events:
                print("Semua event telah diproses ✅")
                return stats
            print(f"Progress: {processed_count}/{total_events} ...")
        except Exception as e:
            print(f"[WARN] Gagal ambil stats: {e}")
        time.sleep(POLL_INTERVAL)

# ===========================
# Main Function
# ===========================

def run_test():
    print(f"Menunggu aggregator siap di {AGGREGATOR_URL}...")
    while True:
        try:
            requests.get(STATS_URL, timeout=3)
            print("Aggregator siap ✅\n")
            break
        except requests.ConnectionError:
            time.sleep(2)

    # Membuat daftar event unik
    unique_ids = [str(uuid.uuid4()) for _ in range(int(TOTAL_EVENTS * (1 - DUPLICATE_PERCENT)))]
    events_to_send = [generate_event(eid) for eid in unique_ids]

    # Tambahkan event duplikat
    num_duplicates = TOTAL_EVENTS - len(events_to_send)
    for _ in range(num_duplicates):
        events_to_send.append(generate_event(random.choice(unique_ids)))

    random.shuffle(events_to_send)
    print(f"Mengirim total {len(events_to_send)} event ({num_duplicates} duplikat)...\n")

    start_time = time.time()
    total_batches = (len(events_to_send) + BATCH_SIZE - 1) // BATCH_SIZE

    # Kirim dalam batch
    for i in range(0, len(events_to_send), BATCH_SIZE):
        batch = events_to_send[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        success = safe_post(AGGREGATOR_URL, batch)
        status = "✅ OK" if success else "❌ Gagal"
        print(f"Batch {batch_num}/{total_batches} dikirim... {status}")

    end_time = time.time()
    print(f"\nSelesai mengirim {len(events_to_send)} event dalam {end_time - start_time:.2f} detik.")

    # Tunggu sampai semua event terproses
    stats = wait_for_processing(len(events_to_send))

    print("\n--- STATISTIK AKHIR ---")
    print(f"Total Received:     {stats['received']}")
    print(f"Unique Processed:   {stats['unique_processed']}")
    print(f"Duplicates Dropped: {stats['duplicate_dropped']}")
    print(f"Uptime:             {stats['uptime_seconds']:.2f}s")
    print(f"Topics:             {stats['topics']}")

    # Validasi hasil
    assert stats['received'] == TOTAL_EVENTS
    assert stats['unique_processed'] == len(unique_ids)
    assert stats['duplicate_dropped'] == num_duplicates
    print("\nVerifikasi statistik berhasil! ✅")

# ===========================
# Entry Point
# ===========================
if __name__ == "__main__":
    run_test()
