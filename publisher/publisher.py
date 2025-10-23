import requests
import time
import random
import uuid
from datetime import datetime, timezone
from requests.exceptions import RequestException
import logging

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

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

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
            r = requests.post(url, json=json, timeout=10)
            if r.status_code == 200:
                return True, r.elapsed.total_seconds() * 1000
            else:
                logging.warning(f"Attempt {attempt}: status {r.status_code} - {r.text}")
        except RequestException as e:
            logging.error(f"Attempt {attempt}: {e}")
        time.sleep(delay)
    return False, 0

def wait_for_processing(total_events):
    """Polling /stats sampai semua event terproses (unik + duplikat)."""
    logging.info("\nMenunggu semua event diproses oleh consumer...")
    stats_latencies_ms = [] # List untuk menyimpan latensi GET /stats
    
    while True:
        try:
            poll_start_time = time.perf_counter()
            response = requests.get(STATS_URL, timeout=5)
            response.raise_for_status() # Cek error HTTP
            stats = response.json()
            poll_end_time = time.perf_counter()
            
            # (TAMBAHAN) Mengukur latensi GET /stats
            poll_latency_ms = (poll_end_time - poll_start_time) * 1000
            stats_latencies_ms.append(poll_latency_ms)
            
            # Kunci 'duplicate_dropped' harus sesuai dengan response API Anda
            processed_count = stats.get("unique_processed", 0) + stats.get("duplicate_dropped", 0)
            
            if processed_count >= total_events:
                logging.info("Semua event telah diproses ✅")
                return stats, stats_latencies_ms # Return stats dan daftar latensi
            
            logging.info(f"Progress: {processed_count}/{total_events} ... (Stats latency: {poll_latency_ms:.2f}ms)")
            
        except Exception as e:
            logging.warning(f"Gagal ambil stats: {e}")
            
        time.sleep(POLL_INTERVAL)

# ===========================
# Fungsi Analisis (DIMODIFIKASI)
# ===========================

def analyze_latencies(latencies_ms, name, indent="  "):
    """Menghitung dan menampilkan statistik latensi."""
    if not latencies_ms:
        return
    
    avg_latency = sum(latencies_ms) / len(latencies_ms)
    max_latency = max(latencies_ms)
    min_latency = min(latencies_ms)
    
    print(f"\n{indent}Analisis Latensi - {name}:")
    print(f"{indent}  Rata-rata: {avg_latency:.2f} ms")
    print(f"{indent}  Max:       {max_latency:.2f} ms")
    print(f"{indent}  Min:       {min_latency:.2f} ms")

# ===========================
# Main Function
# ===========================

def run_test():
    logging.info(f"Menunggu aggregator siap di {AGGREGATOR_URL}...")
    while True:
        try:
            requests.get(STATS_URL, timeout=3)
            logging.info("Aggregator siap ✅\n")
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
    logging.info(f"Mengirim total {len(events_to_send)} event ({num_duplicates} duplikat)...\n")

    total_ingestion_start = time.perf_counter()
    total_batches = (len(events_to_send) + BATCH_SIZE - 1) // BATCH_SIZE
    
    batch_latencies_ms = [] # List untuk latensi per batch

    # Kirim dalam batch
    for i in range(0, len(events_to_send), BATCH_SIZE):
        batch = events_to_send[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        
        success, latency_ms = safe_post(AGGREGATOR_URL, batch)
        
        if success:
            batch_latencies_ms.append(latency_ms)
            
        status = "✅ OK" if success else "❌ Gagal"
        logging.info(f"Batch {batch_num}/{total_batches} dikirim... {status} (Latency: {latency_ms:.2f}ms)")

    total_ingestion_end = time.perf_counter()
    total_ingestion_time_sec = total_ingestion_end - total_ingestion_start
    
    print(f"\nSelesai mengirim {len(events_to_send)} event dalam {total_ingestion_time_sec:.2f} detik.")

    # Tunggu dan ambil latensi stats
    stats, stats_latencies_ms = wait_for_processing(len(events_to_send))

    # ========================================
    # (MODIFIKASI) LAPORAN STATISTIK GABUNGAN
    # ========================================
    print("\n--- STATISTIK AKHIR (GABUNGAN) ---")
    
    print("\n[ Hasil Aggregator ]")
    # Menggunakan .get() untuk keamanan jika key tidak ada
    print(f"  Total Received:     {stats.get('received', 'N/A')}")
    print(f"  Unique Processed:   {stats.get('unique_processed', 'N/A')}")
    print(f"  Duplicates Dropped: {stats.get('duplicate_dropped', 'N/A')}")
    print(f"  Uptime:             {stats.get('uptime_seconds', 0.0):.2f}s")
    print(f"  Topics:             {stats.get('topics', [])}")

    print("\n[ Kinerja & Latensi (Publisher) ]")
    print(f"  Total Waktu Ingesti: {total_ingestion_time_sec:.2f} detik")
    print(f"  Avg. Throughput:     {TOTAL_EVENTS / total_ingestion_time_sec:.2f} event/detik")
    
    # Panggil fungsi analyze_latencies dengan indentasi
    analyze_latencies(batch_latencies_ms, f"POST /publish (per batch {BATCH_SIZE} event)", indent="  ")
    analyze_latencies(stats_latencies_ms, "GET /stats (Responsivitas)", indent="  ")

    # Validasi hasil
    print("\n--- VERIFIKASI ---")
    # Memastikan assert menggunakan key yang benar
    assert stats.get('received') == TOTAL_EVENTS
    assert stats.get('unique_processed') == len(unique_ids)
    assert stats.get('duplicate_dropped') == num_duplicates
    print("Verifikasi statistik berhasil! ✅")

# ===========================
# Entry Point
# ===========================
if __name__ == "__main__":
    run_test()