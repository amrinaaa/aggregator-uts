# UTS - Pub-Sub Log Aggregator

Layanan aggregator log berbasis FastAPI yang idempotent dan melakukan deduplikasi, berjalan di dalam Docker.

## Fitur
- **API (FastAPI)**: Endpoint untuk `/publish`, `/events`, dan `/stats`.
- **Idempotency**: Consumer idempotent menggunakan SQLite.
- **Deduplikasi**: Berdasarkan `(topic, event_id)` sebagai Primary Key.
- **Persistensi**: Data deduplikasi tahan restart container (via SQLite).
- **Containerized**: Sepenuhnya berjalan di Docker dengan user non-root.
- **Bonus**: Termasuk `docker-compose.yml` dengan service publisher terpisah untuk stress test.

## Cara Menjalankan

### Opsi 1: Docker (Wajib)

1.  **Build Image:**
    ```sh
    docker build -t uts-aggregator .
    ```

2.  **Run Container:**
    Kita harus me-mount volume agar database SQLite persisten.

    ```sh
    # Buat direktori data di host
    mkdir -p (pwd)/data
    
    # Jalankan container dengan volume
    docker run -d -p 8080:8080 \
      -v  (pwd)/data:/app/data \
      --name my-aggregator \
      uts-aggregator
    ```

### Opsi 2: Docker Compose (Bonus)

Ini akan menjalankan aggregator DAN publisher untuk stress test.

1.  **Build dan Run:**
    ```sh
    docker-compose up --build
    ```

    Anda akan melihat output dari `publisher` yang mengirim 5000 event dan kemudian log dari `aggregator` yang memprosesnya (dan mendeteksi duplikat). `publisher` akan berhenti setelah selesai, tetapi `aggregator` akan tetap berjalan.

2.  **Cek Hasil:**
    Setelah publisher selesai, cek stats:
    ```sh
    curl http://localhost:8080/stats
    ```
    Outputnya akan terlihat seperti:
    `{"received":5000,"unique_processed":3750,"duplicate_dropped":1250, ...}`

3.  **Hentikan:**
    ```sh
    docker-compose down
    ```

## Menjalankan Unit Tests

Tes harus dijalankan dari root direktori proyek.

1.  **Install dependencies (di venv):**
    ```sh
    pip install -r requirements.txt
    pip install pytest pytest-asyncio httpx
    ```

2.  **Run Pytest:**
    ```sh
    python -m pytest
    ```
    *Catatan: Tes akan membuat file `test_aggregator.db` di direktori `/data` dan menghapusnya secara otomatis.*

## API Endpoints

-   `POST /publish`
    -   Menerima satu event JSON atau array event.
    -   Body (single): `{ "topic": "...", "event_id": "...", ... }`
    -   Body (batch): `[ { ... }, { ... } ]`

-   `GET /events?topic={topic_name}`
    -   Mengembalikan semua event unik yang telah diproses untuk topic tersebut.

-   `GET /stats`
    -   Mengembalikan statistik operasional.