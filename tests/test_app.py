# tests/test_app.py
import pytest
import pytest_asyncio
import os
import time
import asyncio
import httpx
from httpx import AsyncClient
from datetime import datetime, timezone

# --- Fixtures ---

@pytest.fixture(scope="session")
def event_loop():
    """Fixture untuk event loop asyncio."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="function")
async def async_client(tmp_path):
    """
    Fixture yang membuat app dan client baru untuk setiap tes.
    Setiap tes akan menggunakan file database unik di direktori temporer.
    """
    # Buat path database unik untuk tes ini menggunakan tmp_path dari pytest
    db_path = tmp_path / "test_aggregator.db"

    # Ganti path DB di modul database
    from src import database
    database.DATABASE_PATH = str(db_path)

    # Import dan buat app
    from src.main import create_app
    app = create_app()

    # Gunakan Lifespan Context Manager untuk startup/shutdown yang andal
    async with app.router.lifespan_context(app):
        # Buat client dan jalankan tes
        transport = httpx.ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client # Tes berjalan di sini
    
    # Tidak perlu menghapus file secara manual, tmp_path akan membersihkannya.


# ---- FUNGSI HELPER ----
def create_event(event_id: str, topic="logs"):
    """Helper untuk membuat event."""
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {"message": f"Event {event_id}"}
    }

# --- Tes API (Total 6 Tes) ---

@pytest.mark.asyncio
async def test_publish_single_event(async_client: AsyncClient):
    """(1/6) Tes kirim satu event dan pastikan statusnya queued."""
    event = create_event("e-1")
    response = await async_client.post("/publish", json=event)
    assert response.status_code == 200
    assert response.json() == {"status": "queued", "received_count": 1}


@pytest.mark.asyncio
async def test_publish_batch_events(async_client: AsyncClient):
    """(2/6) Tes kirim batch dua event sekaligus."""
    events = [create_event("e-2"), create_event("e-3")]
    response = await async_client.post("/publish", json=events)
    assert response.status_code == 200
    assert response.json() == {"status": "queued", "received_count": 2}

@pytest.mark.asyncio
async def test_deduplication(async_client: AsyncClient):
    """(3/6) Tes agar event duplikat tidak disimpan dua kali."""
    event = create_event("e-duplicate", topic="logs")
    r1 = await async_client.post("/publish", json=event)
    r2 = await async_client.post("/publish", json=event)
    assert r1.status_code == 200
    assert r2.status_code == 200

    # Beri waktu consumer untuk memproses batch
    await asyncio.sleep(0.3) 

    # Ambil event di DB via endpoint /events
    r3 = await async_client.get("/events?topic=logs") 
    assert r3.status_code == 200
    data = r3.json()

    # Pastikan hanya 1 event dengan ID yang sama
    assert len(data) == 1
    assert data[0]["event_id"] == "e-duplicate"

@pytest.mark.asyncio
async def test_schema_validation_fail(async_client: AsyncClient):
    """(4/6) Tes event dengan skema yang salah (cakupan: Validasi Skema)."""
    bad_event = {"topic": "logs", "event_id": None} # event_id tidak valid
    response = await async_client.post("/publish", json=bad_event)
    assert response.status_code == 422 # Unprocessable Entity

@pytest.mark.asyncio
async def test_get_events_endpoint_consistency(async_client: AsyncClient):
    """(5/6) Tes konsistensi data di /events (cakupan: Konsistensi GET /events)."""
    event_1 = create_event("e-20", topic="topic-b")
    event_2 = create_event("e-20", topic="topic-c") # ID sama, topic beda (unik)

    await async_client.post("/publish", json=[event_1, event_2])
    await asyncio.sleep(0.3)

    # Cek topic-b
    response_b = await async_client.get("/events?topic=topic-b")
    assert response_b.status_code == 200
    assert len(response_b.json()) == 1
    assert response_b.json()[0]["event_id"] == "e-20"

    # Cek topic-c
    response_c = await async_client.get("/events?topic=topic-c")
    assert response_c.status_code == 200
    assert len(response_c.json()) == 1
    assert response_c.json()[0]["event_id"] == "e-20"

@pytest.mark.asyncio
async def test_persistence_on_restart(async_client: AsyncClient, tmp_path):
    """(6/6) Tes simulasi restart untuk memastikan persistensi (cakupan: Persistensi)."""
    # Dapatkan path DB yang digunakan oleh fixture pertama
    db_path = tmp_path / "test_aggregator.db"
    
    # --- Sesi Aplikasi Pertama ---
    event_persist = create_event("e-persist-1")
    await async_client.post("/publish", json=event_persist)
    await asyncio.sleep(0.1)

    stats_1 = await async_client.get("/stats")
    assert stats_1.json()["unique_processed"] == 1

    # --- Sesi Aplikasi Kedua (Simulasi Restart) ---
    # Kita akan membuat app baru yang menunjuk ke file DB yang SAMA
    from src import database
    database.DATABASE_PATH = str(db_path) # Pastikan menunjuk ke file yang sama
    from src.main import create_app

    app2 = create_app()
    async with app2.router.lifespan_context(app2):
        transport2 = httpx.ASGITransport(app=app2)
        async with AsyncClient(transport=transport2, base_url="http://test") as client2:
            # Kirim event yang SAMA lagi
            await client2.post("/publish", json=event_persist)
            await asyncio.sleep(0.3)

            # Cek stats di app KEDUA
            stats_2 = await client2.get("/stats")
            stats_data = stats_2.json()

            # Diterima 1 (di app baru), tapi 0 unik diproses, 1 duplikat
            assert stats_data["received"] == 1
            assert stats_data["unique_processed"] == 0
            assert stats_data["duplicate_dropped"] == 1

            # Cek DB (via endpoint) untuk memastikan hanya ada 1 total
            events = await client2.get("/events?topic=logs")
            assert len(events.json()) == 1

