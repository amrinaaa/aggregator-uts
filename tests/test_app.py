# tests/test_app.py
import pytest
import pytest_asyncio
import asyncio
import os
import httpx
from datetime import datetime, timezone

# Path database untuk testing
TEST_DB_PATH = "/app/data/test_aggregator.db"
os.makedirs("/app/data", exist_ok=True)


# --- Fixtures ---

@pytest.fixture(scope="session")
def event_loop():
    """Fixture untuk event loop asyncio (dibutuhkan pytest-asyncio)."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="function")
async def async_client():
    """Fixture membuat FastAPI app dan HTTPX client untuk tiap test."""
    # Bersihkan database lama jika ada
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)

    # Ganti path database ke test DB
    from src import database
    database.DATABASE_PATH = TEST_DB_PATH

    # Import dan buat instance FastAPI app
    from src.main import create_app
    app = create_app()

    # Jalankan startup manual (inisialisasi DB & background task)
    await app.router.startup()

    # Gunakan transport ASGI
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    # Jalankan shutdown manual
    await app.router.shutdown()

    # Hapus database setelah tes
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)


# --- Helper untuk membuat event ---

def create_event(event_id: str, topic="logs"):
    """Buat event dengan ID unik."""
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test_publisher",
        "payload": {"message": f"Event {event_id}"}
    }


# --- Tes API ---

@pytest.mark.asyncio
async def test_publish_single_event(async_client: httpx.AsyncClient):
    """Tes kirim satu event dan pastikan statusnya queued."""
    event = create_event("e-1")
    response = await async_client.post("/publish", json=event)
    assert response.status_code == 200
    assert response.json() == {"status": "queued", "received_count": 1}


@pytest.mark.asyncio
async def test_publish_batch_events(async_client: httpx.AsyncClient):
    """Tes kirim batch dua event sekaligus."""
    events = [create_event("e-2"), create_event("e-3")]
    response = await async_client.post("/publish", json=events)
    assert response.status_code == 200
    assert response.json() == {"status": "queued", "received_count": 2}


@pytest.mark.asyncio
async def test_deduplication(async_client: httpx.AsyncClient):
    """Tes agar event duplikat tidak disimpan dua kali."""
    event = create_event("e-duplicate", topic="logs") # Tentukan topic
    r1 = await async_client.post("/publish", json=event)
    r2 = await async_client.post("/publish", json=event)
    assert r1.status_code == 200
    assert r2.status_code == 200

    # Beri waktu consumer untuk memproses batch
    await asyncio.sleep(0.1) 

    # Ambil event di DB via endpoint /events
   # test_app.py -> test_deduplication
# ...
    # Ambil event di DB via endpoint /events
    # HARUS ADA PARAMETER TOPIC
    r3 = await async_client.get("/events?topic=logs") 
    assert r3.status_code == 200
    data = r3.json()
# ...)

    # Pastikan hanya 1 event dengan ID yang sama
    assert len(data) == 1
    assert data[0]["event_id"] == "e-duplicate"