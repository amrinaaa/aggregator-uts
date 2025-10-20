# src/stats.py
import time
from dataclasses import dataclass, field

@dataclass
class StatsTracker:
    """
    Kelas untuk melacak statistik operasional aggregator.
    Digunakan untuk menghitung jumlah event diterima, unik, duplikat, dan uptime sistem.
    """
    received: int = 0
    unique_processed: int = 0
    duplicate_dropped: int = 0
    start_time: float = field(default_factory=time.monotonic)

    def inc_received(self, count: int = 1):
        """Menambah jumlah event yang diterima dari publisher."""
        self.received += count

    def inc_unique(self, count: int = 1):
        """Menambah jumlah event unik yang berhasil diproses."""
        self.unique_processed += count

    def inc_duplicate(self, count: int = 1):
        """Menambah jumlah event duplikat yang di-drop."""
        self.duplicate_dropped += count

    def get_stats(self) -> dict:
        """Mengembalikan statistik sistem dalam bentuk dictionary."""
        uptime = time.monotonic() - self.start_time
        return {
            "received": self.received,
            "unique_processed": self.unique_processed,
            "duplicate_dropped": self.duplicate_dropped,
            "uptime_seconds": round(uptime, 2),
            "throughput": (
                round(self.unique_processed / uptime, 4)
                if uptime > 0 else 0
            ),
            "duplicate_rate": (
                round(self.duplicate_dropped / self.received, 4)
                if self.received > 0 else 0
            ),
        }