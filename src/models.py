from pydantic import BaseModel, Field
from datetime import datetime
from typing import Dict, Any

# Event JSON 
class Event(BaseModel):
    topic: str
    event_id: str = Field(..., min_length=1)
    timestamp: datetime
    source: str
    payload: Dict[str, Any]

    # Kunci unik untuk deduplikasi
    @property
    def unique_key(self) -> tuple:
        return (self.topic, self.event_id)