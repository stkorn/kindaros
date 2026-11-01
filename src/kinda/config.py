import os
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID


@dataclass(frozen=True)
class Config:
    brokers: str
    topic: str
    epoch: UUID
    checkpoint: Path

    @property
    def status_path(self) -> Path:
        return self.checkpoint / "kinda-status.json"

    @classmethod
    def load(cls) -> "Config":
        return cls(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                   os.environ.get("POS_TOPIC", "kinda.pos.sales.v1"),
                   UUID(os.environ.get("STREAM_EPOCH", "b1cbe56c-c853-4b97-a2bb-623f547ac31b")),
                   Path(os.environ.get("CHECKPOINT_PATH", "/checkpoints/pos-sales-v1")))
