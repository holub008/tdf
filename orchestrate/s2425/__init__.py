from enum import auto, Enum


class Event(Enum):
    skadischase = auto()

    def to_string(self) -> str:
        return self.name

    @staticmethod
    def from_string(s: str):
        if s == 'skadischase':
            return Event.skadischase

        raise ValueError(f'Unrecognized event string representation {s}')