from enum import Enum, auto


class Event(Enum):
    skadischase = auto()
    firstchance = auto()

    def to_string(self) -> str:
        return self.name

    @staticmethod
    def from_string(s: str):
        if s == 'skadischase':
            return Event.skadischase
        elif s == 'firstchance':
            return Event.firstchance

        raise ValueError(f'Unrecognized event string representation {s}')
