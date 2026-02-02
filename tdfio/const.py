from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Optional

import polars as pl


class Technique(Enum):
    SKATE = auto()
    CLASSIC = auto()

    @staticmethod
    def from_string(s: str):
        if s == 'skate':
            return Technique.SKATE
        elif s == 'classic':
            return Technique.CLASSIC
        else:
            raise ValueError(f'Unrecognized technique {s}')


class Gender(Enum):
    male = auto()
    female = auto()
    nb = auto()

    def to_string(self):
        """
        deprecated
        """
        if self == Gender.male:
            return 'male'
        elif self == Gender.female:
            return 'female'
        else:
            return 'nb'

    def __str__(self) -> str:
        return self.to_string()


class Event(Enum):
    """
    an interface for events, to be used in our logic modules.
    create a new Event enum with each season (TdF events rotate season to season)

    This class would ideally be an ABC, but Python OOP doesn't jive with that.
    """

    def to_string(self) -> str:
        raise NotImplementedError

    def get_human_readable_name(self) -> str:
        raise NotImplementedError

    def get_event_days(self) -> Optional[int]:
        raise NotImplementedError

    def save_df(self, df: pl.DataFrame, g: Gender):
        raise NotImplementedError
