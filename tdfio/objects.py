from enum import Enum, auto

class Technique(Enum):
    SKATE = auto()
    CLASSIC = auto()


class Gender(Enum):
    MALE = auto()
    FEMALE = auto()
    NB = auto()