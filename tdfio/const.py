from enum import Enum, auto


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
    MALE = auto()
    FEMALE = auto()
    NB = auto()

    @staticmethod
    def from_string(s: str):
        if s == 'male':
            return Gender.MALE
        elif s == 'female':
            return Gender.FEMALE
        elif s == 'nb':
            return Gender.NB
        else:
            raise ValueError(f'Unrecognized gender {s}')
