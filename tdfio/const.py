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
    male = auto()
    female = auto()
    nb = auto()

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
