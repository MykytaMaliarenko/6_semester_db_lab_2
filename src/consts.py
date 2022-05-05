import typing as t
from enum import Enum


class TestStatus(Enum):
    PASSED = 'Зараховано'
    DIDNT_SHOW_UP = 'Не з’явився'
    BELOW_THE_THRESHOLD = 'Не подолав поріг'
    NOT_SET = 'Не обрано 100-200'
    ANNULLED = 'Анульовано'


class SubjectCodes(Enum):
    UKRAINIAN_LANGUAGE_AND_LITERATURE = 'Ukr'
    HISTORY = 'Hist'
    MATH = 'Math'
    PHYSICS = 'Phys'
    CHEMISTRY = 'Chem'
    BIOLOGY = 'Bio'
    GEOGRAPHY = 'Geo'
    ENGLISH = 'Eng'
    FRENCH = 'Fra'
    GERMAN = 'Deu'
    SPANISH = 'Spa'

    @property
    def name(self) -> str:
        if self == SubjectCodes.UKRAINIAN_LANGUAGE_AND_LITERATURE:
            return 'Українська мова і література'
        elif self == SubjectCodes.HISTORY:
            return 'Історія України'
        elif self == SubjectCodes.MATH:
            return 'Математика'
        elif self == SubjectCodes.PHYSICS:
            return 'Фізика'
        elif self == SubjectCodes.CHEMISTRY:
            return 'Хімія'
        elif self == SubjectCodes.BIOLOGY:
            return 'Біологія'
        elif self == SubjectCodes.GEOGRAPHY:
            return 'Географія'
        elif self == SubjectCodes.ENGLISH:
            return 'Англійська мова'
        elif self == SubjectCodes.FRENCH:
            return 'Французька мова'
        elif self == SubjectCodes.GERMAN:
            return 'Німецька мова'
        elif self == SubjectCodes.SPANISH:
            return 'Іспанська мова'
        else:
            return super(SubjectCodes, self).name


def generate_subject_fields(field_name: str, _type: t.Type) -> t.Dict:
    return {f'{subject.value}{field_name}': _type for subject in SubjectCodes}


EXTRA_FIELDS = {
    'year': int
}

SPECIAL_FIELDS_SCHEMA = {
    'Birth': int,
    **generate_subject_fields('Ball12', int),
    **generate_subject_fields('Ball100', float),
    **generate_subject_fields('Ball', float),
    **EXTRA_FIELDS,
}


NOT_NULLABLE_FIELDS = [
    'OUTID',
    'RegName',
    'TERNAME',
    'Birth',
    'RegTypeName',
    'SexTypeName',
    'TerTypeName',
    'AREANAME',
]


SQL_TYPE_BINDINGS = {
    str: 'varchar(500)',
    int: 'integer',
    float: 'numeric(4,1)'
}

PRIMAY_KEY = 'OUTID'

TABLE_NAME = 'zno_data'
