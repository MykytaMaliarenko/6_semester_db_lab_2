import typing as t
from enum import Enum


class TestStatus(Enum):
    PASSED = 'Зараховано'
    DIDNT_SHOW_UP = 'Не з’явився'
    BELOW_THE_THRESHOLD = 'Не подолав поріг'
    NOT_SET = 'Не обрано 100-200'
    ANNULLED = 'Анульовано'


class SubjectCodes(Enum):
    UKRAINIAN_LANGUAGE_AND_LITERATURE = 'UML'
    UKRAINIAN_LANGUAGE = 'Ukr'
    PHYSICS = 'Phys'
    MATH = 'Math'
    HISTORY = 'His'
    ENGLISH = 'Eng'
    GEOGRAPHY = 'Geo'
    CHEMISTRY = 'Chem'
    BIOLOGY = 'Bio'
    FRENCH = 'Fra'
    GERMAN = 'Deu'
    SPAIN = 'Spa'


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
