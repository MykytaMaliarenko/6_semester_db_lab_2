import typing as t
from consts import (
    SPECIAL_FIELDS_SCHEMA, NOT_NULLABLE_FIELDS,
    PRIMAY_KEY, SQL_TYPE_BINDINGS,
    TABLE_NAME
)
from models import Student, Exam
from consts import SubjectCodes


GET_PLACE_FUNCTON_TYPE = t.Callable[[str, str, str], int]
GET_EI_FUNCTON_TYPE = t.Callable[
    [str, str, str, str, t.Optional[str], t.Optional[str]],
    int
]


def extract_student(
        entry: t.Dict,
        get_place: t.Callable,
        get_educational_institute: t.Callable
) -> Student:
    return Student(
        id=entry['outid'],
        gender=entry['sextypename'],
        birth_year=int(entry['birth']),
        studying_lang=entry['classlangname'],
        place_of_living_id=get_place(
            entry['regname'],
            entry['areaname'],
            entry['tername']
        ),
        educational_institution_id=get_educational_institute(
            entry['eoname'],
            entry['eoregname'],
            entry['eoareaname'],
            entry['eotername'],
            entry['eotypename'],
            entry['eoparent'],
        ),
        class_profile=entry['classprofilename']
    )


def extract_exams(
        entry: t.Dict,
        year: int,
        get_educational_institute: t.Callable,
        get_subject_id: t.Callable[[str], int],
) -> list[Exam]:
    parsed_exams = list()
    for subject in SubjectCodes:
        code = subject.value.lower()

        pt_name = entry[f'{code}ptname']
        pt_reg_name = entry[f'{code}ptregname']
        pt_area_name = entry[f'{code}ptareaname']
        pt_ter_name = entry[f'{code}pttername']
        ball = entry[f'{code}ball']
        ball100 = entry[f'{code}ball100']
        ball12 = entry[f'{code}ball12']
        test_status = entry[f'{code}teststatus']
        if pt_name is None or ball is None:
            continue

        ei_id = get_educational_institute(
            pt_name,
            pt_reg_name,
            pt_area_name,
            pt_ter_name,
            None,
            None
        )

        parsed_exams.append(Exam(
            id=None,
            subject_id=get_subject_id(subject.value),
            student_id=entry['outid'],
            educational_institution_id=ei_id,
            raw_score=_sql_repr_value(ball, float),
            score=_sql_repr_value(ball100, float),
            school_score=_sql_repr_value(ball12, int),
            year=year,
            status=test_status
        ))

    return parsed_exams


def _sql_repr_value(v: str, expected_type: t.Type) -> str:
    if v == 'null' or v is None:
        return v

    v = v.replace(',', '.').replace("'", "''")
    if expected_type == int:
        return int(v)
    elif expected_type == float:
        return round(float(v), 1)
    else:
        return f"'{v}'"


def generate_data_schema_from_entry(entry: t.Dict) -> t.Dict[str, t.Type]:
    return {key: str for key in entry}


def generate_bulk_insert_row_sql(rows: t.List[t.Dict[str, str]]) -> str:
    def _generate_sql_values(row: t.Dict[str, str]):
        values = []
        for field, value in row.items():
            _type = (
                SPECIAL_FIELDS_SCHEMA[field]
                if field in SPECIAL_FIELDS_SCHEMA
                else str
            )
            values.append(_sql_repr_value(value, _type))

        return '(' + ','.join(values) + ')'

    column_names = ",".join(map(
        lambda key: key.replace('"', ''),
        rows[0].keys()
    ))

    return f'insert into {TABLE_NAME}({column_names}) ' \
           f'values {",".join([_generate_sql_values(entry) for entry in rows])} ' \
           f'ON CONFLICT DO NOTHING'


def generate_create_table_sql(fields: t.Dict[str, t.Type]) -> str:
    def _generate_sql_field(field: str) -> str:
        _type = (
            SPECIAL_FIELDS_SCHEMA[field]
            if field in SPECIAL_FIELDS_SCHEMA
            else str
        )
        is_nullable = ' NOT NULL' if field in NOT_NULLABLE_FIELDS else ''
        is_primary_key = ' PRIMARY KEY' if field == PRIMAY_KEY else ''
        field_name = field.replace('"', '')
        return f'{field_name} {SQL_TYPE_BINDINGS[_type]}{is_primary_key}{is_nullable}'

    sql_fields = ',\n'.join([_generate_sql_field(field) for field in fields])
    return f'create table if not exists {TABLE_NAME} ({sql_fields});'
