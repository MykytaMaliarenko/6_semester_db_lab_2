import typing as t
from consts import (
    SPECIAL_FIELDS_SCHEMA, NOT_NULLABLE_FIELDS,
    PRIMAY_KEY, SQL_TYPE_BINDINGS,
    TABLE_NAME
)


def _sql_repr_value(v: str, expected_type: t.Type) -> str:
    if v == 'null':
        return v

    v = v.replace(',', '.').replace("'", "''")
    if expected_type == int:
        return v
    elif expected_type == float:
        return str(round(float(v), 1))
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

    sql_fields = ','.join([_generate_sql_field(field) for field in fields])
    return f'create table if not exists {TABLE_NAME} ({sql_fields});'
