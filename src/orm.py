import typing as t
from psycopg2.extensions import cursor
from dataclasses import dataclass, fields


@dataclass
class ModelMeta:
    table_name: str


@dataclass
class AbstractModel:
    id: t.Optional[int]
    META: t.ClassVar[ModelMeta]  # type: ModelMeta

    def __init__(self, *args, **kwargs):
        assert self.META is not None, \
            f'{self.__class__.__name__} requires meta'

        super(AbstractModel, self).__init__(*args, **kwargs)

    @staticmethod
    def _sql_repr_value(value: t.Any) -> str:
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        elif value is None:
            return 'null'
        else:
            return str(value)

    @classmethod
    def create_from_db_result(
            cls,
            _fields: list[str],
            values: tuple[t.Any]
    ) -> 'AbstractModel':
        assert len(_fields) == len(values), \
            'len of fields and values doesnt match'
        return cls(**{
            field_name: values[index]
            for index, field_name in enumerate(_fields)
        })

    @classmethod
    def get_from_db(
            cls, c: cursor,
            **kwargs
    ) -> t.Optional['AbstractModel']:
        field_names = [field.name for field in fields(cls)]
        where_clause = ' and '.join([
            f'{key} = {cls._sql_repr_value(value)}'
            for key, value in kwargs.items()
        ])
        sql = f'select {",".join(field_names)} from {cls.META.table_name} ' \
              f'where {where_clause};'

        c.execute(sql)
        db_res = c.fetchone()
        return (
            cls.create_from_db_result(field_names, db_res)
            if db_res
            else None
        )

    def create(self, c: cursor) -> 'AbstractModel':
        field_names = [field.name for field in fields(self)]
        if self.id is None:
            field_names.pop(field_names.index('id'))

        sql_model_values = ",".join([
            self._sql_repr_value(getattr(self, field))
            for field in field_names
        ])

        c.execute(
            f'insert into {self.META.table_name} ({",".join(field_names)}) '
            f'values ({sql_model_values}) returning id;'
        )

        _id, = c.fetchone()
        self.id = _id
        return self

    def update(self, c: cursor):
        field_names = [field.name for field in fields(self)]
        field_names.pop(field_names.index('id'))

        sql_model_values = ",".join([
            f"{field} = {self._sql_repr_value(getattr(self, field))}"
            for field in field_names
        ])

        c.execute(
            f'update {self.META.table_name} set {sql_model_values} '
            f'where id = {self._sql_repr_value(self.id)}'
        )

    @classmethod
    def bulk_create(
            cls, c: cursor,
            models: list['AbstractModel'],
            exclude_id=False
    ):
        field_names = [field.name for field in fields(cls)]
        if exclude_id:
            field_names.pop(field_names.index('id'))

        sql_models_values: list[str] = []
        for model in models:
            sql_models_values.append(
                '(' + ",".join([
                    cls._sql_repr_value(getattr(model, field))
                    for field in field_names
                ]) + ')'
            )

        c.execute(
            f'insert into {cls.META.table_name} ({",".join(field_names)}) '
            f'values {",".join(sql_models_values)};'
        )
