import typing as t
from dataclasses import dataclass
from orm import AbstractModel, ModelMeta
from psycopg2.extensions import cursor


@dataclass
class Region(AbstractModel):
    name: str

    META = ModelMeta(
        table_name='region'
    )


@dataclass
class Area(AbstractModel):
    name: str

    META = ModelMeta(
        table_name='area'
    )


@dataclass
class Territory(AbstractModel):
    name: str
    type: str

    META = ModelMeta(
        table_name='territory'
    )


@dataclass
class Place(AbstractModel):
    region_id: int
    area_id: int
    territory_id: int

    META = ModelMeta(
        table_name='place'
    )

    @classmethod
    def get_or_create(
        cls,
        c: cursor,
        region_name: str,
        area_name: str,
        territory_name: str
    ) -> 'Place':
        region = Region.get_from_db(c, name=region_name)
        area = Area.get_from_db(c, name=area_name)
        territory = Territory.get_from_db(c, name=territory_name)

        assert region is not None, \
            f'Region with name "{region_name}" doesnt exists'
        assert area is not None, \
            f'Area with name "{area_name}" doesnt exists'
        assert territory is not None, \
            f'Territory with name "{territory_name}" doesnt exists'

        place_data = dict(
            region_id=region.id,
            area_id=area.id,
            territory_id=territory.id
        )

        place = cls.get_from_db(c, **place_data)
        if place is None:
            place = Place(id=None, **place_data)
            place.create(c)
            return place
        else:
            return place


@dataclass
class EducationalInstitution(AbstractModel):
    name: str
    type: str
    place_id: int
    parent_body_name: t.Optional[str]

    META = ModelMeta(
        table_name='educational_institution'
    )


@dataclass
class Subject(AbstractModel):
    code: str
    name: str

    META = ModelMeta(
        table_name='subject'
    )


@dataclass
class Student(AbstractModel):
    gender: str
    birth_year: int
    studying_lang: str

    place_of_living_id: int
    educational_institution_id: int

    class_profile: str

    META = ModelMeta(
        table_name='student'
    )


@dataclass
class Exam(AbstractModel):
    subject_id: int
    student_id: str
    educational_institution_id: int

    dpa_level: t.Optional[str]
    adapt_scale: int

    raw_score: float
    score: float
    school_score: int
