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
    type: t.Optional[str]

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
        if region is None:
            region = Region(id=None, name=region_name).create(c)

        area = Area.get_from_db(c, name=area_name)
        if area is None:
            area = Area(id=None, name=area_name).create(c)

        territory = Territory.get_from_db(c, name=territory_name)
        if territory is None:
            territory = Territory(
                id=None,
                name=territory_name,
                type=None
            ).create(c)

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
    place_id: int
    type: t.Optional[str]
    parent_body_name: t.Optional[str]

    META = ModelMeta(
        table_name='educational_institution'
    )

    @classmethod
    def get_or_create(
            cls,
            c: cursor,
            name: str,
            place_id: int,
            _type: t.Optional[str] = None,
            parent_body_name: t.Optional[str] = None
    ) -> 'EducationalInstitution':
        ei = cls.get_from_db(c, name=name, place_id=place_id)
        if ei is None:
            ei = EducationalInstitution(
                id=None,
                name=name,
                place_id=place_id,
                type=_type,
                parent_body_name=parent_body_name
            ).create(c)
            return ei
        else:
            if (
                    ei.type != _type or
                    ei.parent_body_name != ei.parent_body_name
            ):
                ei.update(c)
            return ei


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

    raw_score: float
    score: float
    school_score: int

    year: int
    status: str

    META = ModelMeta(
        table_name='exam'
    )
