import typing as t
import logging
from yoyo import step
from models import Place, EducationalInstitution, Student
from functools import cache
from psycopg2.extensions import connection

__depends__ = ['003.create-educational-institution']


BULK_UPLOAD_SIZE = 5000


def transfer_student_data(conn: connection):
    cursor = conn.cursor()

    @cache
    def get_or_create_place(
        region_name: str,
        area_name: str,
        territory_name: str
    ) -> int:
        nonlocal cursor
        return Place.get_or_create(
            cursor,
            region_name,
            area_name,
            territory_name
        ).id

    @cache
    def get_or_create_educational_institution(
        name: str,
        _type: str,
        region_name: str,
        area_name: str,
        territory_name: str,
        parent_body_name: t.Optional[str] = None
    ) -> t.Optional[int]:
        if name is None:
            return None

        if ('. ' + territory_name) in area_name:
            area_name = area_name[:area_name.index('. ' + territory_name)]
            area_name = area_name.strip()

        place = Place.get_or_create(
            cursor,
            region_name,
            area_name,
            territory_name
        )

        return EducationalInstitution(
            id=None,
            name=name,
            type=_type,
            place_id=place.id,
            parent_body_name=parent_body_name
        ).create(cursor).id

    logging.info('start')

    index = 0
    while True:
        cursor.execute(
            f'select outid, sextypename, birth, classprofilename, '
            f'classlangname, regname, areaname, tername, eoname, '
            f'eotypename, eoparent, eoregname, eoareaname, eotername '
            f'from zno_data order by outid '
            f'offset {index * BULK_UPLOAD_SIZE} limit {BULK_UPLOAD_SIZE}; '
        )

        logging.info(f'start handling offset {index * BULK_UPLOAD_SIZE}')

        try:
            data: list[Student] = []
            for entry in cursor.fetchall():
                (
                    outid, sextypename, birth,
                    classprofilename, classlangname,
                    regname, areaname, tername,
                    eoname, eotypename, eoparent,
                    eoregname, eoareaname, eotername
                ) = entry

                place_id = get_or_create_place(regname, areaname, tername)
                educational_institution_id = get_or_create_educational_institution(
                    eoname, eotypename, eoregname,
                    eoareaname, eotername, eoparent
                )

                data.append(Student(
                    id=outid,
                    gender=sextypename,
                    birth_year=birth,
                    studying_lang=classlangname,
                    place_of_living_id=place_id,
                    educational_institution_id=educational_institution_id,
                    class_profile=classprofilename
                ))

            Student.bulk_create(cursor, data)
            index += 1

            if len(data) < BULK_UPLOAD_SIZE:
                return
        except Exception as e:
            logging.error(f'error handling record outid={outid}')
            raise e


steps = [
    step(
        "CREATE TYPE gender AS ENUM ('чоловіча', 'жіноча');",
        "DROP TYPE gender;"
    ),
    step(
        """
        CREATE TABLE student (
            id varchar(50) primary key not null, 
            gender gender not null,
            birth_year integer not null, 
            studying_lang varchar(50),
            place_of_living_id integer not null,
            educational_institution_id integer,
            class_profile varchar(50),
            CONSTRAINT fk_place_of_living_id
              FOREIGN KEY(place_of_living_id) 
              REFERENCES place(id)
              ON DELETE RESTRICT,
            CONSTRAINT fk_educational_institution_id
              FOREIGN KEY(educational_institution_id) 
              REFERENCES educational_institution(id)
              ON DELETE RESTRICT
        );
        """,
        "DROP TABLE student",
    ),
    step(transfer_student_data)
]
