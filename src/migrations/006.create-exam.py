import itertools
import logging
from functools import cache

from yoyo import step
from consts import SubjectCodes
from models import Subject, Place, EducationalInstitution, Exam
from psycopg2.extensions import connection

__depends__ = ['005.create-subject']

BULK_UPLOAD_SIZE = 5000


def transfer_exam_data(conn: connection):
    def generate_subject_specific_fields_to_select(s: SubjectCodes) -> list[str]:
        return list(map(
            lambda f: f'{s.value}{f}',
            [
                'ptname', 'ptregname', 'ptareaname',
                'pttername', 'ball', 'ball100', 'ball12',
                'teststatus'
            ]
        ))

    subject_handling_order = tuple(subject for subject in SubjectCodes)

    fields_to_select = ['outid', 'year'] + list(itertools.chain(*[
        generate_subject_specific_fields_to_select(subject)
        for subject in subject_handling_order
    ]))

    cursor = conn.cursor()

    @cache
    def get_subject_id(subject_code: str) -> int:
        return Subject.get_from_db(cursor, code=subject_code).id

    @cache
    def get_or_create_educational_institution(
            name: str,
            region_name: str,
            area_name: str,
            territory_name: str,
    ) -> int:
        place = Place.get_or_create(
            cursor,
            region_name,
            area_name,
            territory_name
        )

        return EducationalInstitution.get_or_create(
            cursor,
            name=name,
            place_id=place.id
        ).id

    cursor.execute('select  count(*) from zno_data;')
    total_count, = cursor.fetchone()

    index = 0
    while index * BULK_UPLOAD_SIZE <= total_count:
        cursor.execute(
            f'select {",".join(fields_to_select)} from zno_data '
            f'limit {BULK_UPLOAD_SIZE} '
            f'offset {BULK_UPLOAD_SIZE * index}'
        )
        logging.info(
            f'handling {BULK_UPLOAD_SIZE * index} - '
            f'{BULK_UPLOAD_SIZE * (index + 1)} out of {total_count}'
        )

        exams = list()
        for entry in cursor.fetchall():
            student_id = entry[0]
            year = entry[1]

            for j, subject in enumerate(subject_handling_order):
                (
                    pt_name, pt_reg_name, pt_area_name,
                    pt_ter_name, ball, ball100, ball12,
                    test_status
                ) = entry[(j * 8) + 2: (j * 8) + 10]
                if pt_name is None or ball is None:
                    continue

                ei_id = get_or_create_educational_institution(
                    name=pt_name, region_name=pt_reg_name,
                    area_name=pt_area_name, territory_name=pt_ter_name
                )
                exams.append(Exam(
                    id=None,
                    subject_id=get_subject_id(subject.value),
                    student_id=student_id,
                    educational_institution_id=ei_id,
                    raw_score=ball,
                    score=ball100,
                    school_score=ball12,
                    year=year,
                    status=test_status
                ))

        Exam.bulk_create(cursor, exams, exclude_id=True)
        index += 1

    cursor.close()


steps = [
    step(
        """
        CREATE TABLE exam (
            id serial primary key, 
            subject_id int not null,
            student_id varchar(50) not null,
            educational_institution_id int,
            adapt_scale integer default 0,
            raw_score numeric(4, 1) not null,
            score numeric(4, 1) not null,
            school_score int,
            status varchar(150),
            year int
        );
        """,
        "DROP TABLE exam",
    ),
    step(transfer_exam_data)
]
