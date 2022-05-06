import csv
import logging
import time
import psycopg2
import config as conf
from functools import cache
from models import Place, EducationalInstitution, Subject, Student, Exam
from yoyo import read_migrations, get_backend
from sql_utils import (
    extract_student, extract_exams
)
from consts import *


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('app.log', mode='w'),
        logging.StreamHandler()
    ]
)


def prettify_csv_line(raw_line: dict) -> dict:
    return {
        key.lower(): value if value != 'null' else None
        for key, value in raw_line.items()
    }


def parse_file(conn, cursor, file, year: int):
    @cache
    def get_subject_id(subject_code: str) -> int:
        return Subject.get_from_db(cursor, code=subject_code).id

    @cache
    def get_or_create_place(
            region_name: str,
            area_name: str,
            territory_name: str
    ) -> int:
        return Place.get_or_create(
            cursor,
            region_name,
            area_name,
            territory_name
        ).id

    @cache
    def get_or_create_educational_institution(
            name: str,
            region_name: str,
            area_name: str,
            territory_name: str,
            _type: t.Optional[str] = None,
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

        return EducationalInstitution.get_or_create(
            cursor,
            name=name,
            place_id=place.id,
            _type=_type,
            parent_body_name=parent_body_name
        ).id

    def _do_insert():
        Student.bulk_create(cursor, students)
        logging.info(f'successfully handled {len(students)} students')
        Exam.bulk_create(cursor, exams, exclude_id=True)
        logging.info(f'successfully handled {len(exams)} exams')
        conn.commit()

    students = list()
    exams = list()
    for index, line in enumerate(csv.DictReader(file, delimiter=';')):
        line.update({'year': str(year)})
        line = prettify_csv_line(line)

        students.append(extract_student(
            line,
            get_or_create_place,
            get_or_create_educational_institution
        ))

        exams += extract_exams(
            line,
            year,
            get_or_create_educational_institution,
            get_subject_id
        )

        if (index + 1) % conf.BULK_UPLOAD_SIZE == 0:
            _do_insert()
            students = []
            exams = []

    if len(students) | len(exams):
        students = []
        exams = []


def query_data(cursor, file_to_write: str):
    with open('query.sql', mode='r') as file:
        cursor.execute(file.read())

    with open(file_to_write, mode='w') as file:
        write = csv.writer(file)
        write.writerow(['Region', 'Year', 'Max Score'])

        for data in cursor.fetchall():
            write.writerow(data)


def main():
    backend = get_backend(conf.DB_CONNECTION_STING)
    migrations = read_migrations('./migrations')

    logging.info('starting migrations...')
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migrations))
    logging.info('migrations are successful')

    conn = psycopg2.connect(**conf.DB_CONFIG)
    cursor = conn.cursor()

    for file_info in conf.FILES_TO_PARSE:
        start_time = time.time()
        logging.info(f'start handling file: {file_info.path}')
        with open(
            file_info.path,
            mode='r',
            encoding=file_info.encoding
        ) as infile:
            parse_file(conn, cursor, infile, file_info.year)
            conn.commit()

        logging.info(
            f'finished handling file: {file_info.path}; '
            f'took time: {round(time.time() - start_time)} seconds'
        )

    query_data(cursor, 'query_result.csv')
    logging.info('successfully queried data')

    cursor.close()
    conn.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.exception(str(e))
        raise e
