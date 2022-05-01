from yoyo import step
from consts import SubjectCodes
from models import Subject
from psycopg2.extensions import connection

__depends__ = ['004.create-student']


BULK_UPLOAD_SIZE = 5000


def fill_subjects(conn: connection):
    cursor = conn.cursor()

    values: list[Subject] = [
        Subject(id=None, code=subject.value, name=subject.name)
        for subject in SubjectCodes
    ]
    Subject.bulk_create(cursor, values, exclude_id=True)
    cursor.close()


steps = [
    step(
        """
        CREATE TABLE subject (
            id serial primary key, 
            name varchar(250) not null,
            code varchar(100) not null
        );
        """,
        "DROP TABLE subject",
    ),
    step(fill_subjects)
]
