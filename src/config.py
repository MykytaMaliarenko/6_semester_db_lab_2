from os import environ
from dataclasses import dataclass

__all__ = [
    'DB_CONFIG',
    'DATA_DIRECTORY',
    'FILES_TO_PARSE',
    'BULK_UPLOAD_SIZE'
]

BULK_UPLOAD_SIZE = 5000

DB_CONFIG = {
    'dbname': environ.get('DB_NAME'),
    'user': environ.get('DB_USER'),
    'password': environ.get('DB_PASSWORD'),
    'host': environ.get('DB_HOST'),
    'port': environ.get('DB_PORT', '5432')
}

DATA_DIRECTORY = '../zno_data/'


@dataclass
class FileToParse:
    path: str
    year: int
    encoding: str


FILES_TO_PARSE = [
    FileToParse(
        path=DATA_DIRECTORY + 'Odata2021File.csv',
        year=2021,
        encoding='utf-8-sig'
    ),
    FileToParse(
        path=DATA_DIRECTORY + 'Odata2019File.csv',
        year=2019,
        encoding='cp1251'
    )
]
