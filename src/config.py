from os import environ
from dataclasses import dataclass

__all__ = [
    'DB_CONFIG',
    'DATA_DIRECTORY',
    'FILES_TO_PARSE',
    'BULK_UPLOAD_SIZE'
]

BULK_UPLOAD_SIZE = 5000

DB_NAME = environ.get('DB_NAME')
DB_USER = environ.get('DB_USER')
DB_PASSWORD = environ.get('DB_PASSWORD')
DB_HOST = environ.get('DB_HOST')
DB_PORT = '5432'
DB_CONFIG = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST,
    'port': DB_PORT
}
DB_CONNECTION_STING = f'postgres://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'

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
