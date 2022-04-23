import csv
import logging
import time
import psycopg2
import config as conf
from sql_utils import (
    generate_bulk_insert_row_sql, generate_data_schema_from_entry,
    generate_create_table_sql
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


def parse_file(cursor, file, year: int):
    def _do_insert():
        cursor.execute(generate_bulk_insert_row_sql(bulk))
        logging.info(f'successfully handled {len(bulk)} entries')

    bulk = list()
    for index, line in enumerate(csv.DictReader(file, delimiter=';')):
        line.update({'year': str(year)})
        bulk.append(line)

        if (index + 1) % conf.BULK_UPLOAD_SIZE == 0:
            _do_insert()
            bulk = []

    if len(bulk):
        _do_insert()


def query_data(cursor, file_to_write: str):
    with open('query.sql', mode='r') as file:
        cursor.execute(file.read())

    with open(file_to_write, mode='w') as file:
        write = csv.writer(file)
        write.writerow(['Region', '2021 max score', '2019 max score'])

        for data in cursor.fetchall():
            write.writerow(data)


def main():
    file_to_generate_schema = conf.FILES_TO_PARSE[0]
    with open(
        file_to_generate_schema.path,
        mode='r',
        encoding=file_to_generate_schema.encoding
    ) as infile:
        line = next(csv.DictReader(infile, delimiter=';'))
        fields_schema: t.Dict[str, t.Type] = {
            **generate_data_schema_from_entry(line),
            **SPECIAL_FIELDS_SCHEMA
        }

    logging.info(f'successfully generated fields_schema')

    conn = psycopg2.connect(**conf.DB_CONFIG)
    cursor = conn.cursor()

    fields_schema.update(EXTRA_FIELDS)
    cursor.execute(generate_create_table_sql(fields_schema))
    conn.commit()

    for file_info in conf.FILES_TO_PARSE:
        start_time = time.time()
        logging.info(f'start handling file: {file_info.path}')
        with open(
            file_info.path,
            mode='r',
            encoding=file_info.encoding
        ) as infile:
            parse_file(cursor, infile, file_info.year)
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
