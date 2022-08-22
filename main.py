import datetime

from sqlalchemy import MetaData, Table, String, Integer, Column, Float, Date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import gspread

import os

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)
DATABASE_PATH = os.getenv('DATABASE_PATH')


def parse_google_sheets():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1HO5_shI_vLhxoTsTOkMWxRGnYh_f0OVrm7kdQuhAX9E/edit#gid'
                        '=1822148873')
    return sh.get_worksheet(1).get_all_values()


def insert_table_in_db(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    expenses_fact = Table('expenses_fact', metadata,
                          Column('id', Integer(), primary_key=True),
                          Column('Date', Date()),
                          Column('Location', String()),
                          Column('level1_acc', String()),
                          Column('level2_acc', String()),
                          Column('Source_type', String()),
                          Column('Source_name', String()),
                          Column('Counterparty', String()),
                          Column('A1', String()),
                          Column('A2', String()),
                          Column('Amount', Float()),
                          )

    metadata.create_all(engine)
    i = 0
    for row in table:
        if i > 0:
            ins = expenses_fact.insert().values(Date=datetime.datetime.strptime(row[0].replace('.', '-'), '%d-%m-%Y'),
                                                Location=row[1],
                                                level1_acc=row[2], level2_acc=row[3],
                                                Source_type=row[4], Source_name=row[5], Counterparty=row[6],
                                                A1=row[8], A2=row[9], Amount=float(row[7].replace(',', '.')))
            conn.execute(ins)
        i += 1


def main():
    insert_table_in_db(parse_google_sheets())


if __name__ == '__main__':
    main()
