import datetime
import re

from sqlalchemy import MetaData, Table, String, Integer, Column, Float, Date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import gspread

import os

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)
DATABASE_PATH = os.getenv('DATABASE_PATH')


def get_expenses_fact():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1HO5_shI_vLhxoTsTOkMWxRGnYh_f0OVrm7kdQuhAX9E/edit#gid'
                        '=1822148873')
    return sh.get_worksheet(1).get_all_values()


def get_beginner_courses():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1NrBTDouZXaJzTwCBKG82FnZDiB5ot_Qf0CheMIhpKY8/edit#gid'
                        '=1266915562')
    return sh.get_worksheet(1).get_all_values()


def get_day_open_doors():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1NrBTDouZXaJzTwCBKG82FnZDiB5ot_Qf0CheMIhpKY8/edit#gid'
                        '=1266915562')
    return sh.get_worksheet(2).get_all_values()


def get_start_lesson():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1NrBTDouZXaJzTwCBKG82FnZDiB5ot_Qf0CheMIhpKY8/edit#gid'
                        '=1266915562')
    return sh.get_worksheet(3).get_all_values()


def get_lm():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1NrBTDouZXaJzTwCBKG82FnZDiB5ot_Qf0CheMIhpKY8/edit#gid'
                        '=1266915562')
    return sh.get_worksheet(4).get_all_values()


def get_yandex_direct():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1DdlS0IrfttuNIKbq-VzOUI4UJXGGm2La5KFr39x6gDA/edit#gid'
                        '=2088754069')
    return sh.get_worksheet(0).get_all_values()


def get_vkontakte():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1vPd78ZIeJ9V_DYs4TL4PpG-W9eURP9pnN57TsydBKj4/edit#gid'
                        '=1823541945')
    return sh.get_worksheet(0).get_all_values()


def get_adwords():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1rrd8zH5SScQU7mW_8vXfjJvWSpLTf-eObjssdiRrGRQ/edit#gid'
                        '=1303477')
    return sh.get_worksheet(0).get_all_values()


def get_facebook():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1ifhe1_gMPbo7rA38iZfy47_7TqeQ_DQwugcZESe1rL4/edit#gid'
                        '=2051422061')
    return sh.get_worksheet(0).get_all_values()


def get_woocommerce_revenue():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1H31z1i9UBsyVRKZE0MfWGdtINbUCi8Y5i6dbBd6ML-M/'
                        'edit#gid=0')
    return sh.get_worksheet(0).get_all_values()


def get_revenue_corporate():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1DaD391ZCA4B8WaXH-yaIfh9fx6azxyly13luetob7SE/'
                        'edit#gid=0')
    return sh.get_worksheet(0).get_all_values()


def get_revenue_remote_learning():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1jSfi9Y6Be_JSqGK6JRWwhtcflo51L63qMuOLbZ0oGWk/'
                        'edit#gid=0')
    return sh.get_worksheet(0).get_all_values()


def get_expenses_fact_cards():
    gc = gspread.service_account(filename='Credentials.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1hAYsdE85m85fPdEygdf9TwHOJfqUTbp3P4sTjEam9vg/edit#gid'
                        '=642436016')
    return sh.get_worksheet(1).get_all_values()


def expenses_fact_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    expenses_fact = Table('expenses_fact', metadata,
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
    sql = text('DELETE FROM expenses_fact')
    engine.execute(sql)
    data = []
    i = 0
    for row in table:
        if i > 0:
            for j in range(len(row)):
                if not row[j]:
                    row[j] = None
                data.append({
                    "Date": datetime.datetime.strptime(row[0].replace('.', '-'), '%d-%m-%Y'),
                    "Location": row[1],
                    "level1_acc": row[2],
                    "level2_acc": row[3],
                    "Source_type": row[4],
                    "Source_name": row[5],
                    "Counterparty": row[6],
                    "Amount": float(row[7].replace(',', '.')),
                    "A1": row[8],
                    "A2": row[9],
                })
            if i >= 1000 and i % 1000 == 0:
                ins = expenses_fact.insert().values(data)
                conn.execute(ins)
                data = []
        i += 1
    if len(data) != 0:
        ins = expenses_fact.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def beginner_courses_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    beginner_courses = Table('bitrix24-Курсы для начинающих', metadata,
                             Column('Код', String()),
                             Column('Название сделки', String()),
                             Column('Сумма', Float()),
                             Column('Код статуса', String()),
                             Column('Статус', String()),
                             Column('Код направления', String()),
                             Column('Посетил', String()),
                             Column('Контакт номер', String()),
                             Column('Дата создания', Date()),
                             Column('Мероприятие 2', String()),
                             Column('Мероприятие 3', String()),
                             Column('Дата мероприятия', Date()),
                             Column('Время мероприятия', String()),
                             Column('Source', String()),
                             Column('Medium', String()),
                             Column('Campaign', String()),
                             Column('Content', String()),
                             Column('Term', String()),
                             Column('Причина отказа', String()),
                             )

    metadata.create_all(engine)
    sql = text('DELETE FROM "bitrix24-Курсы для начинающих"')
    engine.execute(sql)
    data = []
    i = 0
    for row in table:
        if i > 0:
            for j in range(len(row)):
                if not row[j]:
                    row[j] = None
            data.append({
                "Код": row[0],
                "Название сделки": row[1],
                "Сумма": row[2],
                "Код статуса": row[3],
                "Статус": row[4],
                "Код направления": row[5],
                "Посетил": row[6],
                "Контакт номер": row[7],
                "Дата создания": row[8],
                "Мероприятие 2": row[9],
                "Мероприятие 3": row[10],
                "Дата мероприятия": row[11],
                "Время мероприятия": row[12],
                "Source": row[13],
                "Medium": row[14],
                "Campaign": row[15],
                "Content": row[16],
                "Term": row[17],
                "Причина отказа": row[18]
            })

            if i >= 1000 and i % 1000 == 0:
                ins = beginner_courses.insert().values(data)
                conn.execute(ins)
                data = []
        i += 1
    if len(data) != 0:
        ins = beginner_courses.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def day_open_doors_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    day_open_doors = Table('bitrix24-ДОД', metadata,
                           Column('ID сделки', Integer()),
                           Column('Название сделки', String()),
                           Column('Код статуса', String()),
                           Column('Статус', String()),
                           Column('Контакт номер', String()),
                           Column('Source', String()),
                           Column('Medium', String()),
                           Column('Campaign', String()),
                           Column('Content', String()),
                           Column('Term', String()),
                           Column('Дата создания', Date()),
                           Column('Мероприятие 3', String()),
                           Column('Дата мероприятия', Date()),
                           )

    metadata.create_all(engine)
    sql = text('DELETE FROM "bitrix24-ДОД"')
    engine.execute(sql)
    i = 0
    data = []
    for row in table:
        if i > 0:
            for j in range(len(row)):
                if not row[j]:
                    row[j] = None
            if row[10]:
                if '-' not in row[10]:
                    row[10] = None
            if row[12]:
                if '.' not in row[12]:
                    row[12] = None
            data.append({
                "ID сделки": row[0],
                "Название сделки": row[1],
                "Код статуса": row[2],
                "Статус": row[3],
                "Контакт номер": row[4],
                "Source": row[5],
                "Medium": row[6],
                "Campaign": row[7],
                "Content": row[8],
                "Term": row[9],
                "Дата создания": row[10],
                "Мероприятие 3": row[11],
                "Дата мероприятия": row[12],
            })

            if i >= 1000 and i % 1000 == 0:
                print(i)
                ins = day_open_doors.insert().values(data)
                conn.execute(ins)
                data = []
        i += 1
    if len(data) != 0:
        ins = day_open_doors.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def start_lesson_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    start_lesson = Table('bitrix24-ПЗ', metadata,
                         Column('ID сделки', Integer()),
                         Column('Название сделки', String()),
                         Column('Телефон контакта', String()),
                         Column('ID контакта', String()),
                         Column('UTM_source', String()),
                         Column('UTM_medium', String()),
                         Column('UTM_campain', String()),
                         Column('UTM_content', String()),
                         Column('UTM_term', String()),
                         Column('Дата создания', Date()),
                         Column('Мероприятие 2', String()),
                         Column('Мероприятие 2 Код', String()),
                         Column('Дата мероприятия', Date()),
                         Column('Причина отказа', String()),
                         Column('Код статуса', String()),
                         Column('Статус', String()),
                         Column('Ответственный', String()),
                         )

    metadata.create_all(engine)
    sql = text('DELETE FROM "bitrix24-ПЗ"')
    engine.execute(sql)
    i = 0
    data = []
    for row in table:
        if i > 0:
            for j in range(len(row)):
                if not row[j]:
                    row[j] = None
            data.append({
                "ID сделки": row[0],
                "Название сделки": row[1],
                "Телефон контакта": row[2],
                "ID контакта": row[3],
                "UTM_source": row[4],
                "UTM_medium": row[5],
                "UTM_campain": row[6],
                "UTM_content": row[7],
                "UTM_term": row[8],
                "Дата создания": row[9],
                "Мероприятие 2": row[10],
                "Мероприятие 2 Код": row[11],
                "Дата мероприятия": row[12],
                "Причина отказа": row[13],
                "Код статуса": row[14],
                "Статус": row[15],
                "Ответственный": row[16],
            })

            if i >= 1000 and i % 1000 == 0:
                ins = start_lesson.insert().values(data)
                conn.execute(ins)
                data = []
        i += 1
    if len(data) != 0:
        ins = start_lesson.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def lm_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()
    lm = Table('bitrix24-ЛМ', metadata,
               Column('ID сделки', Integer()),
               Column('Название сделки', String()),
               Column('Код статуса', String()),
               Column('Статус', String()),
               Column('Телефон контакта', String()),
               Column('Контакт номер', String()),
               Column('Source', String()),
               Column('Medium', String()),
               Column('Campaign', String()),
               Column('Content', String()),
               Column('Term', String()),
               Column('Дата создания', Date()),
               Column('Причина отказа', String()),
               Column('Направление', String()),
               Column('Тип контекст РК', String()),
               Column('Тип FB', String()),
               )

    metadata.create_all(engine)
    sql = text('DELETE FROM "bitrix24-ЛМ"')
    engine.execute(sql)
    i = 0
    data = []
    for row in table:
        if i > 0:
            for j in range(len(row)):
                if not row[j]:
                    row[j] = None
            data.append({
                "ID сделки": row[0],
                "Название сделки": row[1],
                "Код статуса": row[2],
                "Статус": row[3],
                "Телефон контакта": row[4],
                "Контакт номер": row[5],
                "Source": row[6],
                "Medium": row[7],
                "Campaign": row[8],
                "Content": row[9],
                "Term": row[10],
                "Дата создания": row[11],
                "Причина отказа": row[12],
                "Направление": row[13],
                "Тип контекст РК": row[14],
                "Тип FB": row[15],
            })

        if i >= 1000 and i % 1000 == 0:
            ins = lm.insert().values(data)
            conn.execute(ins)
            data = []
        i += 1
    if len(data) != 0:
        ins = lm.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def yandex_direct_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()
    yandex_direct = Table('yandex_direct', metadata,
                          Column('Дата', Date()),
                          Column('AdID', String()),
                          Column('AdGroupID', String()),
                          Column('AdGroupName', String()),
                          Column('CampaignId', String()),
                          Column('CampaignName', String()),
                          Column('Cost', Float()),
                          Column('Impressions', Float()),
                          Column('Clicks', Float()),
                          Column('Ctr', Float()),
                          Column('AvgCpc', String()),
                          Column('Convertion', String()),
                          )

    metadata.create_all(engine)
    sql = text('DELETE FROM yandex_direct')
    engine.execute(sql)
    data = []
    i = 0
    for row in table:
        if i > 0:
            for j in range(len(row)):
                if not row[j]:
                    row[j] = None

            data.append({
                "Дата": row[0],
                "AdID": row[1],
                "AdGroupID": row[2],
                "AdGroupName": row[3],
                "CampaignId": row[4],
                "CampaignName": row[5],
                "Cost": row[6],
                "Impressions": row[7],
                "Clicks": row[8],
                "Ctr": row[9],
                "AvgCpc": row[10],
                "Convertion": row[11],
            })
        if i >= 1000 and i % 1000 == 0:
            ins = yandex_direct.insert().values(data)
            conn.execute(ins)
            data = []
        i += 1
    if len(data) != 0:
        ins = yandex_direct.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def vkontakte_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    vkontakte = Table('VKontakte-Расходы_вчера', metadata,
                      Column('Date', Date()),
                      Column('AdId', String()),
                      Column('AdName', String()),
                      Column('CampaignId', String()),
                      Column('CampaignName', String()),
                      Column('Cost', Float()),
                      Column('Impressions', Float()),
                      Column('Clicks', Float()),
                      Column('Reach', Float()),
                      )

    metadata.create_all(engine)
    sql = text('DELETE FROM "VKontakte-Расходы_вчера"')
    engine.execute(sql)
    data = []
    i = 0
    for row in table:
        if i > 0:
            for j in range(len(row)):
                if not row[j]:
                    row[j] = None
                    print(1)
            data.append({
                "Date": row[0],
                "AdId": row[1],
                "AdName": row[2],
                "CampaignId": row[4],
                "CampaignName": row[4],
                "Cost": row[5],
                "Impressions": row[6],
                "Clicks": row[7],
                "Reach": row[8],
            })
            if i >= 1000 and i % 1000 == 0:
                ins = vkontakte.insert().values(data)
                conn.execute(ins)
                data = []
        i += 1
    if len(data) != 0:
        ins = vkontakte.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def adwords_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    adwords = Table('Adwords-Расходы_Вчера', metadata,
                    Column('Date', Date()),
                    Column('campaignID', String()),
                    Column('campaignName', String()),
                    Column('allConv', String()),
                    Column('conversions', Float()),
                    Column('network', String()),
                    Column('adID', String()),
                    Column('adGroupID', String()),
                    Column('adGroup', String()),
                    Column('averageCpc', String()),
                    Column('clicks', Float()),
                    Column('cost', Float()),
                    Column('impressions', Float()),
                    Column('utm_medium', String()),
                    Column('utm_source', String()),
                    Column('utm_campaign', String()),
                    )

    metadata.create_all(engine)
    sql = text('DELETE FROM "Adwords-Расходы_Вчера"')
    engine.execute(sql)
    data = []
    i = 0
    for row in table:
        if i > 0:
            for j in range(len(row)):
                if not row[j]:
                    row[j] = None

            data.append({
                "Date": row[0],
                "campaignID": row[1],
                "campaignName": row[2],
                "allConv": row[3],
                "conversions": row[4],
                "network": row[5],
                "adID": row[6],
                "adGroupID": row[7],
                "adGroup": row[8],
                "averageCpc": row[9],
                "clicks": row[10],
                "cost": row[11],
                'impressions': row[12],
                'utm_medium': row[13],
                'utm_source': row[14],
                'utm_campaign': row[15],
            })
        if i >= 1000 and i % 1000 == 0:
            ins = adwords.insert().values(data)
            conn.execute(ins)
            data = []
        i += 1
    if len(data) != 0:
        ins = adwords.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def facebook_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    facebook = Table('facebook-Расходы_Вчера', metadata,
                     Column('Дата', Date()),
                     Column('ID объявления', String()),
                     Column('Название объявления', String()),
                     Column('ID группы', String()),
                     Column('Название группы', String()),
                     Column('ID кампании', String()),
                     Column('Название кампании', String()),
                     Column('Показы', Float()),
                     Column('Клики', Float()),
                     Column('Уникальные клики', Float()),
                     Column('Стоимость за клик', Float()),
                     Column('% показов', Float()),
                     Column('Охват', Float()),
                     Column('Расход', Float()),
                     Column('Просмотры целевой стр.', Float()),
                     Column('Лиды', Float()),
                     Column('Цена за результат клики', Float()),
                     Column('Пробное занятие', Float()),
                     )

    metadata.create_all(engine)
    sql = text('DELETE FROM "facebook-Расходы_Вчера"')
    engine.execute(sql)
    data = []
    i = 0
    for row in table:
        if i > 0:
            for j in range(len(row)):
                if not row[j]:
                    row[j] = None

            data.append({
                "Дата": row[0],
                "ID объявления": row[1],
                "Название объявления": row[2],
                "ID группы": row[3],
                "Название группы": row[4],
                "ID кампании": row[5],
                "Название кампании": row[6],
                "Показы": row[7],
                "Клики": row[8],
                "Уникальные клики": row[9],
                "Стоимость за клик": row[10],
                "% показов": row[11],
                'Охват': row[12],
                'Расход': row[13],
                'Просмотры целевой стр.': row[14],
                'Лиды': row[15],
                'Цена за результат клики': row[16],
                'Пробное занятие': row[17],
            })
        if i >= 1000 and i % 1000 == 0:
            ins = facebook.insert().values(data)
            conn.execute(ins)
            data = []
        i += 1
    if len(data) != 0:
        ins = facebook.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def revenue_corporate_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    revenue_corporate = Table('revenue_corporate', metadata,
                              Column('Date', Date()),
                              Column('Location', String()),
                              Column('Administrator', String()),
                              Column('Level1_acc', String()),
                              Column('Level2_acc', String()),
                              Column('Item_num', String()),
                              Column('Client_name', String()),
                              Column('Acc_type', String()),
                              Column('Amount', Float()),
                              Column('A1', String()),
                              Column('A2', String()),
                              )

    metadata.create_all(engine)
    sql = text('DELETE FROM revenue_corporate')
    engine.execute(sql)
    data = []
    i = 0
    for row in table:
        if i > 0:
            for j in range(len(row)):
                if not row[j]:
                    row[j] = None

            data.append({
                "Date": row[0],
                "Location": row[1],
                "Administrator": row[2],
                "Level1_acc": row[3],
                "Level2_acc": row[4],
                "Item_num": row[5],
                "Client_name": row[6],
                "Acc_type": row[7],
                "Amount": float(re.sub(r"(\xa0)", "", row[8].replace(',', '.'))),
                "A1": row[9],
                "A2": row[10],
            })
        if i >= 1000 and i % 1000 == 0:
            ins = revenue_corporate.insert().values(data)
            conn.execute(ins)
            data = []
        i += 1
    if len(data) != 0:
        ins = revenue_corporate.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def revenue_remote_learning_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    revenue_remote_learning = Table('revenue_remote_learning', metadata,
                                    Column('Date', Date()),
                                    Column('Location', String()),
                                    Column('Administrator', String()),
                                    Column('Level1_acc', String()),
                                    Column('Level2_acc', String()),
                                    Column('Item_num', String()),
                                    Column('Client_name', String()),
                                    Column('Acc_type', String()),
                                    Column('Amount', Float()),
                                    Column('A1', String()),
                                    Column('A2', String()),
                                    )

    metadata.create_all(engine)
    sql = text('DELETE FROM revenue_remote_learning')
    engine.execute(sql)
    data = []
    i = 0
    for row in table:
        if i > 0:
            for j in range(len(row)):
                if not row[j]:
                    row[j] = None

            data.append({
                "Date": row[0],
                "Location": row[1],
                "Administrator": row[2],
                "Level1_acc": row[3],
                "Level2_acc": row[4],
                "Item_num": row[5],
                "Client_name": row[6],
                "Acc_type": row[7],
                "Amount": row[8],
                "A1": row[9],
                "A2": row[10],
            })
        if i >= 1000 and i % 1000 == 0:
            ins = revenue_remote_learning.insert().values(data)
            conn.execute(ins)
            data = []
        i += 1
    if len(data) != 0:
        ins = revenue_remote_learning.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def expenses_fact_cards_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    expenses_fact_cards = Table('Статьи ОПиУ-Expenses_fact_cards', metadata,
                                Column('Date', Date()),
                                Column('Location', String()),
                                Column('Level1_acc', String()),
                                Column('Level2_acc', String()),
                                Column('Source_type', String()),
                                Column('Source_name', String()),
                                Column('Counterparty', String()),
                                Column('Amount', Float()),
                                Column('A1', String()),
                                Column('A2', String()),
                                )

    metadata.create_all(engine)
    sql = text('DELETE FROM "Статьи ОПиУ-Expenses_fact_cards"')
    engine.execute(sql)
    data = []
    i = 0
    for row in table:
        if i > 0:
            for j in range(len(row)):
                if not row[j]:
                    row[j] = None
            if row[0] is not None:
                date_ = str(row[0]).replace('.', '-')
                try:
                    row[0] = datetime.datetime.strptime(date_.split(' ')[0], '%d-%m-%Y')
                except ValueError:
                    row[0] = datetime.datetime.strptime(date_.split(' ')[0], '%Y-%m-%d')

            data.append({
                "Date": row[0],
                "Location": row[1],
                "Level1_acc": row[2],
                "Level2_acc": row[3],
                "Source_type": row[4],
                "Source_name": row[5],
                "Counterparty": row[6],
                "Amount": row[7],
                "A1": row[8],
                "A2": row[9],
            })
        if i >= 1000 and i % 1000 == 0:
            ins = expenses_fact_cards.insert().values(data)
            conn.execute(ins)
            data = []
        i += 1
    if len(data) != 0:
        ins = expenses_fact_cards.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def woocommerce_revenue_migration(table):
    engine = create_engine(DATABASE_PATH)
    conn = engine.connect()
    metadata = MetaData()

    woocommerce_revenue = Table('WooCommerce export for revenue 2022-Revenue_woocommerce', metadata,
                                Column('Date', Date()),
                                Column('Location', String()),
                                Column('Administrator', String()),
                                Column('Level1_acc', String()),
                                Column('Level2_acc', String()),
                                Column('Item_num', String()),
                                Column('Client_name', String()),
                                Column('Acc_type', String()),
                                Column('Amount', Float()),
                                Column('A1', String()),
                                )

    metadata.create_all(engine)
    sql = text('DELETE FROM "WooCommerce export for revenue 2022-Revenue_woocommerce"')
    engine.execute(sql)
    data = []
    i = 0
    for row in table:
        for j in range(len(row)):
            if not row[j]:
                row[j] = None

        data.append({
            "Date": row[0],
            "Location": row[1],
            "Administrator": row[2],
            "Level1_acc": row[3],
            "Level2_acc": row[4],
            "Item_num": row[5],
            "Client_name": row[6],
            "Acc_type": row[7],
            "Amount": row[8],
            "A1": row[9],
        })
    if i >= 1000 and i % 1000 == 0:
        ins = woocommerce_revenue.insert().values(data)
        conn.execute(ins)
        data = []
    i += 1
    if len(data) != 0:
        ins = woocommerce_revenue.insert().values(data)
        conn.execute(ins)
    conn.close()
    engine.dispose()


def main():
    time_ = datetime.datetime.now()
    lm_migration(get_lm())
    vkontakte_migration(get_vkontakte())
    beginner_courses_migration(get_beginner_courses())
    day_open_doors_migration(get_day_open_doors())
    expenses_fact_migration(get_expenses_fact())
    adwords_migration(get_adwords())
    expenses_fact_cards_migration(get_expenses_fact_cards())
    facebook_migration(get_facebook())
    revenue_corporate_migration(get_revenue_corporate())
    revenue_remote_learning_migration(get_revenue_remote_learning())
    start_lesson_migration(get_start_lesson())
    yandex_direct_migration(get_yandex_direct())
    woocommerce_revenue_migration(get_woocommerce_revenue())
    print(datetime.datetime.now() - time_)


if __name__ == '__main__':
    main()

# todo: Уровнить таблицу yandex direct
# todo: Уронить Revenue_corporate-Revenue_corporate
# todo: Уронить Revenue_remote_learning-Revenue_remote_learning
