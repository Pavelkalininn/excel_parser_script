import datetime
import logging
import sqlite3
from random import randrange

import pandas


def _connection_decorator(func):
    def _wrapper(*args, **kwargs):
        try:
            connection = sqlite3.connect('orders.db')
            cursor = connection.cursor()
            func(*args, cursor, **kwargs)
            cursor.close()
            connection.commit()

        except sqlite3.Error as error:
            logging.error(
                f"Ошибка при подключении к sqlite {error}",
                exc_info=True
            )
        finally:
            if connection:
                connection.close()

    return _wrapper


class Parser:
    def __init__(self, filename):
        self.filename = filename
        self.cleaned_data = pandas.read_excel(filename).dropna().values.tolist(
            )[2:]
        for company in self.cleaned_data:
            company.append(datetime.date(2020, 1, randrange(1, 7, 1)))
        print("Введенные данные: ", *self.cleaned_data, sep='\n')

    @staticmethod
    @_connection_decorator
    def create_table(cursor=None):
        if cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS companies
                (id INTEGER PRIMARY KEY,
                company TEXT,
                fact_qliq_data1 INTEGER,
                fact_qliq_data2 INTEGER,
                fact_qoil_data1 INTEGER,
                fact_qoil_data2 INTEGER,
                forecast_qliq_data1 INTEGER,
                forecast_qliq_data2 INTEGER,
                forecast_qoil_data1 INTEGER,
                forecast_qoil_data2 INTEGER,
                date DATE)
        ''')

    @_connection_decorator
    def data_insert(self, cursor=None):
        if cursor:
            cursor.executemany("""
                INSERT INTO companies
                    (id,
                    company,
                    fact_qliq_data1,
                    fact_qliq_data2,
                    fact_qoil_data1,
                    fact_qoil_data2,
                    forecast_qliq_data1,
                    forecast_qliq_data2,
                    forecast_qoil_data1,
                    forecast_qoil_data2,
                    date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id)
                DO NOTHING;""", self.cleaned_data)

    @staticmethod
    @_connection_decorator
    def data_get(cursor):
        if cursor:
            cursor.execute("""
            SELECT
                SUM(fact_qliq_data1)
                + SUM(fact_qliq_data2)
                + SUM(forecast_qliq_data1)
                + SUM(forecast_qliq_data2),
                SUM(fact_qoil_data1)
                + SUM(fact_qoil_data2)
                + SUM(forecast_qoil_data1)
                + SUM(forecast_qoil_data2),
                date
            FROM companies 
            GROUP BY
                date; 
            """, )
            data = cursor.fetchall()
            if data:
                for row in data:
                    print(
                        'Total qliq = {0}, total qoil = {1}, at {2}'.format(
                            *row
                        )
                    )


if __name__ == '__main__':
    parser = Parser('input.xlsx')
    parser.create_table()
    parser.data_insert()
    parser.data_get()
