"""Скрипт для заполнения данными таблиц в БД Postgres."""

import psycopg2
import csv


def fill_db_customers(row_cd):
    conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='Mentos19')
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    'INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)',
                    (row_cd['customer_id'], row_cd['company_name'], row_cd['contact_name'])
                )
    finally:
        conn.close()


def fill_db_employees(row_ed):
    conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='Mentos19')
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    'INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s, %s, %s, %s)',
                    (row_ed['employee_id'], row_ed['first_name'], row_ed['last_name'], row_ed['title'], row_ed['birth_date'], row_ed['notes'])
                )
    finally:
        conn.close()


def fill_db_orders(row_od):
    conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='Mentos19')
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    'INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city) VALUES (%s, %s, %s, %s, %s)',
                    (row_od['order_id'], row_od['customer_id'], row_od['employee_id'], row_od['order_date'], row_od['ship_city'])
                )
    finally:
        conn.close()


customers_db_lst = ['./north_data/customers_data.csv', 0]
employees_db_lst = ['./north_data/employees_data.csv', 1]
orders_db_lst = ['./north_data/orders_data.csv', 2]


def open_db(db_str):
    with open(db_str[0], newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        if db_str[1] == 0:
            for row in reader:
                fill_db_customers(row)
        elif db_str[1] == 1:
            for row in reader:
                fill_db_employees(row)
        else:
            for row in reader:
                fill_db_orders(row)


open_db(customers_db_lst)
open_db(employees_db_lst)
open_db(orders_db_lst)
