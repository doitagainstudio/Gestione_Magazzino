#####################################################################
#                     GESTIONE MAGAZZINO 1.0                        #
#           a Python program by Cristiano Esposito ¢2021            #
#                                                                   #
#     Gestione del carico e scarico merce in un magazzino con       #
#         valorizzazione del prezzo medio della giacenza            #
#         secondo metodologia FIFO (First In First Out)             #
#                                                                   #
#####################################################################

import datetime
import sqlite3
from sqlite3 import Error

import pandas as pd


# Funzione per connettersi al database
def connect_db(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES |
                                                     sqlite3.PARSE_COLNAMES)
        return conn
    except Error as e:
        print(e)

    return conn


# Funzione per creare tabelle nel database
def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


# Funzione per effettuare query di tipo SELECT sul database
def select_query(conn, select_table_sql):
    try:
        c = conn.cursor()
        c.row_factory = sqlite3.Row
        cursor = c.execute(select_table_sql)
        return cursor
    except Error as e:
        print(e)


# Funzione per effettuare query di tipo INSERT sul database
def insert_query(conn, insert_table_sql, data):
    try:
        today = datetime.datetime.now()
        c = conn.cursor()
        c.execute(insert_table_sql, data)
        conn.commit()
    except Error as e:
        print(e)


# Funzione per effettuare carico merce in magazzino e ricalcolo del valore medio giacenza
def carico(conn):
    global ultima_giacenza, old_prezzo_medio, ultimo_carico, ultimo_valore_carico
    ultima_giacenza = 0
    old_prezzo_medio = 0
    ultimo_carico = 0
    ultimo_valore_carico = 0
    now = datetime.datetime.now().replace(microsecond=0)
    carico = float(input('Inserire q.tà carico: \n'))
    val_carico = float(input('Inserire valore carico unitario: \n'))
    sql_select_ultimocarico = """ SELECT * FROM movimenti WHERE entrata IS NOT NULL ORDER BY id DESC LIMIT 1; """
    sql_select_giacenza = """ SELECT giacenza FROM movimenti ORDER BY id DESC LIMIT 1; """
    if conn is not None:
        cursor = select_query(conn, sql_select_giacenza)
        for row in cursor:
            ultima_giacenza = row['giacenza']

        cursor = select_query(conn, sql_select_ultimocarico)
        for row in cursor:
            ultimo_carico = row['entrata']
            ultimo_valore_carico = row['prezzo_carico']

        nuova_giacenza = (carico + ultima_giacenza)
        if ultima_giacenza > 0:
            val_medio = round((ultimo_valore_carico * ultima_giacenza + val_carico * carico) / nuova_giacenza, 3)
        else:
            val_medio = round(val_carico, 3)
        sql_insert_giacenza = """INSERT INTO movimenti (date, entrata, prezzo_carico, prezzo_medio, giacenza) VALUES 
        (?, ?, ?, ?, ?); """
        data = (now, carico, val_carico, val_medio, nuova_giacenza)
        insert_query(conn, sql_insert_giacenza, data)


# Funzione per effettuare scarico merce dal magazzino e ricalcolo del valore medio giacenza
def scarico(conn):
    global ultima_giacenza, old_prezzo_medio, ultimo_carico, ultimo_valore_carico, penultimo_valore_carico
    penultimo_valore_carico = 0
    ultima_giacenza = 0
    old_prezzo_medio = 0
    ultimo_carico = 0
    ultimo_valore_carico = 0
    now = datetime.datetime.now().replace(microsecond=0)
    scarico = float(input('Inserire q.tà scarico: \n'))
    sql_select_penultimocarico = """SELECT prezzo_carico FROM movimenti WHERE entrata IS NOT NULL ORDER BY id DESC 
    LIMIT 1 OFFSET 1 """
    sql_select_ultimocarico = """ SELECT * FROM movimenti WHERE entrata IS NOT NULL ORDER BY id DESC LIMIT 1; """
    sql_select_giacenza = """ SELECT giacenza FROM movimenti ORDER BY id DESC LIMIT 1; """
    if conn is not None:
        cursor = select_query(conn, sql_select_penultimocarico)
        for row in cursor:
            penultimo_valore_carico = row['prezzo_carico']

        cursor = select_query(conn, sql_select_ultimocarico)
        for row in cursor:
            ultimo_carico = row['entrata']
            ultimo_valore_carico = row['prezzo_carico']

        cursor = select_query(conn, sql_select_giacenza)
        for row in cursor:
            ultima_giacenza = row['giacenza']
        nuova_giacenza = (ultima_giacenza - scarico)
        if (nuova_giacenza - ultimo_carico) > 0:
            val_medio = round((((
                                            nuova_giacenza - ultimo_carico) * penultimo_valore_carico) + ultimo_carico * ultimo_valore_carico) / nuova_giacenza,
                              3)
        else:
            val_medio = round(ultimo_valore_carico, 3)
        sql_insert_giacenza = """INSERT INTO movimenti (date, uscita, prezzo_medio, giacenza) VALUES 
        (?, ?, ?, ?); """
        data = (now, scarico, val_medio, nuova_giacenza)
        insert_query(conn, sql_insert_giacenza, data)


# Funzione per visualizzare tutti i movimenti di magazzino
def movimenti(conn):
    sql_select_movimenti = """ SELECT date, entrata, uscita, giacenza, prezzo_medio FROM movimenti; """
    print('Tabella movimenti: \n')
    if conn is not None:
        cursor = select_query(conn, sql_select_movimenti)
        print(pd.read_sql_query(sql_select_movimenti, conn))


# Funzione per visualizzare il valore medio giacenza attuale
def val_giacenza(conn):
    sql_select_valGiacenza = """ SELECT prezzo_medio FROM movimenti ORDER BY ID DESC LIMIT 1 """
    if conn is not None:
        cursor = select_query(conn, sql_select_valGiacenza)
        for row in cursor:
            val_medio_giacenza = row['prezzo_medio']
    print('Valore Medio Giacenza: \n', val_medio_giacenza)


# Funzione per la visualizzazione del Menu
def menu(conn):
    choice = int(input('Scegliere operazione:\n 1- Carico \n 2- Scarico \n 3- Visualizza Movimenti \n 4- Valore Medio '
                       'Giacenza \n 5- Exit \n'))

    if choice == 1:
        carico(conn)
        menu(conn)

    elif choice == 2:
        scarico(conn)
        menu(conn)

    elif choice == 3:
        movimenti(conn)
        menu(conn)

    elif choice == 4:
        val_giacenza(conn)
        menu(conn)

    elif choice == 5:
        exit()


def main():
    database = "TestDB.db"
    conn = connect_db(database)
    sql_create_movimenti_table = """ CREATE TABLE IF NOT EXISTS movimenti (
                                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                                           date timestamp,
                                           entrata FLOAT,
                                           uscita FLOAT,
                                           prezzo_carico FLOAT,
                                           prezzo_medio FLOAT,
                                           prezzo_vendita FLOAT,
                                           giacenza FLOAT
                                       ); """
    if conn is not None:
        create_table(conn, sql_create_movimenti_table)
    else:
        print("Error! Cannot create database connection.")

    print('***************************************************************')
    print('*----------------- Gestione Magazzino 1.0 --------------------*')
    print('***************************************************************')
    menu(conn)


if __name__ == '__main__':
    main()
