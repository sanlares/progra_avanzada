# database.py
from psycopg2 import pool
import os

class Database:
    #clase singleton para manejar el pool de conexiones
    _connection_pool = None

    @staticmethod
    def initialize(): #se conecta
        Database._connection_pool = pool.SimpleConnectionPool(
            1,
            10,
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )

    @staticmethod
    def get_connection(): #devuelve la conexion
        return Database._connection_pool.getconn()

    @staticmethod
    def return_connection(connection):
        Database._connection_pool.putconn(connection)

    @staticmethod
    def close_all_connections():
        Database._connection_pool.closeall()
