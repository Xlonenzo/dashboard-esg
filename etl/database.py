"""
Modulo de conexao com o banco de dados PostgreSQL
"""
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from contextlib import contextmanager
from config import get_connection_string, get_psycopg2_connection_params


class DatabaseConnection:
    """Classe para gerenciar conexoes com PostgreSQL."""

    def __init__(self):
        self.connection_string = get_connection_string()
        self.conn_params = get_psycopg2_connection_params()
        self.engine = None

    def get_engine(self):
        """Retorna uma engine SQLAlchemy."""
        if self.engine is None:
            self.engine = create_engine(self.connection_string)
        return self.engine

    @contextmanager
    def get_connection(self):
        """Context manager para conexao psycopg2."""
        conn = psycopg2.connect(**self.conn_params)
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, query: str, params: tuple = None):
        """Executa uma query SQL."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor

    def execute_many(self, query: str, data: list):
        """Executa uma query com multiplos registros."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, data)
            conn.commit()

    def read_sql(self, query: str) -> pd.DataFrame:
        """Le dados do SQL para um DataFrame."""
        engine = self.get_engine()
        return pd.read_sql(query, engine)

    def to_sql(self, df: pd.DataFrame, table_name: str, schema: str = "esg",
               if_exists: str = "append", index: bool = False):
        """Insere um DataFrame em uma tabela SQL."""
        engine = self.get_engine()
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=index,
            chunksize=1000
        )

    def truncate_table(self, table_name: str, schema: str = "esg"):
        """Limpa uma tabela."""
        query = f"TRUNCATE TABLE {schema}.{table_name} RESTART IDENTITY CASCADE"
        self.execute_query(query)

    def get_max_id(self, table_name: str, id_column: str, schema: str = "esg") -> int:
        """Retorna o maior ID de uma tabela."""
        query = f"SELECT COALESCE(MAX({id_column}), 0) FROM {schema}.{table_name}"
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else 0

    def get_lookup(self, table_name: str, key_column: str, value_column: str,
                   schema: str = "esg") -> dict:
        """Retorna um dicionario de lookup de uma tabela."""
        query = f"SELECT {value_column}, {key_column} FROM {schema}.{table_name}"
        df = self.read_sql(query)
        return dict(zip(df[value_column], df[key_column]))

    def test_connection(self) -> bool:
        """Testa a conexao com o banco."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                return True
        except Exception as e:
            print(f"Erro de conexao: {e}")
            return False

    def log_import(self, tabela: str, arquivo: str, registros: int,
                   erros: int = 0, status: str = "Sucesso", mensagem: str = None):
        """Registra log de importacao."""
        query = """
        INSERT INTO esg.logimportacao
        (tabeladestino, arquivoorigem, registrosimportados, registroscomerro, status, mensagemerro)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.execute_query(query, (tabela, arquivo, registros, erros, status, mensagem))


# Instancia global
db = DatabaseConnection()
