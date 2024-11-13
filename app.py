# Standard
from typing import Any
import os
from decimal import Decimal

# Third Party
import psycopg2
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Local
from queries import query_chiefs_of_departments, query_graduated_students, query_professor_academic_record, query_student_academic_record, query_tcc_group

load_dotenv()

postgres_conn = psycopg2.connect(os.environ['POSTGRES_URL'])
neo4j_driver = GraphDatabase.driver(os.environ['NEO4J_URL'], auth=(os.environ['NEO4J_USER'], os.environ['NEO4J_PASSWORD']))

def show_tables() -> list[str]:
    print("Buscando nomes das tabelas no banco relacional...")
    with postgres_conn.cursor() as db:
        db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        res = db.fetchall()
        postgres_conn.commit()
        return [table[0] for table in res]

def select_all(table_name: str) -> list[Any]:
    print(f"Selecionando registros da tabela '{table_name}'...")
    with postgres_conn.cursor() as db:
        db.execute(f"SELECT * FROM {table_name};")
        res = db.fetchall()
        postgres_conn.commit()
        return res
    
def select_columns(table_name: str) -> list[str]:
    print(f"Selecionando os nomes das colunas da tabela '{table_name}'...")
    with postgres_conn.cursor() as db:
        db.execute(f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table_name}';")
        res = db.fetchall()
        postgres_conn.commit()
        return [table[0] for table in res]

def transfer_data():
    tables = show_tables()

    print("\n-----------------------------------------------------------------------------\n")

    for table in tables:
        cols = select_columns(table)
        rows = select_all(table)

        with neo4j_driver.session() as session:
            # Criando nós para cada registro da tabela
            for row in rows:
                node_data = {cols[i]: (float(value) if isinstance(value, Decimal) else value) for i, value in enumerate(row)}
                
                # Criando nós no Neo4J com base na tabela
                query = f"CREATE (n:{table.capitalize()} {{ {', '.join([f'{col}: ${col}' for col in cols])} }})"
                session.run(query, **node_data)

            print(f"Nós para a tabela '{table}' inseridos no Neo4J.")

        print("\n-----------------------------------------------------------------------------\n")
    
    print("Transferência concluída!")

    print("\n-----------------------------------------------------------------------------\n")

def delete_all_nodes():
    with neo4j_driver.session() as session:
        print("Deletando todos os nós no banco Neo4j...")
        session.run("MATCH (n) DETACH DELETE n")
        print("Todos os nós foram deletados com sucesso!")

if __name__ == '__main__':
    delete_all_nodes()
    transfer_data()

    print("Outputs estarão na pasta ./output")
    print("\n-----------------------------------------------------------------------------\n")

    if not os.path.exists('./output'):
        os.mkdir('./output')

    query_student_academic_record()
    query_professor_academic_record()
    query_graduated_students()
    query_chiefs_of_departments()
    query_tcc_group()

    print("\n-----------------------------------------------------------------------------\n")
    print("Outputs das queries disponíveis na pasta output!")
    print("\n-----------------------------------------------------------------------------\n")
