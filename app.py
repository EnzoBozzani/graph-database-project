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
    print("Buscando nomes das tabelas no banco relacional....")
    with postgres_conn.cursor() as db:
        db.execute("SHOW TABLES;")
        res = db.fetchall()
        return [table[0] for table in res]


def select_all(table_name: str) -> list[Any]:
    print(f"Selecionado registros da tabela '{table_name}'...")
    with postgres_conn.cursor() as db:
        db.execute(f"SELECT * FROM {table_name};")
        res = db.fetchall()
        return res


def select_columns(table_name: str) -> list[str]:
    print(f"Selecionando os nomes das colunas da tabela '{table_name}'...")
    with postgres_conn.cursor() as db:
        db.execute(f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table_name}';")
        res = db.fetchall()
        return [table[0] for table in res]


def create_constraints():
    print("Criando constraints e indíces no Neo4j...")
    with neo4j_driver.session() as session:
        session.run("CREATE CONSTRAINT FOR (s:Student) REQUIRE s.id IS UNIQUE;")
        session.run("CREATE CONSTRAINT FOR (p:Professor) REQUIRE p.id IS UNIQUE;")
        session.run("CREATE CONSTRAINT FOR (c:Course) REQUIRE c.id IS UNIQUE;")
        session.run("CREATE CONSTRAINT FOR (d:Department) REQUIRE d.dept_name IS UNIQUE;")
        session.run("CREATE CONSTRAINT FOR (g:TccGroup) REQUIRE g.id IS UNIQUE;")
        session.run("CREATE CONSTRAINT FOR (sub:Subj) REQUIRE sub.id IS UNIQUE;")


def transfer_data():
    tables = show_tables()

    print("\n-----------------------------------------------------------------------------\n")

    for table in tables:
        cols = select_columns(table)
        rows = select_all(table)

        with neo4j_driver.session() as session:
            # Criando nós para cada registro da tabela
            for row in rows:
                # Convertendo valores Decimal para float, se necessário
                node_data = {cols[i]: (float(value) if isinstance(value, Decimal) else value) for i, value in enumerate(row)}

                # Identificar chaves estrangeiras e criar relacionamentos, se aplicável
                if table == 'takes':
                    # Defina aqui as chaves estrangeiras e relacionamento para 'takes' como exemplo
                    fk1 = 'student_id'  # chave estrangeira 1
                    fk2 = 'subj_id'  # chave estrangeira 2
                    
                    # Criar um relacionamento entre o estudante e a disciplina
                    rel_data = {k: v for k, v in node_data.items() if k not in [fk1, fk2]}
                    query = f"""
                    MATCH (s:Student {{id: $student_id}}), (sub:Subj {{id: $subj_id}})
                    CREATE (s)-[:TAKES {{ {', '.join([f'{k}: ${k}' for k in rel_data])} }}]->(sub)
                    """
                    session.run(query, **{**{fk1: row[cols.index(fk1)], fk2: row[cols.index(fk2)]}, **rel_data})
                else:
                    # Caso contrário, cria o nó normalmente
                    query = f"CREATE (n:{table.capitalize()} {{ {', '.join([f'{col}: ${col}' for col in cols])} }})"
                    session.run(query, **node_data)

            print(f"Nós para a tabela '{table}' inseridos no Neo4j.")

        print("\n-----------------------------------------------------------------------------\n")
    
    print("Transferência concluída!")

    print("\n-----------------------------------------------------------------------------\n")


def delete_all_nodes():
    with neo4j_driver.session() as session:
        print("Deletando todos nós no Neo4j...")
        session.run("MATCH (n) DETACH DELETE n")
        print("Nós deletados com sucesso!!")

# Main script execution
if __name__ == '__main__':
    delete_all_nodes()
    transfer_data()
    
    # Ensure output directory exists
    if not os.path.exists('./output'):
        os.mkdir('./output')

    # Running predefined queries for output (assuming queries are in place)
    query_student_academic_record()
    query_professor_academic_record()
    query_graduated_students()
    query_chiefs_of_departments()
    query_tcc_group()

    print("Outputs das queries disponíveis na pasta './output'.")
