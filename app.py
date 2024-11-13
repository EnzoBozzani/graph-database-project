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
        db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
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

    join_tables = ['takes', 'tcc_group', 'teaches', 'req', 'graduate']

    for table in tables:
        if table not in join_tables:
            cols = select_columns(table)
            rows = select_all(table)

            with neo4j_driver.session() as session:
                for row in rows:
                    node_data = {cols[i]: (float(value) if isinstance(value, Decimal) else value) for i, value in enumerate(row)}
                    query = f"CREATE (n:{table.capitalize()} {{ {', '.join([f'{col}: ${col}' for col in cols])} }})"
                    session.run(query, **node_data)

            print(f"Nós para a tabela '{table}' inseridos no Neo4j.")

            print("\n-----------------------------------------------------------------------------\n")

    dept_cols = select_columns('department')
    dept_rows = select_all('department')

    with neo4j_driver.session() as session:
        for row in dept_rows:
            node_data = {dept_cols[i]: (float(value) if isinstance(value, Decimal) else value) for i, value in enumerate(row)}
            fk1 = "boss_id"
            fk2 = "dept_name"

            query = f"""
            MATCH (p:Professor {{id: $boss_id}}), (d:Department {{dept_name: $dept_name}}) 
            CREATE (p)-[:HEADS]->(d)"""
            session.run(query, **{**{fk1: row[dept_cols.index(fk1)], fk2: row[dept_cols.index(fk2)]}})
    
    print(f"Relacionamentos 'HEADS' inserido no Neo4j.")

    print("\n-----------------------------------------------------------------------------\n")

    for table in tables:
        if table in join_tables: 
            cols = select_columns(table)
            rows = select_all(table)

            with neo4j_driver.session() as session:
                for row in rows:
                    node_data = {cols[i]: (float(value) if isinstance(value, Decimal) else value) for i, value in enumerate(row)}

                    relationship = ""
   
                    if table == 'takes':
                        fk1 = 'student_id'
                        fk2 = 'subj_id'
                        relationship = "TAKES"

                        rel_data = {k: v for k, v in node_data.items() if k not in [fk1, fk2]}
                        query = f"""
                        MATCH (s:Student {{id: $student_id}}), (sub:Subj {{id: $subj_id}})
                        CREATE (s)-[:TAKES {{ {', '.join([f'{k}: ${k}' for k in rel_data])} }}]->(sub)
                        """
                        session.run(query, **{**{fk1: row[cols.index(fk1)], fk2: row[cols.index(fk2)]}, **rel_data})
                    elif table == 'tcc_group':
                        fk1 = 'id'
                        fk2 = 'professor_id'
                        relationship = "MENTORED_BY"

                        rel_data = {k: v for k, v in node_data.items() if k not in [fk1, fk2]}
                        query = f"""
                        MATCH (s:Student {{group_id: $id}}), (prof:Professor {{id: $professor_id}})
                        CREATE (s)-[:MENTORED_BY {{ {', '.join([f'{k}: ${k}' for k in rel_data])} }}]->(prof)
                        """
                        session.run(query, **{**{fk1: row[cols.index(fk1)], fk2: row[cols.index(fk2)]}, **rel_data})
                    elif table == 'teaches':
                        fk1 = 'subj_id'
                        fk2 = 'professor_id'
                        relationship = "TEACHES"

                        rel_data = {k: v for k, v in node_data.items() if k not in [fk1, fk2]}
                        query = f"""
                        MATCH (prof:Professor {{id: $professor_id}}), (sub:Subj {{id: $subj_id}})
                        CREATE (prof)-[:TEACHES {{ {', '.join([f'{k}: ${k}' for k in rel_data])} }}]->(sub)
                        """
                        session.run(query, **{**{fk1: row[cols.index(fk1)], fk2: row[cols.index(fk2)]}, **rel_data})
                    elif table == 'req':
                        fk1 = 'subj_id'
                        fk2 = 'course_id'
                        relationship = "IS_REQ_OF"

                        rel_data = {k: v for k, v in node_data.items() if k not in [fk1, fk2]}
                        query = f"""
                        MATCH (sub:Subj {{id: $subj_id}}), (c:Course {{id: $course_id}})
                        CREATE (sub)-[:IS_REQ_OF {{ {', '.join([f'{k}: ${k}' for k in rel_data])} }}]->(c)
                        """
                        session.run(query, **{**{fk1: row[cols.index(fk1)], fk2: row[cols.index(fk2)]}, **rel_data})
                    elif table == 'graduate':
                        fk1 = 'student_id'
                        fk2 = 'course_id'
                        relationship = "GRADUATED"

                        rel_data = {k: v for k, v in node_data.items() if k not in [fk1, fk2]}
                        query = f"""
                        MATCH (s:Student {{id: $student_id}}), (c:Course {{id: $course_id}})
                        CREATE (s)-[:GRADUATED {{ {', '.join([f'{k}: ${k}' for k in rel_data])} }}]->(c)
                        """
                        session.run(query, **{**{fk1: row[cols.index(fk1)], fk2: row[cols.index(fk2)]}, **rel_data}) 

            print(f"Relacionamentos '{relationship}' inserido no Neo4j.")

            print("\n-----------------------------------------------------------------------------\n")

    
    
    
    # print(f"Relacionamentos 'HEADS' inserido no Neo4j.")

    # print("\n-----------------------------------------------------------------------------\n")
    
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
