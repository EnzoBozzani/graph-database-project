# Standard
import os
import json

# Third Party
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

neo4j_driver = GraphDatabase.driver(os.environ['NEO4J_URL'], auth=(os.environ['NEO4J_USER'], os.environ['NEO4J_PASSWORD']))

def query_student_academic_record():
    print("Buscando o histórico escolar do aluno de RA 100000001")
    query = """
    MATCH (s:Student {id: '100000001'})-[t:TAKES]->(sub:Subj)
    RETURN s.id AS student_id, sub.id AS subj_id, sub.title AS subject_title,
           t.semester AS semester, t.year AS year, t.grade AS grade
    """

    with neo4j_driver.session() as session:
        result = session.run(query)
        records = [record.data() for record in result]

    with open('./output/query-1.json', 'w') as f:
        json.dump(records, f, ensure_ascii=False)

def query_professor_academic_record():
    print("Buscando o histórico de disciplinas ministradas pelo professor de ID P005")
    query = """
    MATCH (p:Professor {id: 'P005'})-[t:TEACHES]->(sub:Subj)
    RETURN p.id AS professor_id, sub.id AS subj_id, sub.title AS subject_title, 
           t.semester AS semester, t.year AS year
    """

    with neo4j_driver.session() as session:
        result = session.run(query)
        records = [record.data() for record in result]

    with open('./output/query-2.json', 'w') as f:
        json.dump(records, f, ensure_ascii=False)

def query_graduated_students():
    print("Buscando os alunos que se formaram no segundo semestre de 2018")
    query = """
    MATCH (s:Student)-[:GRADUATED {semester: 2, year: 2018}]->(c:Course)
    RETURN s.id AS student_id, s.name AS name
    """

    with neo4j_driver.session() as session:
        result = session.run(query)
        records = [record.data() for record in result]

    with open('./output/query-3.json', 'w') as f:
        json.dump(records, f, ensure_ascii=False)

def query_chiefs_of_departments():
    print("Buscando os professores que são chefes de departamento")
    query = """
    MATCH (p:Professor)-[:HEADS]->(d:Department)
    RETURN p.id AS professor_id, p.name AS professor_name, d.dept_name AS department_name, d.budget AS department_budget
    """

    with neo4j_driver.session() as session:
        result = session.run(query)
        records = [record.data() for record in result]

    with open('./output/query-4.json', 'w') as f:
        json.dump(records, f, ensure_ascii=False)

def query_tcc_group():
    print("Buscando os alunos que formaram o grupo de TCC de ID CC1111111")
    query = """
    MATCH (s:Student {group_id: 'CC1111111'})-[:MENTORED_BY]->(p:Professor)
    RETURN s.id AS student_id, s.name AS student_name, p.name AS professor_name
    """

    with neo4j_driver.session() as session:
        result = session.run(query)
        records = [record.data() for record in result]

    with open('./output/query-5.json', 'w') as f:
        json.dump(records, f, ensure_ascii=False)
