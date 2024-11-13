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
    MATCH (s:Student {student_id: '100000001'})-[:TOOK]->(c:Course)
    RETURN s.student_id AS student_id, c.course_id AS course_id, c.title AS course_title, 
           c.semester AS semester, c.year AS year, c.grade AS grade
    """

    with neo4j_driver.session() as session:
        result = session.run(query)
        records = [record.data() for record in result]

    with open('./output/query-1.json', 'w') as f:
        json.dump(records, f, ensure_ascii=False)

def query_professor_academic_record():
    print("Buscando o histórico de disciplinas ministradas pelo professor de ID P005")
    query = """
    MATCH (p:Professor {professor_id: 'P005'})-[:TAUGHT]->(c:Course)
    RETURN p.professor_id AS professor_id, c.course_id AS course_id, c.title AS course_title, 
           c.semester AS semester, c.year AS year
    """

    with neo4j_driver.session() as session:
        result = session.run(query)
        records = [record.data() for record in result]

    with open('./output/query-2.json', 'w') as f:
        json.dump(records, f, ensure_ascii=False)

def query_graduated_students():
    print("Buscando os alunos que se formaram no segundo semestre de 2018")
    query = """
    MATCH (s:Student)-[:GRADUATED_IN]->(c:Course {semester: 2, year: 2018})
    RETURN s.student_id AS student_id, s.name AS name
    """

    with neo4j_driver.session() as session:
        result = session.run(query)
        records = [record.data() for record in result]

    with open('./output/query-3.json', 'w') as f:
        json.dump(records, f, ensure_ascii=False)

def query_chiefs_of_departments():
    print("Buscando os professores que são chefes de departamento")
    query = """
    MATCH (p:Professor)-[:HEAD_OF]->(d:Department)
    RETURN p.professor_id AS professor_id, p.name AS professor_name, d.department_id AS department_id, d.name AS department_name
    """

    with neo4j_driver.session() as session:
        result = session.run(query)
        records = [record.data() for record in result]

    with open('./output/query-4.json', 'w') as f:
        json.dump(records, f, ensure_ascii=False)

def query_tcc_group():
    print("Buscando os alunos que formaram o grupo de TCC de ID CC1111111")
    query = """
    MATCH (g:TccGroup {group_id: 'CC1111111'})<-[:PART_OF]-(s:Student), (g)-[:MENTORED_BY]->(p:Professor)
    RETURN g.group_id AS group_id, p.name AS professor_name, collect(s.name) AS students
    """

    with neo4j_driver.session() as session:
        result = session.run(query)
        records = result.single().data()

    with open('./output/query-5.json', 'w') as f:
        json.dump(records, f, ensure_ascii=False)
