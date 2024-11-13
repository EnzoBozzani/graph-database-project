# graph-database-project

_Desenvolvido por:_

-   Adriel Henrique Foppa Lima - R.A.: 24.122.096-1
-   Alan Mantelatto Mlatisuma - R.A.: 24.122.015-1
-   Enzo Bozzani Martins - R.A.: 24.122.020-1
-   Igor Augusto Fiorini Rossi - R.A.: 24.122.023-5

_Seções_:

-   [Node Labels](#node-labels)
-   [Relacionamentos](#relacionamentos)
-   [Pré-requisitos](#pré-requisitos)
-   [Instalação](#instalação)
-   [Execução](#execução)

### Node Labels

-   Student

    -   Atributos:
        -   course_id: String
        -   name: String
        -   id: String
        -   group_id: String | null
    -   Relacionamentos:
        -   TAKES
        -   MENTORED_BY
        -   GRADUATED

-   Department

    -   Atributos:
        -   dept_name: String
        -   boss_id: String
        -   budget: Float

-   Professor

    -   Atributos:
        -   dept_name: String
        -   name: String
        -   id: String
        -   salary: Float
    -   Relacionamentos:
        -   TEACHES
        -   HEADS

-   Subj

    -   Atributos:
        -   dept_name: String
        -   title: String
        -   id: String
    -   Relacionamentos:
        -   IS_REQ_OF

-   Course

    -   Atributos:
        -   title: String
        -   id: String

### Relacionamentos

-   GRADUATED (Student-[:GRADUATED]->Course)

    -   year: Integer
    -   semester: Integer

-   IS_REQ_OF (Subj-[:IS_REQ_OF]->Course)

-   MENTORED_BY (Student-[:METORED_BY]->Professor)

-   TAKES (Student-[:TAKES]->Subj)

    -   subjroom: String
    -   year: Integer
    -   grade: Float
    -   semester: Integer

-   TEACHES (Professor-[:TEACHES]->Subj)
    -   year: Integer
    -   semester: Integer

### Pré-requisitos

Essa aplicação usa [Python](https://www.python.org/) com [Poetry](https://python-poetry.org/) como gerenciador de dependências.

Tenha instalado em sua máquina:

-   [Python](https://www.python.org/). Versão ^3.11
-   [Poetry](https://python-poetry.org/). O projeto foi construído usando Poetry, mas é possível instalar as dependências usando pip (siga para a próxima seção).

### Instalação

Crie um novo virtual env com seu gerenciador de ambientes virtuais favoritos (pyenv-virtualenv, venv, etc)

Clone o repositório:

```
git clone git@github.com:EnzoBozzani/graph-database-project.git
```

Ative o virtualenv em sua pasta da aplicação.

-   Instale as dependências:

    -   Usando pip (cheque se o pip está no seu virtualenv usando `pip --version`):

        ```
        pip install -r requirements.txt
        ```

    -   Usando poetry:
        ```
        poetry install
        ```

Você pode usar Neo4J e PostgreSQL em sua máquina local ou de maneira remota. Apenas configure as variáveis de ambiente com as URLs de conexão, assim como especificado no arquivo .env.example:

```
POSTGRES_URL=""
NEO4J_URL=""
NEO4J_USER=""
NEO4J_PASSWORD=""
```

Para transferir dados de um banco PostgreSQL, é necessário tem um banco PostgreSQL. Siga o exemplo em [https://github.com/EnzoBozzani/projeto-banco-de-dados](https://github.com/EnzoBozzani/projeto-banco-de-dados), o qual contém migrations, seeder e queries (em SQL).

Feito isso, siga para a próxima seção.

### Execução

Para executar a aplicação:

```
python app.py
```

Os logs (que informam o andamento) serão exibidos no terminal. Ao fim da execução, será gerada uma pasta 'output' com os resultados das seguintes queries (que serão feitas sobre os dados do Neo4J):

1. histórico escolar de qualquer aluno, retornando o código e nome da disciplina, semestre e ano que a disciplina foi cursada e nota final
2. histórico de disciplinas ministradas por qualquer professor, com semestre e ano
3. listar alunos que já se formaram (foram aprovados em todos os cursos de uma matriz curricular) em um determinado semestre de um ano
4. listar todos os professores que são chefes de departamento, junto com o nome do departamento
5. saber quais alunos formaram um grupo de TCC e qual professor foi o orientador
