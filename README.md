# graph-database-project

_Desenvolvido por:_

-   Adriel Henrique Foppa Lima - R.A.: 24.122.096-1
-   Alan Mantelatto Mlatisuma - R.A.: 24.122.015-1
-   Enzo Bozzani Martins - R.A.: 24.122.020-1
-   Igor Augusto Fiorini Rossi - R.A.: 24.122.023-5

_Seções_:

-   [Collections](#collections)
-   [Pré-requisitos](#pré-requisitos)
-   [Instalação](#instalação)
-   [Execução](#execução)

### Collections

-   course

```
{
    _id: ObjectId(),
    id: String,
    title: String
}
```

-   department

```
{
    _id: ObjectId(),
    dept_name: String,
    budget: Double,
    boss_id: String
}
```

-   graduate

```
{
    _id: ObjectId(),
    course_id: String,
    student_id: String,
    year: Integer,
    semester: Integer
}
```

-   professor

```
{
    _id: ObjectId(),
    id: String,
    name: String,
    dept_name: String,
    salary: Double
}
```

-   req

```
{
    _id: ObjectId(),
    course_id: String,
    subj_id: String
}
```

-   student

```
{
    _id: ObjectId(),
    id: String,
    name: String,
    course_id: String,
    group_id: String
}
```

-   subj

```
{
    _id: ObjectId(),
    id: String,
    title: String,
    dept_name: String
}
```

-   takes

```
{
    _id: ObjectId(),
    student_id: String,
    subj_id: String,
    year: Integer,
    semester: Integer,
    grade: Double,
    subjroom: String
}
```

-   tcc_group

```
{
    _id: ObjectId(),
    id: String,
    professor_id: String
}
```

-   teaches

```
{
    _id: ObjectId(),
    subj_id: String,
    professor_id: String,
    year: Integer,
    semester: Integer
}
```

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
