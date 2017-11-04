import json

def read_procedure(procedure_path, *args, **kwargs):
    with open("./assets/procedure/{}".format(procedure_path)) as src:
        sql_procedure = json.loads(src.read())
        sql_to_execute = []
        for sql_file in sql_procedure:
            sql_content = read_sql(sql_file["file"], *args, **kwargs)
            for sql in sql_content.split(";\n"):
                if not sql.endswith(';'):
                    sql = sql + ';'
                sql_to_execute.append(sql)
        return sql_to_execute


def read_sql(sql_file_path, *args, **kwargs):
    with open("./assets/sql/{}".format(sql_file_path)) as sql_file:
        return sql_file.read().format(*args, **kwargs)