import json
import uuid

def main():
    sql_batch = SQLBatch()
    sql_batch.result_persistence()
    sql_batch.add_procedure('hello.json', foo="10", bar="20")
    build_result_set(sql_batch, '/tmp/hello.txt')


class SQLBatch:
    def __init__(self):
        self._statements = []
        self.buffer_id = None
        self.result_should_persistence = False

    def result_persistence(self):
        self._statements = []
        self.buffer_id = uuid.uuid4()
        self.add_sql(create_buffer_table(self.buffer_id))
        self.result_should_persistence = True

    def add_procedure(self, procedure_path, *args, **kwargs):
        if self.buffer_id:
            kwargs.setdefault('temp_table', self.buffer_id)
        self._statements += read_procedure(procedure_path, *args, **kwargs)

    def add_sql(self, sql):
        self._statements.append(sql)

    def get_statements(self):
        return self._statements

def read_procedure(procedure_path, *args, **kwargs):
    with open("./assets/procedure/{}".format(procedure_path)) as src:
        sql_procedure =  json.loads(src.read())
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

def build_result_set(sql_batch, destination):
    sql_batch.add_sql(flush_buffer_table(sql_batch.buffer_id, destination))
    execute_sql(sql_batch.get_statements())

def create_buffer_table(buffer_id):
    return read_sql('init_buffer.sql', temp_table=buffer_id)

def flush_buffer_table(buffer_id, destination):
    return read_sql('flush_buffer.sql', temp_table=buffer_id, destination=destination)

def execute_sql(sql_to_executes):
    for sql_to_execute in sql_to_executes:
        print('Executing \r\n{}'.format(sql_to_execute))

if __name__ == '__main__':
    main()