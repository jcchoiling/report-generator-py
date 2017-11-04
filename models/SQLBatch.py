import uuid

from sql import reader

class SQLBatch:
    def __init__(self):
        self._statements = []
        self.buffer_id = None
        self.result_should_persistence = False

    def result_persistence(self):
        self._statements = []
        self.buffer_id = uuid.uuid4()
        self.add_sql(_create_buffer_table(self.buffer_id))
        self.result_should_persistence = True

    def add_procedure(self, procedure_path, *args, **kwargs):
        if self.buffer_id:
            kwargs.setdefault('temp_table', self.buffer_id)
        self._statements += reader.read_procedure(procedure_path, *args, **kwargs)

    def add_sql(self, sql):
        self._statements.append(sql)

    def get_statements(self):
        return self._statements


def _create_buffer_table(buffer_id):
    return reader.read_sql('init_buffer.sql', temp_table=buffer_id)


def flush_buffer_table(buffer_id, destination):
    return reader.read_sql('flush_buffer.sql', temp_table=buffer_id, destination=destination)