from models.SQLBatch import SQLBatch, flush_buffer_table

def main():
    sql_batch = SQLBatch()
    sql_batch.result_persistence()
    sql_batch.add_procedure('hello.json', foo="10", bar="20")
    build_result_set(sql_batch, '/tmp/hello.txt')


def build_result_set(sql_batch, destination):
    sql_batch.add_sql(flush_buffer_table(sql_batch.buffer_id, destination)) # Do i have better design about that?
    execute_sql(sql_batch.get_statements())


def execute_sql(sql_to_executes):
    for sql_to_execute in sql_to_executes:
        print('Executing \r\n{}'.format(sql_to_execute))


if __name__ == '__main__':
    main()
