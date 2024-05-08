import sql_queries

if __name__ == '__main__':
    for i in sql_queries.drop_table_queries:
        print(i)
    for i in sql_queries.create_table_queries:
        print(i)
    for i in sql_queries.copy_table_queries:
        print(i)
    for i in sql_queries.insert_table_queries:
        print(i)
