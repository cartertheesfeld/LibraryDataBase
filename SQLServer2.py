import pyodbc



class SQLServer2:
    def __init__(self, server, database, trusted_connection=True):
        self.server = server
        self.database = database
        self.trusted_connection = trusted_connection
        self.conn = None
        self.cursor = None
    
    def connect(self):
        if self.trusted_connection:
            self.conn = pyodbc.connect(
                f"Driver={{ODBC Driver 17 for SQL Server}};Server={self.server};Database={self.database};Trusted_Connection=yes;"
            )
        else:
            raise ValueError("Not implemented: Non-trusted connection")

        self.cursor = self.conn.cursor()
    
    def disconnect(self):
        self.conn.close()
    
    def execute_query(self, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows
    
    def send_data(self, table_name, data, columns=None):
        if columns is None:
            query = f"INSERT INTO {table_name} VALUES ({','.join(['?' for _ in range(len(data[0]))])})"
        else:
            query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(['?' for _ in range(len(data[0]))])})"

        self.cursor.executemany(query, data)
        self.conn.commit()
    
    def query_table(self, table_name):
        query = f"SELECT * FROM {table_name}"
        return self.execute_query(query)
