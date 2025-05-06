import pyodbc as podbc

class SQLServerConnector:
    def __init__(self, driver, server, database, trusted_connection="Yes", username = "sa", password = "mapdms01", timeout = 120 ):
        self.driver = driver
        self.server = server
        self.database = database
        self.trusted_connection = trusted_connection
        self.username = username
        self.password = password
        self.timeout = timeout
    
    def connect(self):
        connection = podbc.connect(
            Trusted_Connection=self.trusted_connection,
            Driver=self.driver,
            Server=self.server,
            Database=self.database
            
        )
        return connection

