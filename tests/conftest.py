
class ConnParams:
    def __init__(self):
        self.host = 'postgresql'
        self.port = '5432'
        self.user = 'postgres'
        self.password = 'postgres'
        self.dbname = 'test'

    def get(self):
        return self.__dict__
