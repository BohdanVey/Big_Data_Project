class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        from cassandra.cluster import Cluster
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def execute(self, query):
        self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def get_unique_domains(self):
        """
        task 1
        """
        query = "SELECT * FROM unique_domains"
        prepared = self.session.prepare(query)
        rows = self.session.execute(prepared, ())
        for row in rows:
            print(row)

    def get_user_articles(self, user_id):
        """
        task 2
        """
        query = "SELECT * FROM pages_by_user WHERE user_id=?"
        prepared = self.session.prepare(query)
        rows = self.session.execute(prepared, (user_id,))
        for row in rows:
            print(row)


    def get_number_articles(self, domain):
        """
        task 3
        """
        query = "SELECT * FROM articles_by_domain WHERE domain=?"

        prepared = self.session.prepare(query)
        rows = self.session.execute(prepared, (domain,))
        for row in rows:
            print(row)

    def get_page_id(self, page_id):
        """
        task 4
        """
        query = "SELECT * FROM page_by_id WHERE page_id=?"
        prepared = self.session.prepare(query)
        rows = self.session.execute(prepared, (page_id,))
        for row in rows:
            print(row)
            print()




def main():
    host = 'cassandra-node1'
    port = 9042
    keyspace = 'wiki_keyspace'
    client = CassandraClient(host, port, keyspace)
    client.connect()

    #1
    print("TASK 1")
    client.get_unique_domains()
    
    #2
    print("TASK 2")

    client.get_user_articles(302461)

    #3
    print("TASK 3")

    client.get_number_articles("it.wikipedia.org")

    #4
    print("TASK 4")

    client.get_page_id(119167345)



    client.close()

if __name__ == '__main__':
    main()