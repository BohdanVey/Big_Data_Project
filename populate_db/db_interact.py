import json
import pandas as pd
import datetime

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

    def insert_unique_domains(self, domain):
        """
        # 1. Return the list of existing domains for which pages were created.
        """
        query = "INSERT INTO unique_domains(page_domain) VALUES (?) IF NOT EXISTS"
        prepared = self.session.prepare(query)
        try:
            self.session.execute(prepared, (domain,))
        except Exception as E:
            print(E)

    def insert_user_pages(self, user_id, page_id, data):
        """
        # 2. Return all the pages which were created by the user with a specified user_id.
        """
        query = "INSERT INTO pages_by_user(user_id, page_id, page) VALUES (?,?,?)"
        prepared = self.session.prepare(query)
        try:
            self.session.execute(prepared, (user_id, page_id, data))
        except Exception as E:
            print(E)

    def insert_articles_per_domain(self, domain):
        """
        # 3. Return the number of articles created for a specified domain.
        """

        # USING BEUTIFUL PROPERTY OF CASSANDRA COUNTER COLUMN to increase count by 1
        query = "UPDATE articles_by_domain SET articles_num = articles_num + 1 WHERE domain=?;"

        prepared = self.session.prepare(query)
        try:
            self.session.execute(prepared, (domain,))
        except Exception as E:
            print(E)

    def insert_page_by_id(self, page_id, data):
        """
        # 4. Return the page with the specified page_id
        """
        query = "INSERT INTO page_by_id(page_id,page) VALUES (?,?)"
        prepared = self.session.prepare(query)
        try:
            self.session.execute(prepared, (page_id, data))
        except Exception as E:
            print(E)
# id, name, created pages

    def insert_user_page_date(self, user_id, page_id, user_text, time_created):
        """
        # 5. Return the id, name, and the number of created pages of all the users who created at least one page in a specified time range. 
        """
        query = "INSERT INTO created_pages_time(user_id,page_id,user_text,time_created) VALUES (?,?,?,?);"
        prepared = self.session.prepare(query)
        try:
            self.session.execute(prepared, (user_id, page_id, user_text, time_created))
        except Exception as E:
            print(E)

    def close(self):
        self.session.shutdown()




def handle_entry(client, entry):
    """
    function that will update all tables with the new entry
    """
    entry_json = json.dumps(entry)

    # 1-st task:
    # client.insert_unique_domains(entry["meta"]["domain"])

    # 2-nd task:

    # there are no user_id columns for some entries:
    # if "performer" in entry.keys():
    #     if "user_id" in entry["performer"].keys():
    #         client.insert_user_pages(entry["performer"]["user_id"], entry["page_id"], entry_json)

    # 3-rd task:
    # client.insert_articles_per_domain(entry["meta"]["domain"])

    # 4-th task
    # client.insert_page_by_id(entry["page_id"], entry_json)

    # 5-th task
    curr_time = datetime.datetime.now()

    if "performer" in entry.keys():
        if "user_id" in entry["performer"].keys() and "user_text" in entry["performer"].keys():
            client.insert_user_page_date(entry["performer"]["user_id"],
                                         entry["page_id"],
                                         entry["performer"]["user_text"],
                                         curr_time
                                        )

    # print()


host = 'cassandra-node1'
port = 9042
keyspace = 'wiki_keyspace'
client = CassandraClient(host, port, keyspace)
client.connect()

with open("data.json") as f:
    db = json.loads(f.read())

    entry_list = db["data"]
    for entry in entry_list:
        # the only fucntion that you will use:
        handle_entry(client, entry)


client.close()
print("done")


# Return the list of existing domains for which pages were created.
# Return all the pages which were created by the user with a specified user_id.
# Return the number of articles created for a specified domain.
# Return the page with the specified page_id
# Return the id, name, and the number of created pages of all the users who created at least one page in a specified time range. 
