from pymongo import MongoClient
from sqlalchemy import asc

class MongoDBController:
    def __init__(self, connection_string, database_name, collection_name):
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]

    def insert_many(self, data):
        """Insère plusieurs documents en une seule opération."""
        self.collection.insert_many(data)

    def insert_one(self, data):
        """Insère un seul document."""
        self.collection.insert_one(data)

    def find(self, query={}, projection=None):
        """Recherche des documents."""
        cursor = self.collection.find(query, projection)
        results = list(cursor)  # Convertit le curseur en une liste de documents
        return results
    
    def find_one(self, query):
        """Recherche un seul document."""
        return self.collection.find_one(query)

    def update_one(self, filter, update):
        """Met à jour un seul document."""
        self.collection.update_one(filter, update)

    def update_many(self, filter, update):
        """Met à jour plusieurs documents."""
        self.collection.update_many(filter, update)

    def delete_one(self, filter):
        """Supprime un seul document."""
        self.collection.delete_one(filter)

    def delete_many(self, filter):
        """Supprime plusieurs documents."""
        self.collection.delete_many(filter)

    def aggregate(self, pipeline):
        """Exécute une pipeline d'agrégation."""
        return self.collection.aggregate(pipeline)

    def create_index(self, keys):
        """Crée un index."""
        self.collection.create_index(keys)

    def drop_index(self, index_name):
        """Supprime un index."""
        self.collection.drop_index(index_name)

    def close(self):
        """Ferme la connexion."""
        self.client.close()