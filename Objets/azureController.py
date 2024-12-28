import pyodbc
import pandas as pd

class AzureController:
    def __init__(self, connection: pyodbc.Connection, table: str):
        self.connection = connection
        self.cursor = self.connection.cursor()
        self.table = table

    def get_value(self, row, column, default):
        """
        Retourne une valeur ou une valeur par défaut.
        """
        return row[column] if pd.notnull(row[column]) else default

    def insert_data(self, dataframe: pd.DataFrame):
        """
        Insère les données dans Azure SQL.
        """
        insert_query = f'''
            INSERT INTO {self.table}
            (date, pm10, pm2_5, carbon_monoxide, nitrogen_dioxide, sulphur_dioxide, ozone, grass_pollen, air_quality)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        for _, row in dataframe.iterrows():
            data_to_insert = (
                row["date"].isoformat() if pd.notnull(row["date"]) else None,
                self.get_value(row, "pm10", 0),
                self.get_value(row, "pm2_5", 0),
                self.get_value(row, "carbon_monoxide", 0),
                self.get_value(row, "nitrogen_dioxide", 0),
                self.get_value(row, "sulphur_dioxide", 0),
                self.get_value(row, "ozone", 0),
                self.get_value(row, "grass_pollen", 0),
                self.get_value(row, "air_quality", "Bonne"),
            )
            try:
                self.cursor.execute(insert_query, *data_to_insert)
            except Exception as e:
                print(f"Erreur lors de l'insertion de la ligne {row['date']}: {e}")
        self.connection.commit()
        print("Insertion terminée.")
    
    def select_data(self):
        try:
            select_query = f"SELECT * FROM {self.table}"
            self.cursor.execute(select_query)
            
            # Récupérer les résultats
            rows = self.cursor.fetchall()

            # Inspecter les colonnes récupérées
            columns = [column[0] for column in self.cursor.description]

            # Convertir les tuples en listes pour le DataFrame
            rows_list = [list(row) for row in rows] 
            
            # Créer le DataFrame
            data = pd.DataFrame(rows_list, columns=columns)
            
            # Afficher les données dans la console
            print(f"Données récupérées avec succès :\n{data.head()}")  # Affiche tout le DataFrame dans la console

            return data

        except pyodbc.Error as e:
            print(f"Erreur lors de la récupération des données : {e.args}")
            return None




    def delete_data(self):
        try:
            delete_query = f"DELETE FROM {self.table}"
            self.cursor.execute(delete_query)
            self.connection.commit()
            print(f"Les données de la table {self.table} ont été supprimées avec succès.")
        except pyodbc.Error as e:
            print(f"Erreur lors de la suppression : {e.args}")

    def reset_data(self):
        try:
            reset_query = f"DBCC CHECKIDENT ({self.table}, RESEED, 0);"
            self.cursor.execute(reset_query)
            self.connection.commit()
            print(f"Les données de la table {self.table} ont été réinitialisé avec succès.")
        except pyodbc.Error as e:
            print(f"Erreur lors de la réinitialisation : {e.args}")

    def close_connection(self):
        """
        Ferme la connexion à la base de données.
        """
        self.cursor.close()
        self.connection.close()
