import pyodbc
from dotenv import load_dotenv
import os
import api
import pandas as pd
import numpy as np
from Objets.exportController import ExportController
from Objets.supabaseController import SupabaseController
from Objets.azureController import AzureController
from Objets.mongoDBController import MongoDBController


# --------- Tables
# Tables
tables = [
    "qualite_air",
    "maladies_chroniques_diabete",
    "maladies_chroniques_respiratoires",
    "maladies_chroniques_cardiologiques",
    "donnees_patient",
    "alimentation",
    "antecedents_familiaux",
    "mode_de_vie"
]

# Sélection d'une table par index
table = tables[7] 


# --------- Variables
# Charger les variables d'environnement
load_dotenv('.env.config')

# Variables d'environnement
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
azure_connection_string = os.getenv("AZURE_CONNECTION_STRING")
mongo_connection_string = os.getenv("CONNECTION_STRING")
mongo_database_string = "Maladies_chroniques"
mongo_collection_name_string = table

# Vérification des variables d'environnement
if not supabase_url or not supabase_key:
    raise ValueError("Les variables 'SUPABASE_URL' ou 'SUPABASE_KEY' sont manquantes.")
if not azure_connection_string:
    raise ValueError("La variable 'AZURE_CONNECTION_STRING' est manquante.")


# --------- Connexion
# Créer la connexion Azure SQL
azure_connection = pyodbc.connect(azure_connection_string)

# Créer la connexion MongoDB
mongo_controller = MongoDBController(
    mongo_connection_string,
    mongo_database_string,
    mongo_collection_name_string
)

# Créer la connexion Supabase
supabaseController = SupabaseController(supabase_url, supabase_key, table)


# --------- CRUD
# -- Supabase
df_supabase = supabaseController.select_data()

# Exporter chaque DataFrame du dictionnaire en CSV
    # for table_name, dataframe in f_.items():
    #     if dataframe is not None and not dataframe.empty:
    #         filename_csv = f"CSV/{table_name}.csv"
    #         filename_json = f"JSON/{table_name}.json"  # Utilisez un nom de fichier différent pour JSON

    #         dataframe.to_csv(filename_csv, index=False)
    #         dataframe.to_json(filename_json, orient='records', indent=4)  # Utilisez orient='records' pour exporter au format JSON

    #         print(f"Les données de la table '{table_name}' ont été exportées vers {filename_csv} et {filename_json}.")
    #     else:
    #         print(f"Aucune donnée à exporter pour la table '{table_name}'.")
# supabaseController.insert_data(api.daily_dataframe)
# supabaseController.delete_data()

# -- Azure SQL
azureController = AzureController(azure_connection, f"{table}")
# azureController.insert_data(api.daily_dataframe)
azureController.select_data()
# azureController.delete_data()
# azureController.reset_data()
azureController.close_connection()


# -- MongoDB
# Insérer data
# Convertir le DataFrame en une liste de dictionnaires
# data_list = api.daily_dataframe.to_dict()
data_list = list(api.daily_dataframe)
print(data_list)
# # Avant la conversion
# print(f"api.daily_dataframe:\n{api.daily_dataframe}")

# # Après la conversion (si applicable)
# print(f"data_list:\n{data_list}")

# # Avant l'insertion
# if not data_list:
#   print("Aucune donnée à insérer dans MongoDB")
# else:
#   mongo_controller.insert_many(data_list)


# Lire tous les documents
results = mongo_controller.find() 
for document in results:
    print(f"document => {document}")

# Ou 
df = pd.DataFrame(results) 
print(f"dataframe => {df}")

# --------- Exports
export_controller = ExportController()

output_dir = 'exports/CSV'
export_controller.export_dataframes(df_supabase, output_dir, format='csv', indent=2)
output_dir = 'exports/JSON'
export_controller.export_dataframes(df_supabase, output_dir, format='json', indent=2)
