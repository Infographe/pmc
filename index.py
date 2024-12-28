import pyodbc
from dotenv import load_dotenv
import os
import api
from Objets.supabaseController import SupabaseController
from Objets.azureController import AzureController
from Objets.csvController import CSVController

# Charger les variables d'environnement
load_dotenv('.env.config')

# Variables environnementales
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
azure_connection_string = os.getenv("AZURE_CONNECTION_STRING")

# Vérification des variables d'environnement
if not supabase_url or not supabase_key:
    raise ValueError("Les variables 'SUPABASE_URL' ou 'SUPABASE_KEY' sont manquantes.")
if not azure_connection_string:
    raise ValueError("La variable 'AZURE_CONNECTION_STRING' est manquante.")

# Créer la connexion Azure SQL
azure_connection = pyodbc.connect(azure_connection_string)

# Tables
tables = [
    "qualite_air",
    "maladies_chroniques_diabete",
    "maladies_chroniques_respiratoires",
    "maladies_chroniques_cardiologiques",
    "donnees_patient",
    "alimentation",
    "antecedents_familiaux"
]

# Sélection d'une table par index
table = tables[1] 

# Supabase
supabaseController = SupabaseController(supabase_url, supabase_key, table)
# controller = SupabaseController(url, key, "qualite_air")
dataframes = supabaseController.select_data()

# Exporter chaque DataFrame du dictionnaire
for table_name, dataframe in dataframes.items():
    if dataframe is not None and not dataframe.empty:
        filename = f"{table_name}.csv"
        dataframe.to_csv(filename, index=False)
        print(f"Les données de la table '{table_name}' ont été exportées vers {filename}.")
    else:
        print(f"Aucune donnée à exporter pour la table '{table_name}'.")

# supabaseController.insert_data(api.daily_dataframe)

supabaseController.select_data()
# supabaseController.delete_data()

# Azure SQL
azureController = AzureController(azure_connection, f"{table}")
# azureController.insert_data(api.daily_dataframe)
# azureController.select_data()
# azureController.delete_data()
# azureController.reset_data()
azureController.close_connection()

# Exporter en CSV
csvController = CSVController(f"CSV/{table}.csv")
csvController.export_to_csv(dataframes)
