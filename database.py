# Azure
import pyodbc

# Variable environnement
from dotenv import load_dotenv
import os


# --------------- Azure ------------
server = os.getenv('SERVER', '')
database = os.getenv('DATABASE', '')
username = os.getenv('USERNAME', '')
password = os.getenv('PASSWORD', '')
driver = os.getenv('DRIVER', '')

# Connexion à la base de données
conn = pyodbc.connect(
    'DRIVER=' + driver + 
    ';SERVER=' + server + 
    ';PORT=1433;DATABASE=' + database + 
    ';UID=' + username + 
    ';PWD=' + password
    )


# --------------- Supabase ------------

# Variables Supabase
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')
