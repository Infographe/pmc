import requests_cache
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv('.env.config')
db_url = os.getenv('WEATHER_API')

# Vérification de l'URL
if not db_url:
    raise ValueError("L'URL de l'API (WEATHER_API) n'est pas définie dans le fichier .env.config.")

# Initialisation de la session de cache
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)

# Configuration de l'URL de l'API et des paramètres
params = {
    "latitude": 44.3773,
    "longitude": 5.6114,
    "hourly": [
        "pm10", "pm2_5", "carbon_monoxide", 
        "nitrogen_dioxide", "sulphur_dioxide", 
        "ozone", "grass_pollen"
    ],
    "timezone": "Europe/Moscow",
    "start_date": "2023-07-02",
    "end_date": "2024-07-02",
}

# Récupération des données de l'API
response = cache_session.get(db_url, params=params)
if response.status_code != 200:
    raise Exception(f"API request failed: {response.status_code}, {response.text}")

# Extraction des données horaires
data = response.json().get("hourly", {})
time_intervals = data.pop("time", [])

# Création d'un DataFrame directement
hourly_dataframe = pd.DataFrame({
    "time": [datetime.fromisoformat(t) for t in time_intervals],
    **{key: data[key] for key in data.keys() if key in params["hourly"]}
})

# Traitement des colonnes
hourly_dataframe["time"] = pd.to_datetime(hourly_dataframe["time"], errors="coerce")
hourly_dataframe.dropna(subset=["time"], inplace=True)  # Supprimer les valeurs invalides
hourly_dataframe["date"] = hourly_dataframe["time"].dt.date

# Regrouper par jour et calculer les moyennes, puis arrondir à 2 décimales
daily_dataframe = hourly_dataframe.groupby("date").mean().round(2).reset_index()

# Fonction de classification de la qualité de l'air
def classify_air_quality(row):
    thresholds = {
        "pm10": [20, 50, 100, 250],
        "pm2_5": [10, 25, 50, 75],
        "ozone": [50, 100, 180, 240],
        "nitrogen_dioxide": [40, 100, 200, 400],
        "sulphur_dioxide": [20, 80, 250, 500],
        "carbon_monoxide": [4, 9, 12, 15],
        "grass_pollen": 50,
    }
    
    air_quality = "Bonne"
    try:
        row = row.fillna(0)  # Remplacer les NaN par 0
        if row["pm10"] >= thresholds["pm10"][3] or row["pm2_5"] >= thresholds["pm2_5"][3]:
            air_quality = "Dangereuse"
        elif row["pm10"] >= thresholds["pm10"][2] or row["pm2_5"] >= thresholds["pm2_5"][2]:
            air_quality = "Très mauvaise"
        elif row["ozone"] >= thresholds["ozone"][2] or row["nitrogen_dioxide"] >= thresholds["nitrogen_dioxide"][2]:
            air_quality = "Mauvaise"
        elif row["pm10"] >= thresholds["pm10"][1] or row["pm2_5"] >= thresholds["pm2_5"][1]:
            air_quality = "Modérée"
        elif row["grass_pollen"] >= thresholds["grass_pollen"]:
            air_quality = "Acceptable avec risques d'allergie"
        elif row["pm10"] >= thresholds["pm10"][0] or row["pm2_5"] >= thresholds["pm2_5"][0]:
            air_quality = "Acceptable"
    except KeyError as e:
        print(f"Clé manquante : {e}")
    return air_quality

# Appliquer la classification de la qualité de l'air
daily_dataframe["air_quality"] = daily_dataframe.apply(classify_air_quality, axis=1)

# Affichage final des données
# print(daily_dataframe)

