from supabase import create_client, Client
import pandas as pd

class SupabaseController:
  
    def __init__(self, url: str, key: str, table: str):
        self.supabase: Client = create_client(url, key)
        self.table = table


    def get_value(self, row, column, default):
        """
        Retourne une valeur ou une valeur par défaut.
        """
        return row[column] if pd.notnull(row[column]) else default


    def insert_data(self, dataframe: pd.DataFrame):
        """
        Insère les données dans Supabase.
        """
        # Itére chaque ligne dans le DataFrame, ' _ ' ignore les index
        for _, row in dataframe.iterrows():
            data_to_insert = {
                "date": row["date"].isoformat() if pd.notnull(row["date"]) else None,
                "pm10": self.get_value(row, "pm10", 0),
                "pm2_5": self.get_value(row, "pm2_5", 0),
                "carbon_monoxide": self.get_value(row, "carbon_monoxide", 0),
                "nitrogen_dioxide": self.get_value(row, "nitrogen_dioxide", 0),
                "sulphur_dioxide": self.get_value(row, "sulphur_dioxide", 0),
                "ozone": self.get_value(row, "ozone", 0),
                "grass_pollen": self.get_value(row, "grass_pollen", 0),
                "air_quality": self.get_value(row, "air_quality", "Inconnu"),
            }
            try:
                response = self.supabase.table(self.table).insert(data_to_insert).execute()
            except Exception as e:
                print(f"Erreur lors de l'insertion : {e}")
        print(f"Insertion réussie : {response.data}")


    def delete_data(self):
        try:
            response = self.supabase.table(self.table).delete().neq("id", -1).execute()
            print("Tout le contenu de la table 'qualite_air' a été supprimé avec succès.")
        except Exception as e:
            print(f"Erreur lors de l'insertion : {e}")


    def select_data(self):
        try:
            # Dictionnaire pour stocker les DataFrames en fonction des tables
            dataframes = {}

            match self.table:
                case "qualite_air":
                    response = self.supabase.table(self.table).select(
                        "id, date, pm10, pm2_5, carbon_monoxide, nitrogen_dioxide, sulphur_dioxide, ozone, grass_pollen, air_quality"
                    ).execute()
                    if response.data:
                        dataframes["qualite_air"] = pd.DataFrame(response.data)
                        print("Supabase => Données de la table 'qualite_air' récupérées avec succès.")
                    else:
                        print("Aucune donnée trouvée dans la table 'qualite_air'.")

                case "maladies_chroniques_diabete":
                    response = self.supabase.table(self.table).select(
                        "id_diabete, annee, patho_niv1, patho_niv2, patho_niv3, top, region, dept, ntop, npop, prevalence, libelle_classe_age, libelle_sexe"
                    ).execute()
                    if response.data:
                        dataframes["maladies_chroniques_diabete"] = pd.DataFrame(response.data)
                        print("Supabase => Données de la table 'maladies_chroniques_diabete' récupérées avec succès.")
                    else:
                        print("Aucune donnée trouvée dans la table 'maladies_chroniques_diabete'.")

                case "maladies_chroniques_respiratoires":
                    response = self.supabase.table(self.table).select(
                        "id_respiratoire, annee, patho_niv1, patho_niv2, patho_niv3, top, region, dept, ntop, npop, prevalence, libelle_classe_age, libelle_sexe"
                    ).execute()
                    if response.data:
                        dataframes["maladies_chroniques_respiratoires"] = pd.DataFrame(response.data)
                        print("Supabase => Données de la table 'maladies_chroniques_respiratoires' récupérées avec succès.")
                    else:
                        print("Aucune donnée trouvée dans la table 'maladies_chroniques_respiratoires'.")

                case "maladies_chroniques_cardiologiques":
                    response = self.supabase.table(self.table).select(
                        "id_cardiologique, annee, patho_niv1, patho_niv2, patho_niv3, top, region, dept, ntop, npop, prevalence, libelle_classe_age, libelle_sexe"
                    ).execute()
                    if response.data:
                        dataframes["maladies_chroniques_cardiologiques"] = pd.DataFrame(response.data)
                        print("Supabase => Données de la table 'maladies_chroniques_cardiologiques' récupérées avec succès.")
                    else:
                        print("Aucune donnée trouvée dans la table 'maladies_chroniques_cardiologiques'.")

                case "donnees_patient":
                    response = self.supabase.table(self.table).select(
                        "id_patient, age, sexe, glycemie, mode_de_vie, alimentation, frequence_cardiaque, pression_systolique, pression_diastolique"
                    ).execute()
                    if response.data:
                        dataframes["donnees_patient"] = pd.DataFrame(response.data)
                        print("Supabase => Données de la table 'donnees_patient' récupérées avec succès.")
                    else:
                        print("Aucune donnée trouvée dans la table 'donnees_patient'.")

                case "mode_de_vie":
                    response = self.supabase.table(self.table).select(
                        "id_patient, sexe, age, poids, taille, region, saison, fastfood_sem, visite_generaliste_12mois, visite_specialiste_12mois, fume, IMC, sed_moy_jour, act_phy_tot_heb, IPAQ, freq_cereales, freq_fruit_leg, freq_proteines"
                    ).execute()
                    if response.data:
                        dataframes["mode_de_vie"] = pd.DataFrame(response.data)
                        print("Supabase => Données de la table 'donnees_patient' récupérées avec succès.")
                    else:
                        print("Aucune donnée trouvée dans la table 'donnees_patient'.")

                # Ajouter d'autres cas ici pour chaque table
                case _:
                    print(f"Table '{self.table}' non reconnue.")

            # Retourner le dictionnaire contenant les DataFrames
            print(dataframes)
            return dataframes

        except Exception as e:
            print(f"Erreur lors de la récupération des données : {e}")
            return {}
