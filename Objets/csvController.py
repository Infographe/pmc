class CSVController:
    def __init__(self, filename: str):
        self.filename = filename

    def export_to_csv(self, dataframe):
        """
        Exporte le DataFrame au format CSV.
        """
        dataframe.to_csv(self.filename, index=False)
        print(f"Les données ont été sauvegardées dans {self.filename}.")

    def export_table_data(self):
        dataframes = self

        if not dataframes:
            print("Aucune donnée récupérée pour les tables.")
            return

        for table_name, dataframe in dataframes:
            if dataframe is not None and not dataframe.empty:
                filename = f"{table_name}.csv"
                dataframe.to_csv(filename, index=False)
                print(f"Les données de la table '{table_name}' ont été exportées vers {filename}.")
            else:
                print(f"Aucune donnée à exporter pour la table '{table_name}'.")
