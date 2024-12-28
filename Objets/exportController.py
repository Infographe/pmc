import pandas as pd
import os

class ExportController:
    def export_dataframe(self, dataframe, filename, format='csv', **kwargs):
        """Exporte un DataFrame vers un fichier.

        Args:
            self: Référence à l'instance de la classe.
            dataframe (pd.DataFrame): Le DataFrame à exporter.
            filename (str): Le nom du fichier.
            format (str, optional): Format d'export (csv ou json). Defaults to 'csv'.
            **kwargs: Arguments supplémentaires pour les méthodes to_csv ou to_json.
        """

        if format == 'csv':
            dataframe.to_csv(filename, index=False, **kwargs)
        elif format == 'json':
            dataframe.to_json(filename, orient='records', indent=4, **kwargs)
        else:
            raise ValueError(f"Format d'export non supporté: {format}")

    def export_dataframes(self, dataframes, output_dir, format='csv', **kwargs):
        """Exporte les DataFrames au format spécifié.

        Args:
            self: Référence à l'instance de la classe.
            dataframes (dict): Dictionnaire de DataFrames.
            output_dir (str): Répertoire de sortie.
            format (str, optional): Format d'export (csv ou json). Defaults to 'csv'.
            **kwargs: Arguments supplémentaires pour les méthodes to_csv et to_json.
        """

        os.makedirs(output_dir, exist_ok=True)

        for table_name, dataframe in dataframes.items():
            if dataframe is not None and not dataframe.empty:
                filename = os.path.join(output_dir, f"{table_name}.{format}")
                try:
                    self.export_dataframe(dataframe, filename, format=format)
                    print(f"Les données de la table '{table_name}' ont été exportées vers {filename}.")
                except (OSError, ValueError) as e:
                    print(f"Erreur lors de l'export de {table_name}: {e}")
            else:
                print(f"Aucune donnée à exporter pour la table '{table_name}'.")

