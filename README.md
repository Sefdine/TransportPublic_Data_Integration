# Projet TransportPuplic_Data_Integration

## Description
Avec l'urbanisation croissante et la demande de mobilité, il est essentiel d'avoir une vue claire des opérations de transport. Les données, allant des horaires des bus aux retards, peuvent offrir des perspectives pour améliorer les services et répondre aux besoins des citoyens.

## Tâches Réalisées
- **Conception de l'Architecture Data Lake :**
  - Création d'un espace de noms hiérarchique dans Azure Data Lake Storage Gen2 :
    - `/public_transport_data/raw/` : pour les données CSV brutes.
    - `/public_transport_data/processed/` : pour les données traitées.

- **Intégration de l'Infrastructure Data Lake :**
  - Utilisation d'Azure Databricks avec PySpark pour lire et structurer les données.

- **Processus ETL avec Azure Databricks :**
  - Utilisation de PySpark pour transformer les données, y compris les transformations de date, les calculs de temps et les analyses de retards, de passagers et d'itinéraires.

- **Documentation :**
  - Documentation des transformations, des sources de données et de l'utilisation dans un catalogue.

- **Automatisation des Politiques de Conservation :**
  - Planification d'un cahier pour appliquer les politiques de conservation des données.

- **Génération de données à intervalles de lots (Batch Intervals) :**
  - Création de plusieurs fichiers CSV représentant de nouvelles données pour différentes périodes de la journée.

- **Batch Processing :**
  - Traitement automatisé des données collectées et enregistrement des nouveaux fichiers dans le dossier traité dans le Data Lake.

## Outils Utilisés
- Azure Data Lake Gen2
- Azure Databricks
- Azure Data Factory

## Technologies Utilisées 
 - Notebook (Azure Databricks)
 - Pyspark

## Languages Utilisés :
 - Python
 - Scala
 - SQL
