## Description du projet
Dans ce fichier, nous allons traiter des données générées aléatoirement à l'aide d'un script Python que vous pouvez trouver dans le répertoire courant. Ce projet scolaire a pour but de nous familiariser avec Azure Databricks lors de nos premiers pas.

L'objectif principal de ce projet est d'intégrer des données provenant de fichiers CSV stockés dans un service Azure Blob Storage. À l'aide de notebooks dans Databricks, nous allons mettre en place un processus ETL en utilisant le framework Apache Spark (pyspark). Les données transformées seront chargées dans le même service Blob Storage, dans un dossier nommé "Processed".

Nous allons automatiser les politiques de rétention des données en supprimant les données brutes déjà traitées et en archivant les données traitées à long terme.

De plus, nous allons générer des données par lots (Batch Intervals) à l'aide du script joint à ce document, puis nous implémenterons un système de traitement par lots (Batch Processing).