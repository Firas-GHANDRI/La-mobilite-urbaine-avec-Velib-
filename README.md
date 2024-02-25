# Vélib Project

Vélib Project est un pipeline ETL automatisé utilisant des données en temps quasi réel de 1462 stations de vélos en libre-service pour cartographier à tout moment et avec précision la dynamique d’offre et de demande pour la mobilité urbaine alternative en zone urbaine parisienne.

# Description
  
 1. Extraire les données sur Kafka Connect à l’aide de la plateforme Confluent Cloud avec :
   - Source (Producer) : Demandes GET toutes les minutes à l'API avec des données complètes en temps réel sur la localisation de 1 462 stations de vélo. capacité libre, vélos disponibles par type etc.
   - Sink (Consumer) : S3 Bucket pour sauvegarder les fichiers JSON bruts obtenus
     
 2. Phase de transformation : orchestrée par Airflow ou Airbyte
 
 3. Phase de chargement : créer et mettre le fichier csv dans la base de données RDS pour permettre une analyse efficace des requêtes SQL.

![alt text](image-2.png)