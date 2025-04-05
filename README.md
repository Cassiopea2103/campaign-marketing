# Projet Data Engineering - E-commerce de Cosmétiques Biologiques

Ce projet vise à améliorer le retour sur investissement (ROI) des campagnes marketing d'une entreprise de cosmétiques biologiques en fusionnant des données provenant de multiples sources (logs web, CRM, plateformes publicitaires) pour obtenir une vision globale des performances marketing.

## 🌟 Fonctionnalités

- Ingestion multi-source de données (Web, CRM, Publicités, Influenceurs)
- Pipelines ETL pour nettoyer, transformer et enrichir les données
- Orchestration automatisée des flux de données
- Tableaux de bord interactifs pour l'analyse des performances marketing
- Environnement technique conteneurisé avec Docker

## 🏗️ Architecture du système

Ce projet utilise une architecture moderne de data engineering basée sur le modèle Lambda (traitement batch et temps réel) avec les composants suivants:

### 📊 Flux de Données

#### ⭐ Streaming Data Path (Logs Web)

- **Flux** : Logs Web → Kafka → Spark Streaming → Data Lake  
- **Objectif** : Optimisé pour le traitement des événements en temps réel  
- **Avantages** : Exploite les capacités de streaming de Kafka pour gérer des événements à haut débit  

#### ⭐ Batch Data Path (CRM & Publicités)

- **Flux** : Données CRM/Publicités → Spark Batch → Data Lake  
- **Objectif** : Ingestion directe dans Spark pour un traitement en lot  
- **Avantages** : Évite l'utilisation inutile de Kafka pour des chargements de données périodiques  

---

### 🛠️ Composants de l'architecture

#### 🔹 Sources de données

- **Logs Web** : Événements de comportement utilisateur, pages vues, conversions  
- **Données CRM** : Profils clients, historique des commandes  
- **Données Publicitaires** : Performance des campagnes, dépenses, impressions  

#### 🔹 Couche de Traitement

- **Kafka** : Broker de messages en temps réel gérant les données de streaming  
- **Spark Streaming** : Traitement des événements en temps réel depuis Kafka  
- **Spark Batch** : Traitement périodique des données CRM et publicitaires  

#### 🔹 Couche de Stockage

- **MinIO** : Stockage objet servant de Data Lake  
  - **Zone Bronze** : Données brutes non modifiées (JSON, CSV)  
  - **Zone Silver** : Données nettoyées et validées  
  - **Zone Gold** : Données agrégées prêtes pour l'analyse  

#### 🔹 Data Warehouse

- **dbt** : Modélisation et transformation des données  
- **Snowflake** : Exécution de requêtes analytiques et Business Intelligence  

#### 🔹 Analytique & Visualisation

- **Metabase** : Tableaux de bord interactifs et reporting  

#### 🔹 Orchestration & Monitoring

- **Airflow** : Ordonnancement et orchestration des pipelines de données  
- **Prometheus/Grafana** : Surveillance des performances système et qualité des données  

Le tout est déployé dans un environnement conteneurisé avec Docker pour assurer la portabilité et la reproductibilité.

### Architecture diagram

![system_architecture](https://github.com/user-attachments/assets/bd8e66f0-697e-4249-8ad4-2cf674b90779)


*Architecture diagram showing the data flow from source systems through processing to analytics*

### Génération du diagramme d'architecture

Pour visualiser l'architecture du système, vous pouvez générer un diagramme à partir du code source:

```bash
# Installez d'abord le package diagrams
pip install diagrams

# Générez le diagramme (depuis le répertoire racine du projet)
python docs/architecture/diagram_code.py
```

Le diagramme sera généré dans le répertoire `docs/architecture/` avec le nom `system_architecture.png`.

## 📋 Structure du projet

```
project-root/
│
├── data/                      # Données générées et transformées
│   ├── raw/                   # Données brutes
│   │   ├── web/               # Logs de navigation
│   │   ├── crm/               # Données clients et commandes
│   │   └── advertising/       # Données des campagnes publicitaires
│   │
│   ├── processed/             # Données traitées
│   └── warehouse/             # Données finales pour analyse
│
├── src/                       # Code source
│   ├── data_generation/       # Scripts de génération de données fictives
│   │   ├── web_data.py        # Génération des logs web
│   │   ├── crm_data.py        # Génération des données CRM
│   │   └── advertising.py     # Génération des données publicitaires
│   │
│   ├── etl/                   # Pipelines d'extraction, transformation et chargement
│   ├── orchestration/         # DAGs Airflow et configuration
│   └── dashboard/             # Configurations pour tableaux de bord
│
├── docs/                      # Documentation
│   └── architecture/          # Diagrammes d'architecture
│       └── diagram_code.py    # Code pour générer les diagrammes
│
├── docker/                    # Fichiers Docker pour les différents services
├── docker-compose.yml         # Orchestration des conteneurs
├── requirements.txt           # Dépendances Python
└── README.md                  # Ce fichier
```

## 🚀 Installation et démarrage

### Prérequis

- Docker et Docker Compose
- Python 3.8+
- Git

### Installation

1. Cloner le dépôt :
   ```bash
   git clone https://github.com/Cassiopea2103/campaign-marketing.git
   cd campaign-marketing
   ```

2. Installer les dépendances :
   ```bash
   pip install -r requirements.txt
   ```

## 📊 Génération des données fictives

Le projet inclut des scripts pour générer des données fictives réalistes simulant le fonctionnement d'une entreprise de cosmétiques bio.

### Génération des logs web

```bash
# Mode batch - Génère des données historiques
python src/data_generation/web_data.py --mode batch --start-date 2025-01-01 --end-date 2025-03-28 --events-per-day 5000

# Mode streaming - Génère des données en continu simulant un trafic en temps réel
python src/data_generation/web_data.py --mode stream --events-per-minute 30 --duration 3600
```

### Génération des données CRM

```bash
python src/data_generation/crm_data.py --start-date 2025-01-01 --end-date 2025-03-28 --frequency daily --initial-customers 500
```

### Génération des données publicitaires

```bash
python src/data_generation/advertising.py --start-date 2025-01-01 --end-date 2025-03-28 --frequency daily
```

### Génération complète des données

Pour générer l'ensemble des données pour le projet, exécutez les scripts dans cet ordre :

```bash
# 1. Données CRM (clients et commandes)
python src/data_generation/crm_data.py --start-date 2025-01-01 --end-date 2025-03-28

# 2. Données publicitaires (campagnes marketing)
python src/data_generation/advertising.py --start-date 2025-01-01 --end-date 2025-03-28

# 3. Logs web (comportement utilisateur)
python src/data_generation/web_data.py --mode batch --start-date 2025-01-01 --end-date 2025-03-28
```

Les données générées seront stockées dans le dossier `data/raw/` avec la structure suivante :
- `data/raw/crm/` : Données clients et commandes
- `data/raw/advertising/` : Données des campagnes publicitaires
- `data/raw/web/` : Logs de navigation

## 🛠️ Exécution des composants

### 1. Démarrage de l'environnement Docker

```bash
# Démarrer tous les services
.\init.bat

# Vérifier l'état des conteneurs
docker ps
```

### 2. Accès aux interfaces utilisateur

- **Airflow**: http://localhost:8088 (utilisateur: admin, mot de passe: admin)
- **Kafka UI**: http://localhost:9482
- **Spark UI**: http://localhost:8080
- **MinIO**: http://localhost:9001 (utilisateur: minioadmin, mot de passe: minioadmin)
- **Grafana**: http://localhost:3001 (utilisateur: admin, mot de passe: admin)
- **Metabase**: http://localhost:3000

### 3. Exécution des pipelines de traitement

```bash
# Se connecter au conteneur Airflow
docker exec -it airflow_webserver bash

# Activer un DAG
airflow dags unpause cosmetics_daily_etl
```

### 4. Visualisation des résultats

Une fois les pipelines exécutés, vous pouvez accéder aux tableaux de bord via l'interface Tableau/Looker, où vous pourrez analyser:
- La performance des produits par canal
- L'efficacité des campagnes marketing
- Le comportement des utilisateurs
- L'impact des promotions sur les ventes


# Pipeline d'Analyse des Campagnes Marketing

Ce répertoire contient l'implémentation du pipeline de données pour l'analyse des performances des campagnes marketing pour l'entreprise e-commerce de cosmétiques biologiques.

## Vue d'ensemble

Le pipeline de données intègre les logs web, les données CRM et les données publicitaires pour fournir des insights complets sur l'efficacité des campagnes marketing, le comportement client et le ROI. Le pipeline suit une architecture Lambda avec des chemins séparés pour les données en streaming et par lots.

## Composants du Pipeline

### Sources de données
- **Logs Web** : Événements de comportement utilisateur, vues de pages, conversions
- **Données CRM** : Profils clients, historique des commandes
- **Données Publicitaires** : Métriques de performance des campagnes Google Ads, réseaux sociaux et influenceurs

### Flux de données
1. **Couche d'Ingestion** : Les données brutes sont ingérées dans la couche Bronze
2. **Couche de Transformation** : Les données sont nettoyées et standardisées dans la couche Silver
3. **Couche d'Agrégation** : Les analyses prêtes à l'emploi sont créées dans la couche Gold
4. **Couche Entrepôt** : Les données sont chargées dans l'entrepôt de données pour le reporting

### Fonctionnalités clés
- Modélisation d'attribution multi-touch
- Segmentation client RFM
- Analyse des performances marketing
- Analyse des performances produits
- Suivi du cycle de vie client

## Structure des répertoires

```
src/
├── ingestion/                # Scripts d'ingestion de données
│   ├── streaming/            # Ingestion de données en streaming (logs web)
│   │   ├── ingest_web_logs.py
│   │   └── web_logs_to_bronze.py
│   └── batch/                # Ingestion de données par lots (CRM, publicité)
│       ├── crm_to_bronze.py
│       └── advertising_to_bronze.py
├── transformation/           # Scripts de transformation de données
│   ├── clean_web_logs.py
│   ├── clean_crm_data.py
│   ├── create_attribution_models.py
│   ├── marketing_performance.py
│   └── customer_segments.py
└── orchestration/            # DAGs Airflow
    └── dags/
        ├── data_ingestion_dag.py
        ├── data_transformation_dag.py
        ├── data_aggregation_dag.py
        ├── warehouse_loading_dag.py
        └── monitoring_dag.py
```

## Description des DAGs

### DAG d'Ingestion de Données
S'exécute quotidiennement pour ingérer des données brutes de diverses sources dans la couche Bronze :
- Vérifie les nouveaux fichiers de logs web et les charge dans Kafka
- Traite les logs web depuis Kafka vers la couche Bronze
- Charge les données CRM et publicitaires directement dans la couche Bronze

### DAG de Transformation de Données
Transforme les données de la couche Bronze vers la couche Silver :
- Nettoie et standardise les logs web, les données CRM et les données publicitaires
- Crée des profils clients enrichis
- Joint les événements web avec les données clients
- Crée des modèles d'attribution marketing

### DAG d'Agrégation de Données
Crée des données agrégées prêtes à l'emploi dans la couche Gold :
- Métriques de performance marketing par canal et campagne
- Analyses de performance produit
- Modèles de segmentation client
- Prévisions et tendances des ventes

### DAG de Chargement de l'Entrepôt
Charge les données de Gold vers l'entrepôt de données :
- Exécute les modèles dbt pour l'analyse
- Crée les tables et vues finales pour le reporting
- Rafraîchit les tableaux de bord

### DAG de Surveillance
Surveille la santé du pipeline et la qualité des données :
- Vérifie la fraîcheur des données
- Valide l'exhaustivité des données
- Surveille les métriques d'exécution du pipeline
- Génère des rapports de qualité des données

## Utilisation

### Prérequis
- Docker et Docker Compose
- Python 3.8+
- Apache Airflow
- Apache Spark
- Apache Kafka

### Exécution du Pipeline
1. Démarrer l'environnement :
   ```
   docker-compose up -d
   ```

2. Accéder à Airflow pour gérer les DAGs :
   ```
   http://localhost:8088
   ```

3. Activer les DAGs dans l'ordre suivant :
   - `data_ingestion_dag`
   - `data_transformation_dag`
   - `data_aggregation_dag`
   - `warehouse_loading_dag`

## Modèles de Données

### Couche Bronze (Données Brutes)
- `web_logs` : Événements web bruts du site e-commerce
- `customers` : Données brutes des profils clients
- `orders` : Données brutes des commandes
- `google_ads` : Données brutes des campagnes Google Ads
- `social_ads` : Données brutes des publicités sur réseaux sociaux
- `influencer` : Données brutes du marketing d'influence

### Couche Silver (Données Nettoyées)
- `web_logs` : Données d'événements web nettoyées
- `web_sessions` : Données de sessions web agrégées
- `customers` : Profils clients nettoyés
- `orders` : Données de commandes nettoyées
- `order_items` : Lignes de commande détaillées
- `marketing_attribution` : Données d'attribution multi-touch
- `channel_performance` : Performance des canaux avec attribution

### Couche Gold (Prêt pour l'Analyse)
- `marketing_performance` : Performance marketing par canal, campagne et période
- `customer_segments` : Segmentation client pour le marketing ciblé
- `rfm_segments` : Segmentation RFM (Récence, Fréquence, Montant)
- `lifecycle_segments` : Données des étapes du cycle de vie client

## Notes d'Implémentation

- Les logs web sont traités via Kafka pour la gestion d'événements en temps réel
- Les données CRM et publicitaires utilisent un traitement par lots direct
- Les modèles d'attribution incluent premier contact, dernier contact, linéaire et décroissance temporelle
- La segmentation client inclut RFM, affinité produit et étapes du cycle de vie
- La qualité des données est surveillée à chaque étape du pipeline

## 📋 Backlog et gestion de projet

Le projet est géré en mode agile avec des sprints de 2 semaines. Le backlog du produit est organisé en epics :
1. Ingestion et stockage des données
2. Transformation et enrichissement
3. Orchestration et automatisation
4. Dashboard - Performance produits
5. Dashboard - Performance marketing
6. Documentation et formation

Pour plus de détails, consultez le fichier `docs/product_backlog.md`.
