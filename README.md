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

### Composants principaux:
- **Sources de données**: Logs web (CSV/JSON), Base CRM, Données publicitaires (Google, Facebook, Instagram)
- **Ingestion**: Apache Kafka pour l'intégration des données en temps réel
- **Orchestration**: Apache Airflow pour la gestion et la planification des workflows
- **Traitement**: 
  - Apache Spark Batch pour le traitement des lots de données
  - Apache Spark Streaming pour le traitement en temps réel
- **Stockage**:
  - Data Lake (Bronze/Silver/Gold) pour les données brutes et transformées
  - Snowflake comme data warehouse analytique
  - dbt pour la modélisation des données
- **Visualisation**: Tableau/Looker pour les tableaux de bord analytiques
- **Monitoring**: Prometheus et Grafana pour la surveillance des pipelines

Le tout est déployé dans un environnement conteneurisé avec Docker pour assurer la portabilité et la reproductibilité.

### Architecture diagram

![E-commerce Data Architecture](docs/architecture/ecommerce_architecture.png)

*Architecture diagram showing the data flow from source systems through processing to analytics*

### Génération du diagramme d'architecture

Pour visualiser l'architecture du système, vous pouvez générer un diagramme à partir du code source:

```bash
# Installez d'abord le package diagrams
pip install diagrams

# Générez le diagramme (depuis le répertoire racine du projet)
python docs/architecture/diagram_code.py
```

Le diagramme sera généré dans le répertoire `docs/architecture/` avec le nom `ecommerce_architecture.png`.

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
   git clone https://github.com/votre-username/cosmetics-data-engineering.git
   cd cosmetics-data-engineering
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
docker-compose up -d

# Vérifier l'état des conteneurs
docker-compose ps
```

### 2. Accès aux interfaces utilisateur

- **Airflow**: http://localhost:8080 (utilisateur: airflow, mot de passe: airflow)
- **Kafka UI**: http://localhost:8081
- **Spark UI**: http://localhost:4040
- **Grafana**: http://localhost:3000 (utilisateur: admin, mot de passe: admin)

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

## 📋 Backlog et gestion de projet

Le projet est géré en mode agile avec des sprints de 2 semaines. Le backlog du produit est organisé en epics :
1. Ingestion et stockage des données
2. Transformation et enrichissement
3. Orchestration et automatisation
4. Dashboard - Performance produits
5. Dashboard - Performance marketing
6. Documentation et formation

Pour plus de détails, consultez le fichier `docs/product_backlog.md`.