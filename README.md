# Projet Data Engineering - E-commerce de Cosmétiques Biologiques

Ce projet vise à améliorer le retour sur investissement (ROI) des campagnes marketing d'une entreprise de cosmétiques biologiques en fusionnant des données provenant de multiples sources (logs web, CRM, plateformes publicitaires) pour obtenir une vision globale des performances marketing.

## 🌟 Fonctionnalités

- Ingestion multi-source de données (Web, CRM, Publicités, Influenceurs)
- Pipelines ETL pour nettoyer, transformer et enrichir les données
- Orchestration automatisée des flux de données
- Tableaux de bord interactifs pour l'analyse des performances marketing
- Environnement technique conteneurisé avec Docker

## 📋 Structure du projet

```bash
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

3. Initialiser l'environnement Docker :
   ```bash
   ./init.bat
   ```

## 🐳 Utilisation de l'environnement Docker

### Accès aux interfaces web

Une fois l'environnement Docker lancé, vous pouvez accéder aux interfaces suivantes :

- **Airflow** : [http://localhost:8088](http://localhost:8088) (identifiants : admin/admin)
- **Spark Master** : [http://localhost:8080](http://localhost:8080)
- **Kafka UI (si configuré)** : [http://localhost:9090](http://localhost:9090)

### Commandes Docker utiles

```bash
# Vérifier l'état des conteneurs
docker-compose ps

# Arrêter l'environnement
docker-compose down

# Redémarrer un service spécifique
docker-compose restart [service]

# Voir les logs d'un service
docker-compose logs [service]
```

## 📊 Génération des données fictives

Le projet inclut des scripts pour générer des données fictives réalistes simulant le fonctionnement d'une entreprise de cosmétiques bio.

### Génération des logs web

```bash
# Mode batch - Génère des données historiques
python src/data_generation/web_data.py --mode batch --start-date 2025-01-01 --end-date 2025-03-28 --events-per-day 5000

# Mode streaming - Génère des données en continu simulant un trafic en temps réel
python src/data_generation/web_data.py --mode stream --events-per-minute 30 --duration 3600

# Mode streaming avec envoi vers Kafka
python src/data_generation/web_data.py --mode stream --output kafka --events-per-minute 30
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

## 🛠️ Architecture technique

L'architecture du projet repose sur les technologies suivantes :

- **Apache Kafka** : Pour l'ingestion de données en temps réel
- **Apache Spark** : Pour le traitement batch et streaming
- **Apache Airflow** : Pour l'orchestration des pipelines de données
- **Snowflake** : Pour le stockage et l'analyse des données
- **dbt** : Pour la modélisation des données
- **Tableau** : Pour la visualisation et les tableaux de bord

## 📋 Backlog et gestion de projet

Le projet est géré en mode agile avec des sprints de 2 semaines. Le backlog du produit est organisé en epics :

- Ingestion et stockage des données
- Transformation et enrichissement
- Orchestration et automatisation
- Dashboard - Performance produits
- Dashboard - Performance marketing
- Documentation et formation

📌 Pour plus de détails, consultez le fichier `docs/product_backlog.md`. 🚀
