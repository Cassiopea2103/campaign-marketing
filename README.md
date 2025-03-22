# Projet Data Engineering - E-commerce de Cosmétiques Biologiques

Ce projet vise à améliorer le retour sur investissement (ROI) des campagnes marketing d'une entreprise de cosmétiques biologiques en fusionnant des données provenant de multiples sources (logs web, CRM, plateformes publicitaires) pour obtenir une vision globale des performances marketing.

## 🌟 Fonctionnalités

- Ingestion multi-source de données (Web, CRM, Publicités, Influenceurs)
- Pipelines ETL pour nettoyer, transformer et enrichir les données
- Orchestration automatisée des flux de données
- Tableaux de bord interactifs pour l'analyse des performances marketing
- Environnement technique conteneurisé avec Docker

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

2. Générer les données fictives :
   ```bash
   python -m src.data_generation.web_data
   python -m src.data_generation.crm_data
   python -m src.data_generation.advertising
   ```
