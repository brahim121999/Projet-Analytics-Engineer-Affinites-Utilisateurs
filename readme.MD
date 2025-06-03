## Projet Analytics Engineer — Affinités Utilisateurs (DBT + DuckDB)

## 🎯 Objectif

Ce projet a pour but de construire un pipeline analytique complet permettant de déterminer les **affinités des utilisateurs** avec différents niveaux de granularité publicitaire :

- **Catégorie**
- **Sous-catégorie**
- **Marque**
- **Thématique métier**

Il s’appuie sur des événements utilisateurs (visites et commandes) pour générer une **table finale silver** utilisable pour du ciblage publicitaire, de la segmentation ou de la recommandation.

---

## 🗂️ Données utilisées

- `users.parquet` : infos utilisateur
- `orders.parquet` : historique de commandes
- `visits.parquet` : événements de navigation
- `campaigns.parquet` : caractéristiques campagnes marketing
- `thematics_mapping.csv` : correspondance secteur ↔ thématique

---

## ⚙️ Setup du projet
## ✅ 1. Créer et activer un environnement virtuel
```bash
python -m venv .venv
.venv\Scripts\activate
```

## ✅ 2. Installer les dépendances
```bash
pip install -r requirements.txt
```

## ⚙️ Étapes d’exécution
## 🔹 1. Charger les données dans DuckDB
```bash
python setup.py
```
Cela crée une base veepee_ads.db avec les tables raw_*.

## 🔹 2. Initialiser et exécuter DBT
Lancer depuis le dossier du projet DBT :

```bash
cd veepee_ads_dbt/veepe_ads
```
Puis exécuter tous les modèles :

```bash
dbt clean
dbt compile
dbt run
```

## 📊 Exploration des résultats
Après avoir exécuté le pipeline DBT, vous pouvez explorer les résultats stockés dans la base veepee_ads.db à l’aide du script :

```bash
python explore_db.py
```
Ce script interroge directement les tables créées par DBT dans DuckDB et affiche les vues analytiques.
