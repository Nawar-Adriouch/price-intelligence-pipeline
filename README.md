# Price Tracker - Pipeline de collecte et d'analyse de prix

Ce projet est un pipeline de données conçu pour suivre les prix de produits sur plusieurs APIs e-commerce, normaliser les données, les stocker dans une base MySQL et les exploiter dans un workflow orchestré par Airflow.

L'objectif est simple mais très utile en pratique : surveiller l'évolution des prix de produits sur différents sites et pouvoir répondre à des questions métier comme :

- Quel est le dernier prix connu pour un produit, par source ?
- Le prix augmente-t-il ou baisse-t-il par rapport à la collecte précédente ?
- Quel est le prix moyen sur une période donnée ?
- Quel site est le plus cher ou le moins cher pour un produit ?

Le projet combine :

- des appels HTTP vers des APIs publiques
- une couche de stockage brut (RAW)
- une table de staging pour valider avant insertion
- une table de faits idempotente en MySQL
- un orchestrateur Airflow pour planifier des exécutions régulières

---

## 1. Problème métier résolu

Ce type de projet est appelé Price Intelligence / Price Tracking.

Il est particulièrement utile pour :

- les analystes data qui veulent comparer la concurrence
- les équipes e-commerce qui surveillent les promotions
- les responsables merchandising qui suivent les tendances de prix
- les équipes de data engineering qui veulent construire un pipeline fiable et reproductible

Le projet ne se limite pas à récupérer des prix : il cherche à construire une base analytique propre, traçable et réutilisable.

---

## 2. Sources de données

Le projet est initialement alimenté par 3 sources de produits :

1. DummyJSON
   - Base : https://dummyjson.com
   - Contient des produits avec prix et remise

2. FakeStoreAPI
   - Base : https://fakestoreapi.com
   - API simple et très utilisée dans les démonstrations

3. Platzi / Fake Store API
   - Base : https://api.escuelajs.co/api/v1
   - Similaire à un catalogue de produits avec prix

Ces sources sont configurées dans le schéma de données et mappées avec un identifiant produit externe.

Les produits suivis ne sont pas simplement absorbés tel quel. L'architecture les réconcilie avec un modèle interne :

- dim_product : les produits métier
- dim_source : les sites/API sources
- product_source_map : le mapping entre produit interne et identifiant externe sur chaque source

Cela permet d'avoir une vue uniforme même si chaque API utilise ses propres IDs.

---

## 3. Architecture générale

Le projet suit un schéma ETL/ELT classique :

- Extraction : récupération des produits et prix depuis les APIs
- Raw storage : sauvegarde du payload brut JSON reçu
- Staging : validation et transformation des données avant chargement
- Final load : insertion ou mise à jour dans la table de faits
- Orchestration : planification via Airflow

Le flux logique est donc :

API -> JSON brut -> staging -> table finale (fact_price)

On peut résumer l'architecture comme ceci :

- src/extract.py : appels HTTP et normalisation des payloads
- src/raw_store.py : sauvegarde des réponses JSON brutes sur disque
- src/load.py : connexion MySQL, chargement dans le staging, UPSERT final
- dags/price_pipeline.py : orchestration Airflow
- sql/schema.sql : définition de la structure de la base
- sql/seed.sql : données initiales de référence
- docker-compose.yml : démarrage MySQL + Airflow

---

## 4. Les données et leur modèle

Le modèle de données a été pensé pour répondre à des questions analytiques et faire du tracking historique de prix.

### 4.1 dim_source

Table des sources de données.

Colonnes principales :

- source_id
- name
- base_url

Exemple :

- 1 => dummyjson
- 2 => fakestoreapi
- 3 => platzi

### 4.2 dim_product

Table de dimension produit.

Colonnes principales :

- product_id
- name
- category

Cette table représente le produit métier indépendamment du site source.

### 4.3 product_source_map

Table de correspondance entre produit interne et identifiant externe.

Colonnes principales :

- product_id
- source_id
- external_id

Exemple :

- produit 1 chez dummyjson peut avoir external_id 2
- le même produit 1 chez Platzi peut avoir external_id 17

Cette table est ce qui permet de lier les informations provenant de plusieurs sources à un seul produit métier.

### 4.4 stg_price

La table de staging est une table intermédiaire utilisée pour recevoir les lignes avant de les valider et les insérer dans la table finale.

Elle contient :

- product_id
- source_id
- collected_at
- list_price
- discount_pct
- price
- currency

Elle est vidée à chaque exécution du pipeline pour éviter les accumulations de données dupliquées ou obsolètes.

### 4.5 fact_price

Table de fait, c'est la table analytique centrale.

Colonnes clés :

- product_id
- source_id
- collected_at
- list_price
- discount_pct
- price
- currency
- ingested_at

La clé primaire est :

(product_id, source_id, collected_at)

Cela garantit l'idempotence et empêche l'insertion de doublons pour la même observation (même produit, même source, même créneau de collecte).

### 4.6 rejected_rows

Table de traçabilité des lignes rejetées.

Elle permet de garder les erreurs de validation, sans casser le pipeline :

- source_id
- payload
- reason
- rejected_at

Cela est полез pour diagnostiquer les problèmes de données sans perdre l'information brute.

### 4.7 pipeline_runs

Table de journalisation des exécutions du pipeline.

Elle enregistre :

- run_id
- collected_at
- rows_read
- rows_rejected
- rows_loaded
- status
- started_at
- ended_at

Cette table aide à audit et au monitoring du pipeline.

---

## 5. Idempotence : point central du projet

L'idempotence est l'un des principes les plus importants dans ce projet.

Une exécution du pipeline ne doit pas créer de doublons si elle est relancée plusieurs fois.

### Pourquoi c'est crucial ?

Imaginez que le DAG tourne à 8h et 20h chaque jour. S'il n'est pas idempotent :

- des prix peuvent être insérés deux fois
- les statuts et analyses deviennent faux
- les historiques de prix sont pollués par des doublons

### Solution utilisée dans le projet

Le projet s'appuie sur 3 leviers :

1. Clé d'unicité explicite
   - (product_id, source_id, collected_at)
   - cette clé est la définition d'une observation unique

2. UPSERT en MySQL
   - insert ... on duplicate key update
   - si une ligne existe déjà, elle est mise à jour au lieu d'être dupliquée

3. Table de staging réinitialisée
   - stg_price est vidée à chaque exécution
   - tout est chargé dans la staging avant un transfert final dans fact_price

Cette logique garantit qu'une relance du même créneau ne modifie pas le sens du résultat final.

---

## 6. La couche RAW

La couche RAW est essentielle dans ce type de pipeline.

Avant toute transformation, le programme sauvegarde le JSON brut tel qu'il a été reçu depuis l'API.

Cela sert à plusieurs choses :

- traçabilité complète du flux
- capacité à reprocesser sans refaire des appels HTTP
- facilité de debugging si une transformation a un bug
- reproductibilité des données et audit

Les fichiers RAW sont stockés dans un dossier du type :

- data/raw/2026-10-18/dummyjson_0800.json

Le format est conservé tel quel, sans transformation, afin de pouvoir le rejouer plus tard.

C'est une très bonne pratique pour les pipelines de données, car la couche RAW devient une source de vérité technique.

---

## 7. Extraction et normalisation

Le fichier principal de logique d'extraction est :

- src/extract.py

Il contient :

- http_get_json() : fonction générique de requête HTTP avec retries
- _money() : conversion des montants vers Decimal afin d'éviter les erreurs liées aux floats
- normalize_dummyjson(), normalize_fakestore(), normalize_platzi() : normalisation par source
- fetch_source() : collecte plusieurs IDs externes pour une source

### Points importants

- Les montants sont gérés sous Decimal, pas float
- Les erreurs temporaires HTTP (429, 500, 502, 503, 504) sont retriées avec backoff exponentiel
- La normalisation standardise les structures de données dans un format métier uniforme

Par exemple, chaque source renvoie un payload différent, mais le pipeline les transforme en une ligne cohérente contenant :

- external_id
- title
- list_price
- discount_pct
- price
- currency

---

## 8. Chargement et transform

Le fichier :

- src/load.py

gère les interactions avec MySQL.

### Les fonctions principales sont :

- get_conn() : ouverture de connexion avec retriess et mécanisme de retry
- get_source(conn, source_id) : récupère le nom et l'URL d'une source par ID
- get_external_ids(conn, source_id) : liste les IDs externes associés à la source
- load_staging(conn, rows, source_id, collected_at) : alimente stg_price puis commit
- upsert_final(conn, source_id, collected_at) : copie staging vers fact_price avec UPSERT

### Logique de chargement

Le flux est :

1. lire la mapping des produits externes depuis product_source_map
2. associer chaque objet normalisé à un product_id interne
3. insérer les lignes dans stg_price
4. ensuite transférer les lignes vers fact_price

Cela découpe clairement le pipeline en deux phases :

- validation / préparation
- insertion finale sous transaction

---

## 9. Airflow et orchestration

Le DAG principal est :

- dags/price_pipeline.py

Le DAG est nommé :

- price_pipeline

Il est planifié selon une expression cron :

- 0 8,20 * * *
- donc deux exécutions par jour, à 08:00 et 20:00 UTC

Le DAG utilise :

- un slot logique basé sur le créneau de collecte
- le même horodatage pour les relances du même créneau
- un mécanisme de protection pour ne pas faire tourner plusieurs instants identiques en parallèle

### Ordre des tâches

Le DAG exécute les tâches dans l'ordre suivant :

- extract_raw
- stage
- upsert

Le graphe logique est donc :

extract_raw() >> stage() >> upsert()

### Pourquoi cette structure ?

Parce qu'elle sépare bien les préoccupations :

- extraction de la donnée brute
- préparation de la donnée
- chargement final dans la couche analytique

Cela rend le pipeline plus lisible, traçable et robuste.

---

## 10. Démarrage rapide

### Prérequis

- Docker Desktop ou Docker Engine
- Docker Compose
- Python 3
- accès Internet pour les APIs externes

### Variables d'environnement

Le projet attend un fichier `.env` ou un fichier compatible avec la structure de `.env.exemple`.

Exemple :

```bash
MYSQL_HOST=localhost
MYSQL_PORT=3307
MYSQL_DATABASE=pricetracker
MYSQL_USER=tracker
MYSQL_PASSWORD=change_me
MYSQL_ROOT_PASSWORD=change_me_too
```

### Démarrer la base et Airflow

```bash
docker compose up -d --wait
```

Cela démarre :

- une base MySQL sur le port 3307
- un service Airflow sur le port 8080

### Vérification

```bash
docker compose ps
```

Et pour voir les logs MySQL :

```bash
docker compose logs db --tail 20
```

---

## 11. Exécution locale du pipeline

Le projet contient un script de test et une exécution locale :

- run_local.py

Ce script :

1. récupère les IDs externes des produits associés à une source
2. appelle l'API DummyJSON
3. enregistre le JSON brut dans data/raw
4. charge le staging
5. effectue l'UPSERT dans fact_price

Commande :

```bash
python run_local.py
```

Cela permet de tester le pipeline hors Airflow, sans avoir à passer par l'orchestration complète.

---

## 12. Scripts utiles dans le dépôt

### list_catalog.py

Ce script récupère les catalogues depuis les APIs et écrit le résultat dans :

- catalog.txt

Il sert à explorer les produits disponibles, leurs IDs et leurs prix initiaux.

### test_sources.py

Ce script teste les adaptateurs de normalisation pour les APIs FakeStoreAPI et Platzi.

### catalog.txt

Fichier produit par le script list_catalog.py ; il sert de référence de catalogue et aide à vérifier la cohérence des sources.

---

## 13. Gestion des données de qualité

Le pipeline est conçu pour être fiable et pour minimiser les pertes d'information.

### Bonnes pratiques déjà appliquées

- Decimal pour l'argent
- Gestion de retries sur erreur HTTP
- standardisation des payloads par source
- séparation RAW / staging / final
- clés uniques sur la table de faits
- journalisation des exécutions
- transaction sur le chargement final

### Points de vigilance

- si le fichier schema.sql est modifié, le conteneur MySQL doit parfois être recréé avec :

```bash
docker compose down -v
```

- si la base ne démarre pas, il faut vérifier les variables d'environnement et l'état de santé du service
- si des valeurs sont manquantes ou des IDs inconnus apparaissent, il faut vérifier la table product_source_map

---

## 14. Structure du dépôt

```text
price-tracker/
├── .env.exemple
├── .gitignore
├── docker-compose.yml
├── requirements.txt
├── run_local.py
├── list_catalog.py
├── test_sources.py
├── catalog.txt
├── config/
│   ├── sources.yml
│   └── products.yml
├── dags/
│   └── price_pipeline.py
├── src/
│   ├── extract.py
│   ├── load.py
│   └── raw_store.py
├── sql/
│   ├── schema.sql
│   └── seed.sql
├── data/
│   └── raw/
├── Documentation/
│   ├── Airflow_Cours
│   ├── ELT_ETL
│   ├── Idempotence
│   ├── La couche_RAW
│   └── Les etapes de projet
└── airflow/
```

---

## 15. Cas d'usage métier du projet

Voici quelques questions métiers que la base peut répondre :

- Quel est le dernier prix de chaque produit pour chaque source ?
- Quelle source est la plus chère pour un produit donné ?
- Comment a évolué le prix d'un produit au cours du temps ?
- Y a-t-il eu une baisse significative du prix après une promotion ?
- Quelle est la moyenne de prix sur une période donnée ?

En pratique, cela permet de construire des tableaux de bord, des alertes de prix et des analyses de concurrence.

---

## 16. Pourquoi ce projet est intéressant

Ce projet est un bon exemple de pipeline de données de la vie réelle parce qu'il couvre plusieurs aspects importants :

- sources hétérogènes
- schéma métier normalisé
- table de fait analytique
- couche RAW pour la traçabilité
- orchestration avec Airflow
- idempotence de l'exécution
- transactions et validation de données
- standardisation de la donnée pour l'analyse

C'est exactement le type de projet qui introduit les bonnes pratiques d'architecture de données et de data engineering.

---

## 17. Conclusions

Ce projet illustre une architecture de données robuste pour la collecte, la normalisation et l'analyse de prix sur plusieurs sources.

Il montre surtout qu'un pipeline de données ne se réduit pas à un simple script Python : il faut penser à :

- gouvernance des données
- qualité de la donnée
- traçabilité
- idempotence
- orchestration
- intégrité transactionnelle

C'est là toute la valeur métier et technique de ce projet.

---

## 18. Commandes utiles

```bash
# Démarrer les services

docker compose up -d --wait

# Vérifier les conteneurs

docker compose ps

# Voir les logs MySQL

docker compose logs db --tail 20

# Arrêter la stack

docker compose down

# Tout supprimer (volumes inclus)

docker compose down -v

# Exécuter le pipeline localement

python run_local.py
```

---

## 19. Remerciements et contexte

Ce projet est organisé comme un mini atelier de data engineering autour des sujets :

- extraction de données
- architecture et modélisation analytique
- orchestration de workflows
- fiabilité d'un pipeline ETL
- qualité et traçabilité des données

Il est particulièrement adapté aux personnes qui veulent comprendre comment construire un pipeline de données reproductible, fiable et exploitable en production.

