# Projet Python

Il s'agit d'un projet de fin de module pour un mastère en Développement Web Full-Stack avec LiveCampus. [Le projet va scraper un site web pour extraire des données au format CSV, les insérer dans une base de données, ainsi que réaliser des manipulations de données.].

## Table des matières

- [Prérequis](#prérequis)
- [Installation](#installation)
- [Contributeurs](#contributeurs)

## Prérequis

Avant de pouvoir exécuter ce projet, assurez-vous d'avoir les éléments suivants installés :

- Python 3.6 ou version ultérieure

## Installation

1. Clonez ce dépôt :
  ```bash
  git clone https://github.com/SurreAymeric/Projet-Python-MDV.git
  ```
2. Installation des dépendances pour le bon déroulement du projet
   ```bash
   cd votre-projet
   ```
   Création de l'environnement :
   Windows :
   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```
   Linux :
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
   MacOs:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```
   Il va ensuite falloir installer les packages
   ```bash
   pip install -r requirements.txt --upgrade
   ```
2. Utilisation :
   ```bash
   python main.py
   ```
## Contributeurs
Ce projet a été réalisé par Aymeric Surre et Matthieu Fanget


## Question Alogo :

1:Nombre d'épisode de chaque chaine
cours_pytho_5421=> SELECT channel, COUNT(*) as episode_count
cours_pytho_5421-> FROM episode
cours_pytho_5421-> GROUP BY channel
cours_pytho_5421-> ORDER BY episode_count DESC;

      channel       | episode_count
--------------------+---------------
 Netflix            |           109
 Disney+            |            30
 Prime Video        |            27
 ...

2:Nombre d'épisode de chaque pays

cours_pytho_5421=> SELECT country, COUNT(*) as episode_count
cours_pytho_5421-> FROM episode
cours_pytho_5421-> GROUP BY country
cours_pytho_5421-> ORDER BY episode_count DESC;

       country        | episode_count
----------------------+---------------
 Etats-Unis           |           353
 France               |            76
 Canada               |            63
 ...

 3: Les mots les plus utilisé
 ('the', 82), ('of', 32), ('de', 24)