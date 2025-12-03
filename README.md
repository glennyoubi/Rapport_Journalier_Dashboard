# Résumé Hebdomadaire (Streamlit)

Dashboard Streamlit pour analyser un rapport hebdo Excel, filtrer, visualiser et exporter les actions.

## Installation
- Python 3.10+ recommandé.
- `pip install -r requirements.txt`.

## Lancer l’app
- `streamlit run app.py`
- Charger le `.xlsx`, choisir la plage de feuilles, cliquer **Lancer / Mettre à jour**.
- Option *Mise à jour auto* pour relancer si le fichier ou les feuilles changent.
- Mode *Analyste* (toggle dans la sidebar) dévoile les tables avancées.

## Processus de données (schéma rapide)
1) Lecture des feuilles Excel, normalisation des colonnes, nettoyage.
2) Construction des vues :
   - `actions_daily` : brut par jour (réservé aux traitements internes).
   - `actions_consistent` : historique consolidé, statuts cohérents jour après jour.
   - `actions_latest` : photo actuelle (une ligne par action).
   - Dérivés : `running_actions`, `postponed_actions`, `ended_actions`, `equipment_downtime`, `transitions`.
3) Filtres (dates, métier, zone, plateforme, texte/TAG) appliqués sur les vues.
4) Visualisations et exports Excel/CSV/Parquet.

## Graphiques (ce qu’ils montrent)
- Évolution statuts (aire empilée) : dynamique quotidienne en cours/reporté/terminé.
- Répartition statuts (camembert) : snapshot actuel.
- Statuts par jour et métier (barres) : qui fait quoi, chaque jour.
- Volume quotidien + cumul : rythme global des actions.
- Cumul terminées : cadence de clôture.
- Répartition statuts par zone (barres empilées) : charge et avancement par zone.
- Heatmap jour × zone : pics d’activité / zones chaudes.
- Pareto des causes (métier par défaut) : catégories dominantes + cumul%.
- Âge médian des actions ouvertes : détecte l’enlisement.
- Temps en “en cours” (boxplot) : dispersion des durées d’exécution.
- Indisponibilités équipements (table) : TAG, début/fin, jours d’indispo.
- TAG avec actions ouvertes (table) : charge résiduelle par TAG.
- Top risques (cartes) : zone et TAG les plus chargés parmi les actions ouvertes.

## Tables visibles
- Par défaut : `running_actions`, `ended_actions`, `postponed_actions`, `equipment_downtime`, `actions_latest`.
- Mode analyste : ajoute `actions_consistent`, `transitions`.
- Export CSV/Parquet disponible pour chaque table ; la clé unique `action_key` reste présente dans les exports (masquée à l’affichage).

## Tests
- `pytest` (unitaire sur le parsing Excel et le rollup des statuts).
