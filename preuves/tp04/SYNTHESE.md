# TP 04 — synthèse des mesures (générée automatiquement)

Machine : 4 CPU, 15Gi de mémoire. Détails dans `01-machine.txt`.

## Partie 1 — les cinq cas d'usage (600 Pix à 10 Pix/s)

| Mesure | Prédit (formule du générateur) | Mesuré | Fichier |
|---|---|---|---|
| Pix validés en base | 600 | 600 | 11 |
| Cas 1 : messages ≥ 150 dans `pix_montant_eleve` | 144 | 144 (base : 144) | 20 |
| Cas 2 : somme des `COUNT(*)` de `pix_par_emetteur` | = base | 600 | 21 |
| Cas 2 : mises à jour dans le topic de sortie | ≤ 600 (cache) | 6 | 21 |
| Changelog : messages | ≤ 600 (cache) | 6 | 22 |
| Repartition : messages | 600 | 600 | 22 |
| Cas 3 : émetteurs signalés (> 50 / min) | 6 très probable | 6 / 6 | 24 |
| Cas 4 : lignes enrichies | 600 | 600 | 25 |
| Cas 5 : pull `TAX-0003` | TAX-0003 \| 100 \| 10000.0 | TAX-0003 | 100 | 10000.0 | 27 |

Topic changelog : `_confluent-ksql-simulpix_ksql_query_CTAS_PIX_PAR_EMETTEUR_5-Aggregate-Aggregate-Materialize-changelog`

## Partie 2 — crash de ksqlDB (1200 Pix à 20 Pix/s, commit toutes les 10 s, 10 s d'arrêt)

| Garantie | SIGKILL après | Base (vérité) | ksqlDB (somme COUNT) | Écart | Avant le crash | Code |
|---|---:|---:|---:|---:|---|---:|
| `at_least_once` | 15 s | 1200 | 1686 | +486 | ksqlDB=486 base=486 | 0 |
| `at_least_once` | 25 s | 1200 | 1338 | +138 | ksqlDB=618 base=618 | 0 |
| `at_least_once` | 35 s | 1200 | 1489 | +289 | ksqlDB=884 base=924 | 0 |
| `exactly_once_v2` | 15 s | 1200 | 1200 | +0 | ksqlDB=480 base=486 | 0 |
| `exactly_once_v2` | 25 s | 1200 | 1200 | +0 | ksqlDB=469 base=597 | 0 |
| `exactly_once_v2` | 35 s | 1200 | 1200 | +0 | ksqlDB=656 base=884 | 0 |

- `at_least_once` : écart min +138, max +486 sur 3 essais.

- `exactly_once_v2` : écart min +0, max +0 sur 3 essais.

## Partie 3 — extension : 5 % de doublons en entrée, ksqlDB en `exactly_once_v2`

| Mesure | Valeur |
|---|---:|
| Messages dans `raw` | 1260 |
| Messages dans `validated` | 1260 |
| Pix validés en base (clé primaire) | 1200 |
| ksqlDB, comptage du TP (`pix_par_emetteur`) | 1260 |
| ksqlDB, après déduplication (`pix_par_emetteur_dedup`) | 1200 |

Écart comptage du TP − base : +60.
Écart comptage dédupliqué − base : +0.
