# TP 04 — exécution automatisée (ksqlDB)

Dépôt dérivé de [`wiguyot/ISIMA-TP-kafka-2026`](https://github.com/wiguyot/ISIMA-TP-kafka-2026) (simul-pix, commit `085b839`).
Seuls `tp04/`, `infra/ksqldb/tp04-extension-doublons.sql` et `.github/workflows/tp04.yml` ont été ajoutés ; la plateforme n'est pas modifiée.

## Ce qui est exécuté

`bash tp04/run-tp04.sh` (depuis la racine) :

1. **Cinq cas d'usage** de `infra/ksqldb/tp14-pix.sql` sur 600 Pix à 10 Pix/s : filtre, agrégation, fenêtre d'une minute, jointure, requête pull. Le script capture aussi le changelog, le topic de repartition et la topologie Kafka Streams (`EXPLAIN`).
2. **Crash de ksqlDB** avec `scripts/ksql-semantique.sh` : `at_least_once` puis `exactly_once_v2`, SIGKILL après 15, 25 et 35 s.
3. **Extension** : 5 % de doublons injectés en entrée, ksqlDB en `exactly_once_v2`, puis un comptage dédupliqué par `transaction_id`.

## Résultats

Les preuves sont écrites dans `preuves/tp04/`. `SYNTHESE.md` résume les mesures et pointe vers les fichiers bruts.

Sur GitHub Actions, le workflow `.github/workflows/tp04.yml` lance le même script sur `ubuntu-latest`, puis publie `preuves/tp04/` sur la branche `results`.
