-- TP 04 — extension : doublons en entrée et exactly_once_v2.
-- Question 3 du TP : un doublon déjà présent dans le topic d'entrée est-il compté ?
-- Exécution : ./scripts/ksql.sh --file /sql/tp04-extension-doublons.sql
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM IF NOT EXISTS pix_valides (
  transaction_id VARCHAR,
  emitter_tax_id VARCHAR,
  amount DOUBLE
) WITH (KAFKA_TOPIC = 'simulpix.transactions.validated', VALUE_FORMAT = 'JSON');

-- Comptage du TP, inchangé : chaque message compte, doublons compris.
CREATE TABLE IF NOT EXISTS pix_par_emetteur
  WITH (KAFKA_TOPIC = 'simulpix.ksql.pix_par_emetteur', PARTITIONS = 6, REPLICAS = 3) AS
  SELECT emitter_tax_id, COUNT(*) AS nb_pix, ROUND(SUM(amount), 2) AS montant_total
  FROM pix_valides
  GROUP BY emitter_tax_id
  EMIT CHANGES;

-- Déduplication : une seule ligne par transaction_id, quel que soit le nombre de copies.
CREATE TABLE IF NOT EXISTS pix_uniques
  WITH (KAFKA_TOPIC = 'simulpix.ksql.pix_uniques', PARTITIONS = 6, REPLICAS = 3) AS
  SELECT transaction_id,
         LATEST_BY_OFFSET(emitter_tax_id) AS emitter_tax_id,
         LATEST_BY_OFFSET(amount) AS amount
  FROM pix_valides
  GROUP BY transaction_id
  EMIT CHANGES;

-- Comptage calculé sur la table dédupliquée.
CREATE TABLE IF NOT EXISTS pix_par_emetteur_dedup
  WITH (KAFKA_TOPIC = 'simulpix.ksql.pix_par_emetteur_dedup', PARTITIONS = 6, REPLICAS = 3) AS
  SELECT emitter_tax_id, COUNT(*) AS nb_pix, ROUND(SUM(amount), 2) AS montant_total
  FROM pix_uniques
  GROUP BY emitter_tax_id
  EMIT CHANGES;
