#!/usr/bin/env bash
# TP 04 — exécution complète du parcours ksqlDB et collecte des preuves.
#
# Usage : bash tp04/run-tp04.sh            (depuis la racine du dépôt)
# Sorties : preuves/tp04/*.txt, preuves/tp04/results.tsv, preuves/tp04/SYNTHESE.md
#
# Parties :
#   1. Les 5 cas d'usage de infra/ksqldb/tp14-pix.sql sur 600 Pix à 10 Pix/s
#   2. Crash de ksqlDB : at_least_once et exactly_once_v2, 3 instants de crash chacun
#   3. Extension : doublons en entrée (5 %) sous exactly_once_v2, puis déduplication
set -uo pipefail
cd "$(dirname "$0")/.."

OUT="${TP04_OUT:-preuves/tp04}"
mkdir -p "$OUT"
RES="$OUT/results.tsv"
LOG="$OUT/00-journal.txt"
: > "$RES"
: > "$LOG"
BS="kafka-1:29092,kafka-2:29092,kafka-3:29092"
KILL_TIMES="${TP04_KILL_TIMES:-15 25 35}"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOG"; }
rec() { printf '%s\t%s\n' "$1" "$2" >> "$RES"; log "RESULTAT $1 = $2"; }
dc() { docker compose "$@"; }
dck() { docker compose --profile ksqldb "$@"; }
kq() { python3 tp04/tp04_ksql.py "$@"; }
psql_q() { dc exec -T postgres psql -U simulpix -d simulpix -At -F ' | ' -c "$1" 2>/dev/null; }
kbin() { local tool="$1"; shift; dc exec -T kafka-1 /opt/kafka/bin/"$tool" --bootstrap-server "$BS" "$@"; }
topic_count() {
  kbin kafka-get-offsets.sh --command-config /config/client.properties --topic "$1" --time -1 2>/dev/null \
    | awk -F: 'NF>=3 {s+=$NF} END {print s+0}'
}
topics() { kbin kafka-topics.sh --command-config /config/client.properties --list 2>/dev/null; }
peek() { # peek TOPIC N : affiche N messages avec partition, offset et clé
  kbin kafka-console-consumer.sh --consumer.config /config/client.properties --topic "$1" \
    --from-beginning --timeout-ms 8000 --max-messages "$2" \
    --property print.partition=true --property print.offset=true --property print.key=true \
    --property key.separator=" | " 2>&1 | grep -v -E 'TimeoutException|Error processing message|^\s+at ' || true
}
wait_ksql() {
  local n=120
  until curl -fsS http://127.0.0.1:8088/info >/dev/null 2>&1; do
    n=$((n - 1)); [ "$n" -gt 0 ] || { log "ksqldb-server ne répond pas"; return 1; }
    sleep 2
  done
}
pg_total() {
  local v
  v="$(psql_q "select (select count(*) from validated_transactions) + (select count(*) from rejected_transactions)")"
  [[ "$v" =~ ^[0-9]+$ ]] && echo "$v" || echo 0
}
wait_pg() { # wait_pg N TIMEOUT : attend validated + rejected >= N, stable sur 3 lectures
  local target="$1" budget="${2:-300}" last="" stable=0 v=0
  while [ "$budget" -gt 0 ]; do
    v="$(pg_total)"
    if [ "$v" -ge "$target" ] && [ "$v" = "$last" ]; then
      stable=$((stable + 1)); [ "$stable" -ge 3 ] && { echo "$v"; return 0; }
    else
      stable=0
    fi
    last="$v"; sleep 3; budget=$((budget - 3))
  done
  echo "$v"; return 1
}
wait_ksql_sum() { # wait_ksql_sum TABLE ATTENDU TIMEOUT : attend que la somme des COUNT soit stable
  local table="$1" want="$2" budget="${3:-180}" last="" stable=0 v=""
  while [ "$budget" -gt 0 ]; do
    v="$(kq sum "$table" NB_PIX --value 2>/dev/null || echo UNAVAILABLE)"
    if [ "$v" = "$last" ] && [[ "$v" =~ ^[0-9]+$ ]] && [ "$v" -ge "$want" ]; then
      stable=$((stable + 1)); [ "$stable" -ge 3 ] && { echo "$v"; return 0; }
    else
      stable=0
    fi
    last="$v"; sleep 5; budget=$((budget - 5))
  done
  echo "$v"; return 1
}
section() { printf '\n===== %s =====\n' "$1"; }
wipe_ksql() { # repart d'un ksqlDB vierge : conteneur supprimé, topics internes et de sortie effacés
  # Nécessaire entre les parties : un TERMINATE ALL sur un serveur chargé peut dépasser
  # le délai du command topic, et ksql-semantique.sh s'arrête alors avant de mesurer.
  dck rm -sf ksqldb-server >/dev/null 2>&1 || true
  local pattern
  for pattern in '_confluent-ksql-simulpix_ksql_.*' 'simulpix_ksql_.*' 'simulpix\.ksql\..*'; do
    kbin kafka-topics.sh --command-config /config/client.properties --delete --topic "$pattern" >/dev/null 2>&1 || true
  done
  sleep 10
}

# ---------------------------------------------------------------------------
log "TP 04 — démarrage"
{
  section "Machine"; date -u; uname -a; echo "CPU: $(nproc)"; free -h; df -h / .
  section "Docker"; docker version; docker compose version
} > "$OUT/01-machine.txt" 2>&1

log "Démarrage de la plateforme (./start.sh)"
chmod +x start.sh stop.sh scripts/*.sh infra/kafka/scripts/*.sh infra/scripts/*.sh 2>/dev/null || true
# Un clone neuf n'a pas ce répertoire (ignoré par Git) : generate-secrets.sh échoue sans lui.
mkdir -p infra/kafka/secrets
./start.sh > "$OUT/02-start.txt" 2>&1 || { log "ÉCHEC start.sh"; tail -40 "$OUT/02-start.txt"; exit 1; }
dc ps > "$OUT/03-docker-ps.txt" 2>&1

# ---------------------------------------------------------------------------
# PARTIE 1 — les 5 cas d'usage
# ---------------------------------------------------------------------------
log "PARTIE 1 — flux nominal 600 Pix à 10 Pix/s"
./scripts/run-scenario.sh nominal 600 10 > "$OUT/10-run-scenario.txt" 2>&1
pg="$(wait_pg 600 300)"; rec p1_pg_total "$pg"
{
  section "Vérité terrain PostgreSQL (après drainage)"
  echo "validated_transactions : $(psql_q 'select count(*) from validated_transactions')"
  echo "rejected_transactions  : $(psql_q 'select count(*) from rejected_transactions')"
  section "Par émetteur (count, somme des montants)"
  psql_q "select emitter_tax_id, count(*), sum(amount) from validated_transactions group by 1 order by 1"
  section "Pix validés avec montant >= 150"
  psql_q "select count(*) from validated_transactions where amount >= 150"
  section "Messages dans le topic validated (offsets de fin, sans transaction)"
  topic_count simulpix.transactions.validated
} > "$OUT/11-postgres-verite.txt"
rec p1_pg_validated "$(psql_q 'select count(*) from validated_transactions')"
rec p1_pg_ge150 "$(psql_q 'select count(*) from validated_transactions where amount >= 150')"
rec p1_validated_topic "$(topic_count simulpix.transactions.validated)"

log "Création des requêtes ksqlDB (tp14-pix.sql, garantie par défaut)"
./scripts/ksql.sh --file /sql/tp14-pix.sql > "$OUT/12-creation-requetes.txt" 2>&1
wait_ksql
p1_valid="$(psql_q 'select count(*) from validated_transactions')"
ks="$(wait_ksql_sum pix_par_emetteur "${p1_valid:-600}" 180)"; rec p1_ksql_sum_count "$ks"

./scripts/ksql.sh -e "SHOW STREAMS; SHOW TABLES; SHOW QUERIES; DESCRIBE pix_par_emetteur EXTENDED;" \
  > "$OUT/13-show-describe.txt" 2>&1
{ section "Topics après création des requêtes"; topics | sort; } > "$OUT/14-topics.txt"
docker stats --no-stream > "$OUT/15-docker-stats.txt" 2>&1

log "Cas 1 — filtre amount >= 150"
{
  section "Requête push sur pix_montant_eleve (depuis le début)"
  kq push "SELECT * FROM pix_montant_eleve EMIT CHANGES;" 15 | head -8
  section "Nombre de messages dans simulpix.ksql.pix_montant_eleve"
  topic_count simulpix.ksql.pix_montant_eleve
} > "$OUT/20-cas1-filtre.txt" 2>&1
rec p1_cas1_topic_count "$(topic_count simulpix.ksql.pix_montant_eleve)"
rec p1_cas1_push_rows "$(kq push 'SELECT * FROM pix_montant_eleve EMIT CHANGES;' 15 | head -1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["row_count"])')"

log "Cas 2 — agrégation par émetteur"
{
  section "Requêtes pull sur pix_par_emetteur, une par émetteur"
  kq sum pix_par_emetteur NB_PIX
  section "Mises à jour publiées dans le topic de sortie simulpix.ksql.pix_par_emetteur"
  topic_count simulpix.ksql.pix_par_emetteur
} > "$OUT/21-cas2-agregation.txt" 2>&1
rec p1_cas2_output_updates "$(topic_count simulpix.ksql.pix_par_emetteur)"

log "Changelog de pix_par_emetteur"
changelog="$(topics | grep -i 'PIX_PAR_EMETTEUR' | grep -i 'changelog' | head -1)"
repart="$(topics | grep -i 'PIX_PAR_EMETTEUR' | grep -i 'repartition' | head -1)"
{
  section "Topic changelog"; echo "${changelog:-introuvable}"
  [ -n "$changelog" ] && { echo "messages : $(topic_count "$changelog")"; section "20 premiers messages (partition, offset, clé, valeur)"; peek "$changelog" 20; }
  section "Topic de repartition"; echo "${repart:-introuvable}"
  [ -n "$repart" ] && { echo "messages : $(topic_count "$repart")"; section "5 premiers messages"; peek "$repart" 5; }
} > "$OUT/22-changelog-repartition.txt" 2>&1
rec p1_changelog_topic "${changelog:-introuvable}"
[ -n "$changelog" ] && rec p1_changelog_messages "$(topic_count "$changelog")"
[ -n "$repart" ] && rec p1_repartition_messages "$(topic_count "$repart")"
kq explain CTAS_PIX_PAR_EMETTEUR > "$OUT/23-topologie-pix_par_emetteur.txt" 2>&1

log "Cas 3 — fenêtre d'une minute"
{
  section "Table pix_rafales_emetteur (HAVING COUNT(*) > 50), requête push depuis le début"
  kq push "SELECT * FROM pix_rafales_emetteur EMIT CHANGES;" 15
  section "Toutes les fenêtres, sans seuil (requête push transitoire)"
  kq push "SELECT emitter_tax_id, WINDOWSTART AS fenetre_debut, COUNT(*) AS nb FROM pix_valides WINDOW TUMBLING (SIZE 1 MINUTE) GROUP BY emitter_tax_id EMIT CHANGES;" 20
} > "$OUT/24-cas3-fenetre.txt" 2>&1
python3 - "$OUT/24-cas3-fenetre.txt" >> "$OUT/24-cas3-fenetre.txt" <<'PY'
import json, sys
lines = open(sys.argv[1], encoding="utf-8").read().split("===== Toutes les fenêtres")
def last_per_window(block):
    final = {}
    for line in block.splitlines():
        line = line.strip()
        if line.startswith("[") and line.endswith("]"):
            row = json.loads(line)
            if len(row) >= 3:
                final[(row[0], row[1])] = row[-1]
    return final
print("\n===== Synthèse : valeur finale par (émetteur, fenêtre) =====")
for title, block in (("Seuil > 50 (table)", lines[0].split("=====", 2)[-1]), ("Sans seuil", lines[1] if len(lines) > 1 else "")):
    final = last_per_window(block)
    print(f"-- {title} : {len(final)} fenêtre(s)")
    for (emitter, start), value in sorted(final.items()):
        print(f"   {emitter}  fenêtre_début={start}  nb={value}")
flagged = {k[0] for k in last_per_window(lines[0].split("=====", 2)[-1])}
print(f"Émetteurs signalés : {len(flagged)} / 6")
PY
rec p1_cas3_emetteurs_signales "$(grep -oE 'Émetteurs signalés : [0-9]+' "$OUT/24-cas3-fenetre.txt" | grep -oE '[0-9]+' | head -1)"

log "Cas 4 — jointure"
{
  section "5 premières lignes de pix_valides_enrichis"
  kq push "SELECT * FROM pix_valides_enrichis EMIT CHANGES;" 15 | head -6
  section "Nombre de messages dans simulpix.ksql.pix_valides_enrichis"
  topic_count simulpix.ksql.pix_valides_enrichis
  section "Topics de repartition de la jointure"
  topics | grep -i 'PIX_VALIDES_ENRICHIS' || echo "aucun"
} > "$OUT/25-cas4-jointure.txt" 2>&1
rec p1_cas4_topic_count "$(topic_count simulpix.ksql.pix_valides_enrichis)"
kq explain CSAS_PIX_VALIDES_ENRICHIS > "$OUT/26-topologie-jointure.txt" 2>&1

log "Cas 5 — requête pull"
./scripts/ksql.sh -e "SELECT * FROM pix_par_emetteur WHERE emitter_tax_id = 'TAX-0003';" > "$OUT/27-cas5-pull.txt" 2>&1
kq pull "SELECT * FROM pix_par_emetteur WHERE emitter_tax_id = 'TAX-0003';" >> "$OUT/27-cas5-pull.txt" 2>&1
rec p1_cas5_tax0003 "$(kq pull "SELECT * FROM pix_par_emetteur WHERE emitter_tax_id = 'TAX-0003';" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(" | ".join(str(x) for x in d["rows"][0]) if d.get("rows") else d)')"

# ---------------------------------------------------------------------------
# PARTIE 2 — crash de ksqlDB, deux garanties, trois instants de crash
# ---------------------------------------------------------------------------
for guarantee in at_least_once exactly_once_v2; do
  for kill_after in $KILL_TIMES; do
    file="$OUT/30-crash-${guarantee}-kill${kill_after}s.txt"
    log "PARTIE 2 — $guarantee, SIGKILL après ${kill_after} s"
    wipe_ksql
    ./scripts/ksql-semantique.sh "$guarantee" 1200 20 "$kill_after" 10 > "$file" 2>&1
    code=$?
    base="$(grep -E 'Pix validés en base' "$file" | awk -F: '{gsub(/ /,"",$NF); print $NF}')"
    ksql="$(grep -E 'Pix comptés par ksqlDB' "$file" | awk -F: '{gsub(/ /,"",$NF); print $NF}')"
    before="$(grep -E 'avant le crash' "$file" | sed 's/.*avant le crash : //')"
    rec "p2_${guarantee}_kill${kill_after}_base" "${base:-?}"
    rec "p2_${guarantee}_kill${kill_after}_ksql" "${ksql:-?}"
    rec "p2_${guarantee}_kill${kill_after}_avant_crash" "${before:-?}"
    rec "p2_${guarantee}_kill${kill_after}_exit" "$code"
  done
done

# ---------------------------------------------------------------------------
# PARTIE 3 — extension : doublons en entrée sous exactly_once_v2
# ---------------------------------------------------------------------------
log "PARTIE 3 — ksqlDB en exactly_once_v2, 1200 Pix avec 5 % de doublons injectés"
wipe_ksql
SIMULPIX_KSQL_PROCESSING_GUARANTEE=exactly_once_v2 dck up -d --no-deps --force-recreate ksqldb-server \
  > "$OUT/40-extension-preparation.txt" 2>&1
wait_ksql
SIMULPIX_KAFKA_AMBIGUOUS_ACK_DUPLICATE_PERCENT=5 ./scripts/run-scenario.sh nominal 1200 20 >> "$OUT/40-extension-preparation.txt" 2>&1
pg3="$(wait_pg 1200 300)"
./scripts/ksql.sh --file /sql/tp04-extension-doublons.sql > "$OUT/41-extension-requetes.txt" 2>&1
p3_valid="$(psql_q 'select count(*) from validated_transactions')"
naive="$(wait_ksql_sum pix_par_emetteur "${p3_valid:-1200}" 240)"
dedup="$(wait_ksql_sum pix_par_emetteur_dedup 1 240)"
{
  section "Garantie ksqlDB"; docker inspect ksqldb-server --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -E 'PROCESSING_GUARANTEE|COMMIT_INTERVAL'
  section "Compteurs"
  echo "Messages dans raw (offsets de fin)       : $(topic_count simulpix.transactions.raw)"
  echo "Messages dans validated (offsets de fin) : $(topic_count simulpix.transactions.validated)"
  echo "Pix validés en base (clé primaire)       : $p3_valid"
  echo "Pix en base, validés + rejetés           : $pg3"
  echo "ksqlDB pix_par_emetteur (naïf)           : $naive"
  echo "ksqlDB pix_par_emetteur_dedup            : $dedup"
  section "Détail par émetteur (naïf)"; kq sum pix_par_emetteur NB_PIX
  section "Détail par émetteur (dédupliqué)"; kq sum pix_par_emetteur_dedup NB_PIX
  section "Exemple de doublon dans validated (même transaction_id deux fois)"
  peek simulpix.transactions.validated 60 | grep -oE '"transaction_id": ?"[^"]+"' | sort | uniq -c | awk '$1>1' | head -5
} > "$OUT/42-extension-resultats.txt" 2>&1
rec p3_raw_topic "$(topic_count simulpix.transactions.raw)"
rec p3_validated_topic "$(topic_count simulpix.transactions.validated)"
rec p3_pg_validated "$p3_valid"
rec p3_ksql_naif "$naive"
rec p3_ksql_dedup "$dedup"

./scripts/ksql.sh stop >/dev/null 2>&1 || true
python3 tp04/synthese.py "$OUT" > "$OUT/SYNTHESE.md"
log "TP 04 — terminé"
cat "$OUT/SYNTHESE.md"
