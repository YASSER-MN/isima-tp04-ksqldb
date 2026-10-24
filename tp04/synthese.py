#!/usr/bin/env python3
"""Génère preuves/tp04/SYNTHESE.md à partir de preuves/tp04/results.tsv."""
import pathlib
import re
import sys

out = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "preuves/tp04")
values = {}
for line in (out / "results.tsv").read_text(encoding="utf-8").splitlines():
    if "\t" in line:
        key, value = line.split("\t", 1)
        values[key] = value.strip()


def v(key, default="?"):
    return values.get(key, default) or default


def as_int(key):
    try:
        return int(values.get(key, ""))
    except ValueError:
        return None


machine = (out / "01-machine.txt").read_text(encoding="utf-8", errors="replace") if (out / "01-machine.txt").exists() else ""
cpu = re.search(r"CPU: (\d+)", machine)
mem = re.search(r"Mem:\s+(\S+)", machine)

print("# TP 04 — synthèse des mesures (générée automatiquement)\n")
print(f"Machine : {cpu.group(1) if cpu else '?'} CPU, {mem.group(1) if mem else '?'} de mémoire. "
      "Détails dans `01-machine.txt`.\n")

print("## Partie 1 — les cinq cas d'usage (600 Pix à 10 Pix/s)\n")
print("| Mesure | Prédit (formule du générateur) | Mesuré | Fichier |")
print("|---|---|---|---|")
print(f"| Pix validés en base | 600 | {v('p1_pg_validated')} | 11 |")
print(f"| Cas 1 : messages ≥ 150 dans `pix_montant_eleve` | 144 | {v('p1_cas1_topic_count')} (base : {v('p1_pg_ge150')}) | 20 |")
print(f"| Cas 2 : somme des `COUNT(*)` de `pix_par_emetteur` | = base | {v('p1_ksql_sum_count')} | 21 |")
print(f"| Cas 2 : mises à jour dans le topic de sortie | ≤ 600 (cache) | {v('p1_cas2_output_updates')} | 21 |")
print(f"| Changelog : messages | ≤ 600 (cache) | {v('p1_changelog_messages')} | 22 |")
print(f"| Repartition : messages | 600 | {v('p1_repartition_messages')} | 22 |")
print(f"| Cas 3 : émetteurs signalés (> 50 / min) | 6 très probable | {v('p1_cas3_emetteurs_signales')} / 6 | 24 |")
print(f"| Cas 4 : lignes enrichies | 600 | {v('p1_cas4_topic_count')} | 25 |")
print(f"| Cas 5 : pull `TAX-0003` | TAX-0003 \\| 100 \\| 10000.0 | {v('p1_cas5_tax0003')} | 27 |")
print(f"\nTopic changelog : `{v('p1_changelog_topic')}`\n")

print("## Partie 2 — crash de ksqlDB (1200 Pix à 20 Pix/s, commit toutes les 10 s, 10 s d'arrêt)\n")
print("| Garantie | SIGKILL après | Base (vérité) | ksqlDB (somme COUNT) | Écart | Avant le crash | Code |")
print("|---|---:|---:|---:|---:|---|---:|")
gaps = {"at_least_once": [], "exactly_once_v2": []}
for guarantee in ("at_least_once", "exactly_once_v2"):
    kills = sorted({int(m.group(1)) for k in values for m in [re.match(rf"p2_{guarantee}_kill(\d+)_base", k)] if m})
    for kill in kills:
        prefix = f"p2_{guarantee}_kill{kill}"
        base, ksql = as_int(f"{prefix}_base"), as_int(f"{prefix}_ksql")
        gap = f"{ksql - base:+d}" if base is not None and ksql is not None else "?"
        if gap != "?":
            gaps[guarantee].append(ksql - base)
        print(f"| `{guarantee}` | {kill} s | {v(prefix + '_base')} | {v(prefix + '_ksql')} | {gap} | {v(prefix + '_avant_crash')} | {v(prefix + '_exit')} |")
for guarantee, series in gaps.items():
    if series:
        print(f"\n- `{guarantee}` : écart min {min(series):+d}, max {max(series):+d} sur {len(series)} essais.")

print("\n## Partie 3 — extension : 5 % de doublons en entrée, ksqlDB en `exactly_once_v2`\n")
print("| Mesure | Valeur |")
print("|---|---:|")
print(f"| Messages dans `raw` | {v('p3_raw_topic')} |")
print(f"| Messages dans `validated` | {v('p3_validated_topic')} |")
print(f"| Pix validés en base (clé primaire) | {v('p3_pg_validated')} |")
print(f"| ksqlDB, comptage du TP (`pix_par_emetteur`) | {v('p3_ksql_naif')} |")
print(f"| ksqlDB, après déduplication (`pix_par_emetteur_dedup`) | {v('p3_ksql_dedup')} |")
naive, dedup, pg = as_int("p3_ksql_naif"), as_int("p3_ksql_dedup"), as_int("p3_pg_validated")
if None not in (naive, pg):
    print(f"\nÉcart comptage du TP − base : {naive - pg:+d}.")
if None not in (dedup, pg):
    print(f"Écart comptage dédupliqué − base : {dedup - pg:+d}.")
