#!/usr/bin/env python3
"""Petit client REST ksqlDB pour la collecte des preuves du TP 04.

Bibliothèque standard uniquement. Usage :
  tp04_ksql.py stmt  "SHOW QUERIES;"                  -> JSON brut de /ksql
  tp04_ksql.py pull  "SELECT ... WHERE k = 'x';"      -> en-tête + lignes
  tp04_ksql.py push  "SELECT ... EMIT CHANGES;" IDLE  -> lignes reçues jusqu'à IDLE s sans nouveauté
  tp04_ksql.py sum   TABLE COLONNE                    -> somme de COLONNE pour TAX-0001..TAX-0006
  tp04_ksql.py explain PREFIXE_ID                     -> topologie Kafka Streams de la requête
"""
import json
import re
import socket
import sys
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8088"
HEADERS = {
    "Content-Type": "application/vnd.ksql.v1+json",
    "Accept": "application/vnd.ksql.v1+json",
}
EMITTERS = [f"TAX-{n:04d}" for n in range(1, 7)]


def _post(path, payload, timeout):
    request = urllib.request.Request(
        BASE + path, data=json.dumps(payload).encode(), headers=HEADERS
    )
    return urllib.request.urlopen(request, timeout=timeout)


def stmt(sql):
    try:
        with _post("/ksql", {"ksql": sql, "streamsProperties": {}}, 60) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as error:
        return {"http_error": error.code, "body": error.read().decode()}


def _columns(header):
    schema = (header or {}).get("schema", "")
    return re.findall(r"`([^`]+)`", schema)


def pull(sql, retries=30):
    """Requête pull ; réessaie tant que l'état n'est pas encore disponible."""
    last_error = None
    for _ in range(retries):
        try:
            with _post("/query", {"ksql": sql, "streamsProperties": {}}, 30) as response:
                body = response.read().decode()
        except urllib.error.HTTPError as error:
            last_error = error.read().decode()
            time.sleep(1)
            continue
        except OSError as error:
            last_error = str(error)
            time.sleep(1)
            continue
        try:
            records = json.loads(body)
        except json.JSONDecodeError:
            last_error = body
            time.sleep(1)
            continue
        records = records if isinstance(records, list) else [records]
        header, rows, errors = None, [], []
        for record in records:
            if not isinstance(record, dict):
                continue
            if "header" in record:
                header = record["header"]
            elif "row" in record:
                rows.append(record["row"].get("columns"))
            elif record.get("@type") == "statement_error" or "message" in record:
                errors.append(record.get("message"))
        if errors:
            last_error = errors[0]
            if "not available" in str(last_error) or "Materialized data" in str(last_error):
                time.sleep(1)
                continue
            return {"error": last_error}
        return {"columns": _columns(header), "rows": rows}
    return {"error": last_error or "indisponible"}


def push(sql, idle_seconds):
    """Requête push depuis le début du topic ; s'arrête après IDLE s sans nouvelle ligne."""
    props = {"ksql.streams.auto.offset.reset": "earliest"}
    header, rows, raw_errors = None, [], []
    try:
        response = _post("/query", {"ksql": sql, "streamsProperties": props}, idle_seconds)
    except urllib.error.HTTPError as error:
        return {"error": error.read().decode()}
    except OSError as error:
        return {"error": str(error)}
    try:
        while True:
            try:
                line = response.readline()
            except (socket.timeout, TimeoutError):
                break
            if not line:
                break
            text = line.decode().strip()
            text = text.lstrip("[").rstrip("]").rstrip(",").strip()
            if not text:
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError:
                continue
            if "header" in record:
                header = record["header"]
            elif "row" in record:
                rows.append(record["row"].get("columns"))
            elif "finalMessage" in record:
                break
            elif "errorMessage" in record or "message" in record:
                raw_errors.append(record)
                break
    finally:
        response.close()
    result = {"columns": _columns(header), "rows": rows}
    if raw_errors:
        result["error"] = raw_errors[0]
    return result


def sum_column(table, column):
    total, per_key = 0, {}
    for emitter in EMITTERS:
        result = pull(f"SELECT * FROM {table} WHERE emitter_tax_id = '{emitter}';")
        if "error" in result:
            return {"total": None, "error": result["error"], "per_key": per_key}
        columns = [name.upper() for name in result["columns"]]
        if not result["rows"]:
            per_key[emitter] = None
            continue
        row = result["rows"][0]
        if column.upper() not in columns:
            return {"total": None, "error": f"colonne {column} absente: {columns}"}
        value = row[columns.index(column.upper())]
        per_key[emitter] = row
        total += int(value)
    return {"total": total, "per_key": per_key}


def explain(prefix):
    queries = stmt("SHOW QUERIES;")
    try:
        ids = [q["id"] for q in queries[0]["queries"]]
    except (KeyError, IndexError, TypeError):
        return {"error": queries}
    matches = [qid for qid in ids if qid.upper().startswith(prefix.upper())]
    if not matches:
        return {"error": f"aucune requête {prefix}* parmi {ids}"}
    described = stmt(f"EXPLAIN {matches[-1]};")
    try:
        desc = described[0]["queryDescription"]
    except (KeyError, IndexError, TypeError):
        return {"error": described}
    return {
        "id": matches[-1],
        "statement": desc.get("statementText"),
        "sources": desc.get("sources"),
        "sinks": desc.get("sinks"),
        "topology": desc.get("topology"),
    }


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2
    command = argv[1]
    if command == "stmt":
        print(json.dumps(stmt(argv[2]), indent=2, ensure_ascii=False))
    elif command == "pull":
        print(json.dumps(pull(argv[2]), indent=2, ensure_ascii=False))
    elif command == "push":
        result = push(argv[2], int(argv[3]) if len(argv) > 3 else 15)
        print(json.dumps({"columns": result.get("columns"), "row_count": len(result.get("rows", [])),
                          "error": result.get("error")}, ensure_ascii=False))
        for row in result.get("rows", []):
            print(json.dumps(row, ensure_ascii=False))
    elif command == "sum":
        result = sum_column(argv[2], argv[3])
        if len(argv) > 4 and argv[4] == "--value":
            print(result["total"] if result.get("total") is not None else "UNAVAILABLE")
        else:
            print(json.dumps(result, indent=2, ensure_ascii=False))
    elif command == "explain":
        result = explain(argv[2])
        if "topology" in result:
            print(f"Requête : {result['id']}")
            print(f"Sources : {result['sources']}")
            print(f"Sinks   : {result['sinks']}")
            print(f"SQL     : {result['statement']}")
            print("\nTopologie Kafka Streams :\n")
            print(result["topology"])
        else:
            print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(__doc__)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
