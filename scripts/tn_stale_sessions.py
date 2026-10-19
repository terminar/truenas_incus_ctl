#!/usr/bin/env python3
# /root/tn_stale_sessions.py – TrueNAS-API-Sessions anzeigen, filtern und beenden.
# Standard: Probelauf. Erst mit --apply werden Sessions wirklich beendet.
#
# Beispiele:
#   tn_stale_sessions.py                                  # tote Sessions (ohne TCP-Verbindung) anzeigen
#   tn_stale_sessions.py --older-than 2026-03-04          # nur tote Sessions, die vor dem 04.03.2026 erstellt wurden
#   tn_stale_sessions.py --older-than 2026-03-04 --ignore-tcp --apply
#                                                         # ALLE Sessions vor dem 04.03.2026 beenden
#   tn_stale_sessions.py --all --origin 10.0.0.5          # alle Sessions von 10.0.0.5 anzeigen
#   tn_stale_sessions.py --all --search incus             # Volltextsuche in origin/credentials/id
#   tn_stale_sessions.py --count --all                    # Anzahl aller Sessions nach Herkunfts-IP
import argparse, json, re, subprocess, sys, time
from collections import Counter
from datetime import datetime


def parse_date(value):
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).timestamp()  # lokale Zeit
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(f"ungültiges Datum: {value!r} (erwartet z. B. 2026-03-04 oder '2026-03-04 12:00')")


ap = argparse.ArgumentParser(description="TrueNAS-API-Sessions anzeigen, filtern und beenden")
ap.add_argument("--apply", action="store_true", help="gefundene Sessions wirklich beenden (sonst nur Probelauf)")
ap.add_argument("--older-than", type=parse_date, metavar="DATUM",
                help="nur Sessions, die vor diesem Zeitpunkt erstellt wurden (z. B. 2026-03-04)")
ap.add_argument("--newer-than", type=parse_date, metavar="DATUM",
                help="nur Sessions, die nach diesem Zeitpunkt erstellt wurden")
ap.add_argument("--min-age", type=int, default=600, metavar="SEK",
                help="Sessions, die jünger sind, nie anfassen (Standard: 600)")
ap.add_argument("--origin", metavar="REGEX", help="nur Sessions, deren Herkunft (IP:Port) passt")
ap.add_argument("--search", metavar="REGEX",
                help="Volltextsuche (ohne Groß-/Kleinschreibung) über die ganze Session (origin, credentials, id …)")
ap.add_argument("--ignore-tcp", action="store_true",
                help="auch Sessions mit noch bestehender TCP-Verbindung auswählen")
ap.add_argument("--include-unix", action="store_true",
                help="auch lokale Unix-Socket-Sessions (midclt, Systemdienste) erfassen")
ap.add_argument("--all", action="store_true", help="nur anzeigen: alle passenden Sessions, nicht nur tote (impliziert --ignore-tcp)")
ap.add_argument("--count", action="store_true", help="nur Anzahl der passenden Sessions nach Herkunfts-IP ausgeben")
ap.add_argument("--json", action="store_true", help="passende Sessions als JSON ausgeben")
args = ap.parse_args()
sys.stdout.reconfigure(line_buffering=True)

if args.all and args.apply:
    sys.exit("--all ist nur zum Anzeigen gedacht; zum Beenden --ignore-tcp zusammen mit Filtern benutzen")


def norm(addr):
    addr = addr.strip().replace("[", "").replace("]", "")
    return re.sub(r"^::ffff:", "", addr)


def created_ts(s):
    c = s.get("created_at")
    if isinstance(c, dict):
        c = c.get("$date")
        return c / 1000 if isinstance(c, (int, float)) else None
    if isinstance(c, (int, float)):
        return c / 1000 if c > 1e11 else c
    if isinstance(c, str):
        try:
            return datetime.fromisoformat(c.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def fmt_ts(ts):
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "?"


def cred_summary(s):
    c = s.get("credentials")
    if not isinstance(c, dict):
        return str(c) if c else "?"
    data = c.get("credentials_data") or {}
    user = data.get("user") if isinstance(data.get("user"), dict) else {}
    api_key = data.get("api_key") if isinstance(data.get("api_key"), dict) else {}
    name = data.get("username") or user.get("username") or api_key.get("name")
    return str(c.get("credentials", "?")) + (f" ({name})" if name else "")


out = subprocess.check_output(["ss", "-Htn", "state", "established"], text=True)
peers = {norm(l.split()[3]) for l in out.splitlines() if len(l.split()) >= 4}

sessions = json.loads(subprocess.check_output(
    ["midclt", "call", "auth.sessions", '[["internal","=",false]]']))

origin_re = re.compile(args.origin) if args.origin else None
search_re = re.compile(args.search, re.IGNORECASE) if args.search else None
check_tcp = not (args.ignore_tcp or args.all)
now = time.time()

matched = []
skipped = Counter()
for s in sessions:
    origin = str(s.get("origin") or "")
    if s.get("current"):
        skipped["eigene"] += 1
        continue  # eigene Session nie anfassen
    is_tcp = bool(re.search(r":\d+$", origin))
    if not is_tcp and not args.include_unix:
        skipped["Unix-Socket (--include-unix)"] += 1
        continue  # Unix-Socket-Sessions nur mit --include-unix
    ts = created_ts(s)
    if ts is None or now - ts < args.min_age:
        skipped[f"jünger als {args.min_age}s (--min-age)"] += 1
        continue
    if args.older_than is not None and ts >= args.older_than:
        continue
    if args.newer_than is not None and ts <= args.newer_than:
        continue
    if origin_re and not origin_re.search(origin):
        continue
    if search_re and not search_re.search(json.dumps(s, default=str)):
        continue
    alive = is_tcp and norm(origin) in peers
    if check_tcp and alive:
        continue
    s["_created"], s["_alive"] = ts, alive
    matched.append(s)

matched.sort(key=lambda s: s["_created"])

if args.json:
    print(json.dumps([{k: v for k, v in s.items() if not k.startswith("_")} for s in matched], indent=2, default=str))
    sys.exit(0)

what = "passend" if not check_tcp else "tot (ohne TCP-Verbindung)"
print(f"{len(sessions)} externe Sessions, {len(peers)} TCP-Verbindungen, {len(matched)} {what}", file=sys.stderr)
if skipped:
    print("ausgeblendet: " + ", ".join(f"{n} {why}" for why, n in skipped.items()), file=sys.stderr)

if args.count:
    counts = Counter(re.sub(r":\d+$", "", norm(str(s.get("origin") or "?"))) for s in matched)
    for origin, n in counts.most_common():
        print(f"{n:7d}  {origin}")
    sys.exit(0)

failed = 0
requested = []
for s in matched:
    state = "aktiv" if s["_alive"] else "tot"
    line = f"{s['id']}  {fmt_ts(s['_created'])}  {state:5s}  {s.get('origin')}  {cred_summary(s)}"
    if not args.apply:
        print(("" if args.all else "würde beenden  ") + line)
        continue
    r = subprocess.run(["midclt", "call", "auth.terminate_session", s["id"]],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    result = r.stdout.strip().lower()
    if r.returncode == 0 and result in ("true", ""):
        requested.append(s["id"])
        print("angefordert    " + line)
    else:
        failed += 1
        err = r.stderr.strip().splitlines()
        reason = err[-1] if err else ("Session nicht gefunden" if result == "false" else r.stdout.strip())
        print("FEHLER         " + line + "  " + reason, file=sys.stderr)

if args.apply:
    # terminate_session schließt nur die WebSocket; entfernt wird die Session erst im Close-Callback
    # der Middleware. Bei bereits verwaisten Sessions passiert das nie – deshalb nachprüfen.
    time.sleep(2)
    remaining = {x["id"] for x in json.loads(subprocess.check_output(
        ["midclt", "call", "auth.sessions", '[["internal","=",false]]']))}
    stuck = [i for i in requested if i in remaining]
    removed = len(requested) - len(stuck)
    print(f"{removed} entfernt, {len(stuck)} trotz terminate_session noch vorhanden, {failed} Fehler", file=sys.stderr)
    if stuck:
        print("Hinweis: verwaiste Sessions lassen sich per API nicht entfernen, "
              "nur ein Neustart von middlewared leert die Liste (systemctl restart middlewared).", file=sys.stderr)
    sys.exit(1 if failed or stuck else 0)
