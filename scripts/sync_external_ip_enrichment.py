#!/usr/bin/env python3
"""Sync external IP enrichment for recent flow endpoints into ClickHouse.

Sources:
- Team Cymru bulk whois for ASN, AS name, prefix, registry, country.
- reverse DNS via system resolver for PTR names.

No paid API key required. Designed for POC-scale periodic refresh.
"""
from __future__ import annotations

import argparse
import ipaddress
import json
import socket
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

DB = "flow_poc"
CH = ["docker", "exec", "-i", "clickhouse", "clickhouse-client"]


def ch_query(sql: str, fmt: str = "TabSeparated") -> str:
    q = sql.strip()
    if fmt:
        q += f"\nFORMAT {fmt}"
    p = subprocess.run(CH + ["--query", q], text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or p.stdout.strip())
    return p.stdout


def is_external_ip(s: str) -> bool:
    try:
        ip = ipaddress.ip_address(s)
    except ValueError:
        return False
    return not (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified)


def recent_external_ips(hours: int, limit: int) -> list[str]:
    # Capture both outbound destinations and inbound/public sources. Focus on IPs with traffic first.
    sql = f"""
WITH endpoints AS
(
    SELECT dst_addr AS ip, sum(bytes) AS bytes
    FROM {DB}.flows_enriched
    WHERE received_at >= now() - INTERVAL {int(hours)} HOUR
    GROUP BY dst_addr
    UNION ALL
    SELECT src_addr AS ip, sum(bytes) AS bytes
    FROM {DB}.flows_enriched
    WHERE received_at >= now() - INTERVAL {int(hours)} HOUR
    GROUP BY src_addr
)
SELECT ip
FROM endpoints
GROUP BY ip
ORDER BY sum(bytes) DESC
LIMIT {int(limit) * 3}
"""
    ips = []
    for line in ch_query(sql).splitlines():
        ip = line.strip()
        if ip and is_external_ip(ip) and ip not in ips:
            ips.append(ip)
        if len(ips) >= limit:
            break
    return ips


def cymru_bulk(ips: list[str], timeout: int = 30) -> dict[str, dict]:
    if not ips:
        return {}
    query = "begin\nverbose\n" + "\n".join(ips) + "\nend\n"
    data = ""
    try:
        with socket.create_connection(("whois.cymru.com", 43), timeout=timeout) as s:
            s.sendall(query.encode())
            s.shutdown(socket.SHUT_WR)
            chunks = []
            while True:
                b = s.recv(65536)
                if not b:
                    break
                chunks.append(b)
        data = b"".join(chunks).decode("utf-8", "replace")
    except Exception as e:
        raise RuntimeError(f"Team Cymru whois failed: {e}")
    out = {}
    for line in data.splitlines():
        if not line.strip() or line.lower().startswith("as"):
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 7:
            continue
        asn_s, ip, prefix, cc, registry, allocated, as_name = parts[:7]
        try:
            asn = int(asn_s) if asn_s and asn_s.upper() != "NA" else 0
        except ValueError:
            asn = 0
        out[ip] = {
            "ip": ip,
            "asn": asn,
            "bgp_prefix": prefix,
            "country_code": cc,
            "registry": registry,
            "allocated": allocated,
            "as_name": as_name,
        }
    return out



def dig_txt(name: str, timeout: int = 5) -> list[str]:
    try:
        p = subprocess.run(["dig", "+time=2", "+tries=1", "+short", "TXT", name], text=True, capture_output=True, timeout=timeout)
    except Exception:
        return []
    if p.returncode != 0:
        return []
    vals = []
    for line in p.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith('"') and line.endswith('"'):
            line = line[1:-1]
        vals.append(line.replace('" "', ''))
    return vals


def cymru_dns_one(ip: str, as_name_cache: dict[int, str]) -> dict:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return {"ip": ip, "asn": 0, "as_name": "", "bgp_prefix": "", "country_code": "", "registry": "", "allocated": ""}
    if addr.version != 4:
        return {"ip": ip, "asn": 0, "as_name": "", "bgp_prefix": "", "country_code": "", "registry": "", "allocated": ""}
    rev = ".".join(reversed(ip.split(".")))
    vals = dig_txt(f"{rev}.origin.asn.cymru.com")
    if not vals:
        return {"ip": ip, "asn": 0, "as_name": "", "bgp_prefix": "", "country_code": "", "registry": "", "allocated": ""}
    parts = [x.strip() for x in vals[0].split("|")]
    # ASN | prefix | CC | registry | allocated
    try:
        asn = int(parts[0].split()[0])
    except Exception:
        asn = 0
    as_name = as_name_cache.get(asn, "") if asn else ""
    if asn and not as_name:
        as_vals = dig_txt(f"AS{asn}.asn.cymru.com")
        if as_vals:
            a = [x.strip() for x in as_vals[0].split("|")]
            if len(a) >= 5:
                as_name = a[4]
            elif a:
                as_name = a[-1]
        as_name_cache[asn] = as_name
    return {
        "ip": ip,
        "asn": asn,
        "bgp_prefix": parts[1] if len(parts) > 1 else "",
        "country_code": parts[2] if len(parts) > 2 else "",
        "registry": parts[3] if len(parts) > 3 else "",
        "allocated": parts[4] if len(parts) > 4 else "",
        "as_name": as_name,
    }

def ptr_lookup(ip: str, timeout: float = 1.5) -> str:
    old = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout)
    try:
        name, _, _ = socket.gethostbyaddr(ip)
        return name.rstrip(".")
    except Exception:
        return ""
    finally:
        socket.setdefaulttimeout(old)


def ptr_bulk(ips: list[str], workers: int = 16) -> dict[str, str]:
    out = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(ptr_lookup, ip): ip for ip in ips}
        for fut in as_completed(futs):
            out[futs[fut]] = fut.result()
    return out


def esc(s) -> str:
    if s is None:
        return ""
    return str(s).replace("\\", "\\\\").replace("\t", " ").replace("\n", " ").replace("\r", " ")


def insert_rows(rows: list[dict]) -> None:
    if not rows:
        return
    lines = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for r in rows:
        vals = [
            r.get("ip", ""),
            str(int(r.get("asn") or 0)),
            r.get("as_name", ""),
            r.get("bgp_prefix", ""),
            r.get("country_code", ""),
            r.get("registry", ""),
            r.get("allocated", ""),
            r.get("reverse_dns", ""),
            r.get("source", "team-cymru+ptr"),
            now,
        ]
        lines.append("\t".join(esc(v) for v in vals))
    sql = f"""
INSERT INTO {DB}.external_ip_enrichment
(ip, asn, as_name, bgp_prefix, country_code, registry, allocated, reverse_dns, source, updated_at)
FORMAT TabSeparated
"""
    p = subprocess.run(CH + ["--query", sql], input="\n".join(lines)+"\n", text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or p.stdout.strip())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=24, help="Recent flow window to mine for external IPs")
    ap.add_argument("--limit", type=int, default=500, help="Max external IPs to enrich per run")
    ap.add_argument("--ptr-workers", type=int, default=16)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    result = {"status": "ok", "hours": args.hours, "limit": args.limit, "ips_found": 0, "rows_inserted": 0, "failures": []}
    try:
        ips = recent_external_ips(args.hours, args.limit)
        result["ips_found"] = len(ips)
        try:
            cymru = cymru_bulk(ips)
        except Exception as e:
            result["failures"].append(str(e))
            cymru = {}
        as_name_cache = {}
        ptrs = ptr_bulk(ips, args.ptr_workers)
        rows = []
        for ip in ips:
            r = cymru.get(ip)
            if not r or not r.get("asn"):
                r = cymru_dns_one(ip, as_name_cache)
            r["reverse_dns"] = ptrs.get(ip, "")
            r["source"] = "team-cymru-dns+ptr" if r.get("asn") else "ptr-only"
            rows.append(r)
        insert_rows(rows)
        result["rows_inserted"] = len(rows)
    except Exception as e:
        result["status"] = "failed"
        result["failures"].append(str(e))

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"status: {result['status']}")
        print(f"ips_found: {result['ips_found']}")
        print(f"rows_inserted: {result['rows_inserted']}")
        if result["failures"]:
            print("failures:")
            for f in result["failures"]:
                print(f"- {f}")
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
