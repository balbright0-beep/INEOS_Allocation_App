#!/usr/bin/env python3
"""INEOS Vehicle Allocation Tool — data extraction and HTML generation.

Reads the Master File, extracts vehicle and dealer data,
computes allocation metrics, and injects them into the allocation template HTML.
"""

import io
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import msoffcrypto
from pyxlsb import open_workbook


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_PATH = os.path.join(BASE_DIR, "Master File V14 binary.xlsb")
TEMPLATE_PATH = os.path.join(BASE_DIR, "allocation_template.html")
OUTPUT_DIR = os.path.join(os.path.dirname(BASE_DIR), "INEOS_Allocation_Output")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "INEOS_Allocation_Tool.html")
DECRYPTED_PATH = os.path.join(BASE_DIR, "master_decrypted.xlsb")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def vi(x):
    if x is None:
        return 0
    try:
        return int(float(x))
    except Exception:
        return 0


def vf(x):
    if x is None:
        return 0.0
    try:
        return float(x)
    except Exception:
        return 0.0


def safe_str(x):
    return str(x).strip() if x else ""


def serial_to_date(s):
    if not s:
        return None
    try:
        return datetime(1899, 12, 30) + timedelta(days=int(float(s)))
    except Exception:
        return None


def decrypt_master(path, pw="INEOS26", output_path=None):
    out_path = output_path or DECRYPTED_PATH
    print(f"  Decrypting {os.path.basename(path)}...")
    with open(path, "rb") as f:
        office_file = msoffcrypto.OfficeFile(f)
        office_file.load_key(password=pw)
        buf = io.BytesIO()
        office_file.decrypt(buf)
        buf.seek(0)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "wb") as out:
            out.write(buf.getvalue())
    return out_path


def read_sheet(wb, name, max_rows=99999):
    rows = []
    with wb.get_sheet(name) as sheet:
        for i, row in enumerate(sheet.rows()):
            if i >= max_rows:
                break
            rows.append([c.v for c in row])
    return rows


def clean_dealer_name(name):
    n = safe_str(name).upper()
    for suffix in [" INEOS GRENADIER", " INEOS", " GRENADIER"]:
        n = n.replace(suffix, "")
    return n.strip()


def replace_const(html, name, data):
    payload = json.dumps(data, separators=(",", ":"))
    pattern = rf"(const {name}=).*?;"
    replacement = f"const {name}={payload};"
    html2, count = re.subn(pattern, replacement.replace("\\", "\\\\"), html, count=1, flags=re.DOTALL)
    if count == 0:
        print(f"  WARNING: {name} not found in HTML")
    else:
        print(f"  {name}: replaced ({len(payload):,} chars)")
    return html2


# ---------------------------------------------------------------------------
# Market mapping (reused from dashboard)
# ---------------------------------------------------------------------------

def build_mkt_map(wb):
    rows = []
    with wb.get_sheet("RBM Assignments") as sheet:
        for row in sheet.rows():
            rows.append([c.v for c in row])

    market_map = {}
    for r in rows[5:]:
        if r and len(r) > 5 and r[3] and r[5]:
            name = safe_str(r[3]).replace(" INEOS Grenadier", "").replace(" INEOS", "").strip()
            market = safe_str(r[5])
            market_map[name] = market
            market_map[name.upper()] = market

    extras = {
        "Mossy SD": "Western", "MOSSY SD": "Western",
        "Mossy TX": "Central", "MOSSY TX": "Central",
        "RTGT": "Western",
        "Crown Dublin": "Northeast", "CROWN DUBLIN": "Northeast",
        "Sewell SA": "Central", "SEWELL SAN ANTONIO": "Central",
        "Orlando": "Southeast", "ORLANDO": "Southeast",
        "Roseville": "Western", "ROSEVILLE": "Western",
        "Mossy San Diego": "Western", "MOSSY SAN DIEGO": "Western",
    }
    market_map.update(extras)
    return market_map


def lookup_mkt(market_map, name):
    n = name.strip()
    if n in market_map:
        return market_map[n]
    if n.upper() in market_map:
        return market_map[n.upper()]
    for k, v in market_map.items():
        if n.upper() in k.upper() or k.upper() in n.upper():
            return v
    return ""


# ---------------------------------------------------------------------------
# Vehicle extraction
# ---------------------------------------------------------------------------

STATUS_MAP = {
    "8. sold": "Sold",
    "7. dealer stock": "Dealer Stock",
    "6. in-transit": "In-Transit to Dealer",
    "5. arrived": "At Americas Port",
    "4. departed": "On Water",
    "3. built": "Built at Plant",
    "2. in production": "In Production",
    "1. preplan": "Preplanning",
    "(blank)": "Awaiting Status",
    "planned": "Planned for Transfer",
    "vehicle written": "Written Off",
}


def classify_status(raw):
    low = raw.lower()
    for key, label in STATUS_MAP.items():
        if key in low:
            return label
    return "Awaiting Status"


def parse_my(material):
    if "27" in material:
        return "MY27"
    if "26" in material:
        return "MY26"
    if "25" in material:
        return "MY25"
    if "24" in material:
        return "MY24"
    return ""


def parse_body(material):
    return "QM" if "quartermaster" in material.lower() else "SW"


def build_vehicles_compact(export_rows, mkt_map):
    """Build compact indexed vehicle data to minimize HTML size.

    Returns (V_DATA, V_DICT):
      V_DATA = list of arrays [vin, idx_my, idx_body, idx_trim, ...]
      V_DICT = dict of lookup arrays keyed by field name
    """
    # Indexed string fields: (field_key, column_index_or_lambda, label)
    INDEXED_FIELDS = [
        ("my",           None),    # computed
        ("body",         None),    # computed
        ("trim",         19),
        ("pack",         20),
        ("color",        21),
        ("seats",        22),
        ("roof",         23),
        ("safari",       24),
        ("wheels",       25),
        ("tyres",        26),
        ("frame",        27),
        ("tow",          28),
        ("heated_seats", 29),
        ("diff_locks",   30),
        ("ladder",       31),
        ("aux_battery",  32),
        ("aux_switch",   33),
        ("carpet",       34),
        ("compass",      35),
        ("centre_diff",  36),
        ("emerg_safety", 37),
        ("floor_trim",   38),
        ("winch",        39),
        ("utility_rails",40),
        ("privacy_glass",41),
        ("air_intake",   42),
        ("smokers",      43),
        ("spare_wheel",  44),
        ("front_tow",    45),
        ("bump_strips",  46),
        ("steering",     47),
        ("wheel_locks",  48),
        ("sound",        49),
        ("status",       None),    # computed
        ("channel",      14),
        ("dealer",       None),    # computed
        ("stock_cat",    9),
        ("plant",        50),
        ("material",     7),
    ]

    # First pass: collect unique values
    unique_vals = {f[0]: set() for f in INDEXED_FIELDS}

    filtered_rows = []
    for r in export_rows:
        country = safe_str(r[11])
        if not country:
            continue
        if not any(x in country.upper() for x in ["UNITED STATES", "CANADA", "MEXICO"]):
            continue
        vin = safe_str(r[8])
        if not vin:
            continue
        filtered_rows.append(r)

        material = safe_str(r[7])
        unique_vals["my"].add(parse_my(material))
        unique_vals["body"].add(parse_body(material))
        unique_vals["status"].add(classify_status(safe_str(r[13])))
        unique_vals["dealer"].add(clean_dealer_name(safe_str(r[0])))

        for key, col in INDEXED_FIELDS:
            if col is not None:
                unique_vals[key].add(safe_str(r[col]).strip() if col == 14 else safe_str(r[col]))

    # Build lookup dictionaries
    v_dict = {}
    idx_maps = {}
    for key, _ in INDEXED_FIELDS:
        vals = sorted(unique_vals[key] - {""}) + [""]  # empty string last
        v_dict[key] = vals
        idx_maps[key] = {v: i for i, v in enumerate(vals)}

    # Second pass: build compact records
    # Format: [vin, so_no, msrp, so_value, dis, eta, vessel, idx_my, idx_body, idx_trim, ...]
    v_data = []
    for r in filtered_rows:
        vin = safe_str(r[8])
        material = safe_str(r[7])
        eta_date = serial_to_date(r[52])

        row = [
            vin,
            safe_str(r[3]),       # so_no
            vi(r[18]),            # msrp
            round(vf(r[15]), 2),  # so_value
            vi(r[57]),            # dis
            eta_date.strftime("%Y-%m-%d") if eta_date else "",  # eta
            safe_str(r[53])[:30] if r[53] else "",  # vessel
        ]

        # Append indexed fields
        row.append(idx_maps["my"].get(parse_my(material), len(v_dict["my"])-1))
        row.append(idx_maps["body"].get(parse_body(material), len(v_dict["body"])-1))
        row.append(idx_maps["status"].get(classify_status(safe_str(r[13])), len(v_dict["status"])-1))
        row.append(idx_maps["channel"].get(safe_str(r[14]).strip(), len(v_dict["channel"])-1))
        row.append(idx_maps["dealer"].get(clean_dealer_name(safe_str(r[0])), len(v_dict["dealer"])-1))
        row.append(idx_maps["stock_cat"].get(safe_str(r[9]), len(v_dict["stock_cat"])-1))
        row.append(idx_maps["plant"].get(safe_str(r[50]), len(v_dict["plant"])-1))
        row.append(idx_maps["material"].get(material, len(v_dict["material"])-1))

        # Spec fields (trim through sound)
        spec_fields = ["trim","pack","color","seats","roof","safari","wheels","tyres",
                        "frame","tow","heated_seats","diff_locks","ladder","aux_battery",
                        "aux_switch","carpet","compass","centre_diff","emerg_safety",
                        "floor_trim","winch","utility_rails","privacy_glass","air_intake",
                        "smokers","spare_wheel","front_tow","bump_strips","steering",
                        "wheel_locks","sound"]
        for key in spec_fields:
            col = next(c for k, c in INDEXED_FIELDS if k == key)
            val = safe_str(r[col])
            row.append(idx_maps[key].get(val, len(v_dict[key])-1))

        v_data.append(row)

    print(f"  Extracted {len(v_data):,} vehicles (compact format)")
    return v_data, v_dict


# ---------------------------------------------------------------------------
# Dealer metrics
# ---------------------------------------------------------------------------

def build_dealer_metrics(export_rows, mkt_map):
    """Compute per-dealer metrics with cumulative sales at multiple breakpoints.

    Sends raw cumulative handover counts at 30/60/90/120/150/180/270/365 day
    breakpoints so the front-end can compute monthly averages for any user-chosen
    time window pair.
    """
    today = datetime.now()
    us_markets = {"Central", "Northeast", "Southeast", "Western"}
    BREAKPOINTS = [30, 60, 90, 120, 150, 180, 270, 365]

    dealer_og = defaultdict(int)
    # Cumulative sales at each breakpoint: dealer -> {30: N, 60: N, ...}
    dealer_cum = defaultdict(lambda: {bp: 0 for bp in BREAKPOINTS})

    for r in export_rows:
        country = safe_str(r[11])
        if "UNITED STATES" not in country.upper():
            continue

        channel = safe_str(r[14]).strip()
        if channel not in ("STOCK", "PRIVATE - RETAILER"):
            continue

        dealer = clean_dealer_name(safe_str(r[0]))
        market = lookup_mkt(mkt_map, dealer)
        if market not in us_markets:
            continue

        status = safe_str(r[13]).lower()

        # On-ground inventory
        if "dealer stock" in status or "7." in status:
            dealer_og[dealer] += 1

        # Sales (handovers) — cumulative at each breakpoint
        ho_date = serial_to_date(r[51])
        if ho_date:
            days_ago = (today - ho_date).days
            for bp in BREAKPOINTS:
                if days_ago <= bp:
                    dealer_cum[dealer][bp] += 1

    # YTD handovers
    dealer_ytd = defaultdict(int)
    for r in export_rows:
        country = safe_str(r[11])
        if "UNITED STATES" not in country.upper():
            continue
        channel = safe_str(r[14]).strip()
        if channel not in ("STOCK", "PRIVATE - RETAILER"):
            continue
        dealer = clean_dealer_name(safe_str(r[0]))
        ho_date = serial_to_date(r[51])
        if ho_date and ho_date.year == today.year:
            dealer_ytd[dealer] += 1

    all_dealers = set(dealer_og.keys()) | set(dealer_cum.keys())

    # Build dealer list with cumulative breakpoint data
    dealers = []
    for dealer in sorted(all_dealers):
        market = lookup_mkt(mkt_map, dealer)
        if market not in us_markets:
            continue

        og = dealer_og.get(dealer, 0)
        cum = dealer_cum.get(dealer, {bp: 0 for bp in BREAKPOINTS})

        dealers.append({
            "name": dealer,
            "market": market,
            "og": og,
            "cum": cum,        # { 30: N, 60: N, 90: N, ... 365: N }
            "ytd_ho": dealer_ytd.get(dealer, 0),
        })

    print(f"  Computed metrics for {len(dealers)} US dealers (cumulative breakpoints: {BREAKPOINTS})")
    return dealers


# ---------------------------------------------------------------------------
# Plant affinity: which plants historically service which dealers
# ---------------------------------------------------------------------------

def build_plant_affinity(export_rows, mkt_map):
    """Build plant -> dealer shipping history for US retail vehicles.

    Returns dict: { plant_code: { dealer_name: ship_count } }
    Only includes dealers in US markets with STOCK/PRIVATE-RETAILER channel.
    """
    us_markets = {"Central", "Northeast", "Southeast", "Western"}
    plant_dealer = defaultdict(lambda: defaultdict(int))

    for r in export_rows:
        country = safe_str(r[11])
        if "UNITED STATES" not in country.upper():
            continue
        channel = safe_str(r[14]).strip()
        if channel not in ("STOCK", "PRIVATE - RETAILER"):
            continue
        plant = safe_str(r[50])
        dealer = clean_dealer_name(safe_str(r[0]))
        if not plant or not dealer:
            continue
        # Skip stock pool names
        if "IN_US_STK" in dealer or "INEOS US STOCK" in dealer:
            continue
        market = lookup_mkt(mkt_map, dealer)
        if market not in us_markets:
            continue
        plant_dealer[plant][dealer] += 1

    # Convert to percentages (affinity scores 0-1)
    result = {}
    for plant, dealers in plant_dealer.items():
        total = sum(dealers.values())
        result[plant] = {d: round(c / total, 4) for d, c in dealers.items()}

    print(f"  Built plant affinity for {len(result)} plants")
    return result


# ---------------------------------------------------------------------------
# Pipeline composition: what body/trim/color each dealer currently has
# ---------------------------------------------------------------------------

def build_pipeline_composition(export_rows, mkt_map):
    """Build current pipeline composition per dealer (non-sold vehicles).

    Returns dict: { dealer: { "body|trim|color": count } }
    """
    us_markets = {"Central", "Northeast", "Southeast", "Western"}
    pipeline = defaultdict(lambda: defaultdict(int))

    for r in export_rows:
        country = safe_str(r[11])
        if "UNITED STATES" not in country.upper():
            continue
        channel = safe_str(r[14]).strip()
        if channel not in ("STOCK", "PRIVATE - RETAILER"):
            continue
        status = safe_str(r[13]).lower()
        if "8. sold" in status:
            continue
        dealer = clean_dealer_name(safe_str(r[0]))
        if "IN_US_STK" in dealer or "INEOS US STOCK" in dealer:
            continue
        market = lookup_mkt(mkt_map, dealer)
        if market not in us_markets:
            continue

        mat = safe_str(r[7])
        body = "QM" if "quartermaster" in mat.lower() else "SW"
        trim = safe_str(r[19])
        color = safe_str(r[21])
        # Track at two granularities: body|trim and body|trim|color
        pipeline[dealer][f"{body}|{trim}"] += 1
        pipeline[dealer][f"{body}|{trim}|{color}"] += 1

    result = {d: dict(counts) for d, counts in pipeline.items()}
    print(f"  Built pipeline composition for {len(result)} dealers")
    return result


# ---------------------------------------------------------------------------
# Sell-through: historical conversion rate by dealer x body|trim
# ---------------------------------------------------------------------------

def build_sell_through(export_rows, mkt_map):
    """Build sell-through rates per dealer x config (body|trim).

    Returns dict: { dealer: { "body|trim": { "delivered": N, "sold": N, "rate": 0.XX } } }
    """
    us_markets = {"Central", "Northeast", "Southeast", "Western"}
    delivered = defaultdict(lambda: defaultdict(int))
    sold = defaultdict(lambda: defaultdict(int))

    for r in export_rows:
        country = safe_str(r[11])
        if "UNITED STATES" not in country.upper():
            continue
        channel = safe_str(r[14]).strip()
        if channel not in ("STOCK", "PRIVATE - RETAILER"):
            continue
        dealer = clean_dealer_name(safe_str(r[0]))
        if "IN_US_STK" in dealer or "INEOS US STOCK" in dealer:
            continue
        market = lookup_mkt(mkt_map, dealer)
        if market not in us_markets:
            continue

        status = safe_str(r[13]).lower()
        mat = safe_str(r[7])
        body = "QM" if "quartermaster" in mat.lower() else "SW"
        trim = safe_str(r[19])
        config = f"{body}|{trim}"

        # Delivered = reached dealer stock or sold
        if "7. dealer stock" in status or "8. sold" in status:
            delivered[dealer][config] += 1
        if "8. sold" in status:
            sold[dealer][config] += 1

    # Also compute network-wide sell-through per config for comparison
    net_delivered = defaultdict(int)
    net_sold = defaultdict(int)
    for d in delivered:
        for cfg, cnt in delivered[d].items():
            net_delivered[cfg] += cnt
            net_sold[cfg] += sold[d].get(cfg, 0)

    result = {}
    for dealer in delivered:
        result[dealer] = {}
        for cfg in delivered[dealer]:
            d_count = delivered[dealer][cfg]
            s_count = sold[dealer].get(cfg, 0)
            dealer_rate = s_count / d_count if d_count > 0 else 0
            net_rate = net_sold[cfg] / net_delivered[cfg] if net_delivered[cfg] > 0 else 0
            result[dealer][cfg] = {
                "d": d_count,
                "s": s_count,
                "r": round(dealer_rate, 3),
                "nr": round(net_rate, 3),  # network rate for comparison
            }

    print(f"  Built sell-through rates for {len(result)} dealers")
    return result


# ---------------------------------------------------------------------------
# Days to sell: avg time each dealer takes to sell each config
# ---------------------------------------------------------------------------

def build_days_to_sell(export_rows, mkt_map):
    """Build avg days-to-sell per dealer x body|trim, and network averages.

    Returns dict: { "dealer": { "body|trim": { "avg": N, "cnt": N } },
                     "_network": { "body|trim": { "avg": N, "cnt": N } } }
    """
    us_markets = {"Central", "Northeast", "Southeast", "Western"}
    dts_dealer = defaultdict(lambda: defaultdict(list))
    dts_network = defaultdict(list)

    for r in export_rows:
        country = safe_str(r[11])
        if "UNITED STATES" not in country.upper():
            continue
        channel = safe_str(r[14]).strip()
        if channel not in ("STOCK", "PRIVATE - RETAILER"):
            continue
        status = safe_str(r[13]).lower()
        if "8. sold" not in status:
            continue

        dts_raw = r[56]
        try:
            dts = float(dts_raw)
            if dts < 0 or dts > 999:
                continue
        except (TypeError, ValueError):
            continue

        dealer = clean_dealer_name(safe_str(r[0]))
        if "IN_US_STK" in dealer or "INEOS US STOCK" in dealer:
            continue
        market = lookup_mkt(mkt_map, dealer)
        if market not in us_markets:
            continue

        mat = safe_str(r[7])
        body = "QM" if "quartermaster" in mat.lower() else "SW"
        trim = safe_str(r[19])
        config = f"{body}|{trim}"

        dts_dealer[dealer][config].append(dts)
        dts_network[config].append(dts)

    result = {}
    for dealer, configs in dts_dealer.items():
        result[dealer] = {}
        for cfg, vals in configs.items():
            result[dealer][cfg] = {
                "a": round(sum(vals) / len(vals), 1),
                "c": len(vals)
            }

    # Network averages
    result["_network"] = {}
    for cfg, vals in dts_network.items():
        result["_network"][cfg] = {
            "a": round(sum(vals) / len(vals), 1),
            "c": len(vals)
        }

    print(f"  Built days-to-sell for {len(result) - 1} dealers + network")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_refresh(master_path, template_path, output_path, decrypted_path=None):
    """Entry point for programmatic use (e.g. from FastAPI app)."""
    if decrypted_path is None:
        decrypted_path = os.path.join(os.path.dirname(master_path), "master_decrypted.xlsb")
    _run(master_path, template_path, output_path, decrypted_path)


def main():
    if not os.path.exists(MASTER_PATH):
        print(f"ERROR: Master file not found: {MASTER_PATH}")
        sys.exit(1)
    if not os.path.exists(TEMPLATE_PATH):
        print(f"ERROR: Template not found: {TEMPLATE_PATH}")
        sys.exit(1)
    _run(MASTER_PATH, TEMPLATE_PATH, OUTPUT_PATH, DECRYPTED_PATH)


def _run(master_path, template_path, output_path, decrypted_path):
    print("=" * 60)
    print("INEOS Vehicle Allocation Tool — Data Refresh")
    print("=" * 60)

    # Step 1: Decrypt
    print("\nStep 1: Decrypt master file...")
    dec_path = decrypt_master(master_path, output_path=decrypted_path)

    # Step 2: Read data
    print("\nStep 2: Read workbook...")
    wb = open_workbook(dec_path)

    print("  Loading Export sheet...")
    export_rows = []
    with wb.get_sheet("Export") as sheet:
        for i, row in enumerate(sheet.rows()):
            vals = [c.v for c in row]
            if i <= 1:
                continue
            export_rows.append(vals)
    print(f"  Read {len(export_rows):,} export rows")

    print("  Loading market map...")
    mkt_map = build_mkt_map(wb)

    print("\nStep 3: Extract vehicles (compact)...")
    v_data, v_dict = build_vehicles_compact(export_rows, mkt_map)

    print("\nStep 4: Compute dealer metrics...")
    dealers = build_dealer_metrics(export_rows, mkt_map)

    print("\nStep 5: Build allocation intelligence...")
    plant_affinity = build_plant_affinity(export_rows, mkt_map)
    pipeline_comp = build_pipeline_composition(export_rows, mkt_map)
    sell_through = build_sell_through(export_rows, mkt_map)
    days_to_sell = build_days_to_sell(export_rows, mkt_map)

    print("\nStep 6: Build HTML...")
    with open(template_path, "r", encoding="utf-8") as f:
        html = f.read()

    html = replace_const(html, "V_DATA", v_data)
    html = replace_const(html, "V_DICT", v_dict)
    html = replace_const(html, "DEALERS", dealers)
    html = replace_const(html, "PLANT_AFFINITY", plant_affinity)
    html = replace_const(html, "PIPELINE_COMP", pipeline_comp)
    html = replace_const(html, "SELL_THROUGH", sell_through)
    html = replace_const(html, "DAYS_TO_SELL", days_to_sell)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    html = replace_const(html, "DATA_TS", ts)

    print("\nStep 7: Write output...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n  Output: {output_path}")
    print(f"  Size: {len(html):,} bytes")
    print(f"  Vehicles: {len(v_data):,}")
    print(f"  Dealers: {len(dealers)}")
    print("\nDone!")


if __name__ == "__main__":
    main()
