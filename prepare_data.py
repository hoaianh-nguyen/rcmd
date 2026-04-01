"""
prepare_data.py  —  Flash Sale SKU Recommendation Engine
=========================================================
Count order:  Package 3 < Package 2 < Package 1  (pkg4/5 subset of pkg3)

─── Price matching logic ───────────────────────────────────────────────────
dish_price (from CSV) is matched against benchmark discount_price at each
percentile: pct25 / pct50 / pct75 / pct90 / pct95.
An item PASSES a percentile if: dish_price is within ±15% of that pct value.
Field `price_pct_match` lists which pcts the item passes (may be empty list).

─── Package filters ─────────────────────────────────────────────────────────
Package 1 — BROADEST   pct50 ADO ≥ 1.5   (no price constraint)
Package 2 — PRICE FIT  pct50 ADO ≥ 1.5   AND dish_price matches ≥1 pct (±15%)
Package 3 — STRICTEST  pct75 ADO ≥ 4.0   + district + keywords + price match
Package 4 — CENTER     pkg3 logic, center districts only
Package 5 — REST       pkg3 logic, non-center districts only
"""

import json, sys, re, gzip
from pathlib import Path
from collections import defaultdict
import pandas as pd
import numpy as np

CSV_FILE  = "cmi_dish_port_DownloadTableCopy_20260331_175043.csv"
XLSX_FILE = "Flashsale_data_library_final.xlsx"
HTC_SHEET = "Cat L2 + HTC Propose"
OUT_FILE  = "data.json"

DAYS     = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
PCTS     = [0.25, 0.50, 0.75, 0.90, 0.95]
PCT_LBLS = ["pct25","pct50","pct75","pct90","pct95"]

CITY_MAP  = {"Hai Phong City":"Hai Phong","Binh Duong":"Binh Duong"}
BENCH_MAP = {"Hai Phong":"HN","Binh Duong":"HCM"}

PKG3_P75 = 4.0
PKG2_P75 = 3.0
PKG1_P50 = 1.5
PRICE_TOL = 0.15   # ±15%

HP_CENTER = {"Ngo Quyen District","Hong Bang District","Le Chan District"}
BD_CENTER = {"Di An","Thuan An","Thu Dau Mot Town"}

STOPWORDS = {
    "và","với","của","các","cho","trong","trên","từ","một","size","l","m","s",
    "xl","combo","set","free","tặng","mua","giá","ăn","uống","không","có",
    "thêm","phần","hộp","ly","cốc","chai","gói","suất","thường","đặc","biệt",
    "siêu","mini","nhỏ","lớn","vừa","đầy","đủ","ngon","mới","hot","sale",
    "deal","ưu","đãi","khuyến","mãi","tháng","ngày","tuần","nay",
}

# ── helpers ──────────────────────────────────────────────────────────────────

def load_merchants():
    print(f"  Reading {CSV_FILE} …")
    df = pd.read_csv(CSV_FILE)
    df["city_key"] = df["city_name"].map(CITY_MAP)
    df = df.dropna(subset=["city_key"])
    df["dish_price_num"] = (
        df["dish_price"].astype(str).str.replace(",","",regex=False).str.strip()
        .apply(lambda x: float(x) if x.replace(".","").isdigit() else None)
    )
    return df

def get_sheet(day, bench):
    return pd.read_excel(XLSX_FILE, sheet_name=f"{day}-{bench}")

def cat_benchmarks(df):
    ado = (df.groupby(["l1_item_cate","l2_item_cate"])["gross_fs_ado"]
           .quantile(PCTS).unstack())
    ado.columns = PCT_LBLS
    price = (df.groupby(["l1_item_cate","l2_item_cate"])["discount_price"]
             .quantile(PCTS).unstack())
    price.columns = [f"price_{c}" for c in PCT_LBLS]
    return ado.reset_index().merge(price.reset_index(), on=["l1_item_cate","l2_item_cate"])

def price_pct_matches(dish_price, row):
    """Return list of pct labels where dish_price is within ±15% of benchmark price."""
    if not dish_price or dish_price <= 0:
        return []
    matches = []
    for lbl in PCT_LBLS:
        pv = float(row.get(f"price_{lbl}", 0) or 0)
        if pv > 0 and pv*(1-PRICE_TOL) <= dish_price <= pv*(1+PRICE_TOL):
            matches.append(lbl)
    return matches

def tokenise(text):
    return set(re.findall(
        r"[a-záàảãạăắặằẳẵâấầẩẫậđéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵ]+",
        str(text).lower()
    ))

def extract_keywords(df, top_n=5):
    result = {}
    for l2, grp in df.groupby("l2_item_cate"):
        tok_ado, tok_price = defaultdict(list), defaultdict(list)
        for _, row in grp.iterrows():
            seen = set()
            for t in tokenise(row["item_name"]):
                if len(t) >= 3 and t not in STOPWORDS and t not in seen:
                    seen.add(t)
                    tok_ado[t].append(row["gross_fs_ado"])
                    tok_price[t].append(row["discount_price"])
        scored = [(t, np.mean(v)) for t, v in tok_ado.items() if len(v) >= 2]
        scored.sort(key=lambda x: -x[1])
        kws = []
        for kw, ado_mean in scored[:top_n]:
            pq = np.quantile(tok_price[kw], PCTS)
            kws.append({"keyword":kw,"ado_mean":round(ado_mean,2),
                        **{f"price_{PCT_LBLS[i]}":int(pq[i]) for i in range(len(PCTS))}})
        result[l2] = kws
    return result

def load_all_bench(bench):
    frames = []
    for day in DAYS:
        try: frames.append(pd.read_excel(XLSX_FILE, sheet_name=f"{day}-{bench}"))
        except: pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def load_htc():
    try:
        df = pd.read_excel(XLSX_FILE, sheet_name=HTC_SHEET)
        print(f"  HTC tab: {len(df)} rows, cols={df.columns.tolist()}")
        return df
    except Exception as e:
        print(f"  ⚠  HTC tab missing ({e})")
        return None

def is_center(district, city):
    return district in (HP_CENTER if city=="Hai Phong" else BD_CENTER)

def reason(bench, day, l2, p50, p75, p90, pkg, pm=None):
    tier = "top" if p75>=p90*0.9 else ("above-median" if p75>=p50*1.5 else "solid")
    thresholds={1:f"p50≥{PKG1_P50}",2:f"p50≥{PKG1_P50}+price±15%",
                3:f"p75≥{PKG3_P75}",4:f"p75≥{PKG3_P75}+center",5:f"p75≥{PKG3_P75}+rest"}
    pm_str = f" Price match: {','.join(pm)}." if pm else ""
    return (f"[{bench}·{day}] '{l2}': p50={p50:.1f}/p75={p75:.1f}/p90={p90:.1f}. "
            f"Tier={tier}. Filter={thresholds[pkg]}.{pm_str}")

# ── main ─────────────────────────────────────────────────────────────────────

def parse_htc_map():
    """Parse Cat L2 + HTC Propose tab into {bench: {weekday/weekend: {slot: {center/rest: [l2...]}}}}"""
    import re as _re
    try:
        dfh = pd.read_excel(XLSX_FILE, sheet_name="Cat L2 + HTC Propose", header=None)
    except Exception as e:
        print(f"  ⚠  HTC map parse failed: {e}")
        return {}

    def clean(val):
        if not val or pd.isna(val): return None
        s = _re.sub(r"^[a-z]+\d+\.\s*", "", str(val)).rstrip(";").strip()
        return s if s else None

    WD = [("5h-9h",2,3),("9h-10h",4,5),("10h-12h",6,7),("12h-13h",8,9),
          ("13h-16h",10,11),("16h-17h",12,13),("17h-20h",14,15),("20h-22h",16,17),("22h-5h",18,19)]
    WE = [("0h-6h",24,25),("6h-10h",26,27),("10h-13h",28,29),
          ("13h-16h",30,31),("16h-20h",32,33),("20h-24h",34,35)]

    def read_sec(r0, r1, slots):
        out = {}
        for lbl, cc, cr in slots:
            center, rest = [], []
            for r in range(r0, r1):
                vc = clean(dfh.iloc[r, cc] if cc < dfh.shape[1] else None)
                vr = clean(dfh.iloc[r, cr] if cr < dfh.shape[1] else None)
                if vc: center.append(vc)
                if vr: rest.append(vr)
            out[lbl] = {"center": center, "rest": rest}
        return out

    return {
        "HN":  {"weekday": read_sec(16, 92,  WD), "weekend": read_sec(16, 92,  WE)},
        "HCM": {"weekday": read_sec(94, 168, WD), "weekend": read_sec(94, 168, WE)},
    }


def main():
    for f in [CSV_FILE, XLSX_FILE]:
        if not Path(f).exists(): sys.exit(f"❌  Not found: {f}")

    merchants = load_merchants()

    print("\n  Extracting benchmark keywords …")
    kw_cache = {}
    for city, bench in BENCH_MAP.items():
        all_df = load_all_bench(bench)
        kw_cache[bench] = extract_keywords(all_df) if not all_df.empty else {}
        print(f"    {bench}: {len(kw_cache[bench])} L2 categories")

    htc_df = load_htc()

    price_map = {
        (str(r["merchant_name"]).strip(), str(r["dish_name"]).strip()): r["dish_price_num"]
        for _, r in merchants.iterrows()
    }

    p1,p2,p3,p4,p5 = [],[],[],[],[]
    summary = {}

    for city, bench in BENCH_MAP.items():
        summary[city] = {}
        city_df = merchants[merchants["city_key"]==city].copy()
        print(f"\n  {city}  →  {bench}")

        for day in DAYS:
            try: fs = get_sheet(day, bench)
            except Exception as e:
                print(f"    ⚠  {day}-{bench}: {e}", file=sys.stderr)
                summary[city][day]={"total_skus":0,"total_merchants":0,"categories":[],
                                    "pkg1_count":0,"pkg2_count":0,"pkg3_count":0,"pkg4_count":0,"pkg5_count":0}
                continue

            bmarks = cat_benchmarks(fs)
            merged = city_df.merge(bmarks, left_on=["l1_category","l2_category"],
                                   right_on=["l1_item_cate","l2_item_cate"], how="inner")

            r1,r2,r3,r4,r5 = [],[],[],[],[]

            for _, r in merged.iterrows():
                l1=str(r["l1_category"]); l2=str(r["l2_category"])
                p25=float(r["pct25"]); p50=float(r["pct50"])
                p75=float(r["pct75"]); p90=float(r["pct90"]); p95=float(r["pct95"])
                dp  = price_map.get((str(r["merchant_name"]).strip(), str(r["dish_name"]).strip()))
                dist = str(r.get("district_name",""))
                center = is_center(dist, city)
                pm  = price_pct_matches(dp, r)   # list of matched pcts

                price_fields = {
                    "price_pct25":int(r.get("price_pct25",0) or 0),
                    "price_pct50":int(r.get("price_pct50",0) or 0),
                    "price_pct75":int(r.get("price_pct75",0) or 0),
                    "price_pct90":int(r.get("price_pct90",0) or 0),
                    "price_pct95":int(r.get("price_pct95",0) or 0),
                    "dish_price": int(dp) if dp and dp>0 else None,
                    "price_pct_match": pm,   # ← which pcts are within ±15%
                }
                kws    = kw_cache.get(bench,{}).get(l2,[])
                kw_str = ", ".join(k["keyword"] for k in kws)
                base = {
                    "day":day,"city":city,"benchmark":bench,
                    "merchant_id": str(r.get("merchant_id","")).strip(),
                    "merchant":str(r["merchant_name"]),
                    "dish_id": str(r.get("dish_id","")).strip(),
                    "sku":str(r["dish_name"]),
                    "l1_category":l1,"l2_category":l2,
                    "pct25":round(p25,2),"pct50":round(p50,2),
                    "pct75":round(p75,2),"pct90":round(p90,2),"pct95":round(p95,2),
                }

                # ── Pkg 1: broadest, no price filter ──
                if p50 >= PKG1_P50:
                    r1.append({**base, **price_fields,
                                "reason":reason(bench,day,l2,p50,p75,p90,1,pm)})

                # ── Pkg 2: medium ADO + price must match ≥1 pct ──
                if p50 >= PKG1_P50 and pm:   # pkg2 = pkg1 scope + price fit required
                    r2.append({**base, **price_fields,
                                "pricing_note":(f"Price {int(dp):,}₫ matches benchmark " if dp else "")
                                    + f"{', '.join(pm)} (±15%). "
                                    + f"Benchmark p50={price_fields['price_pct50']:,}₫ p75={price_fields['price_pct75']:,}₫",
                                "reason":reason(bench,day,l2,p50,p75,p90,2,pm)})

                # ── Pkg 3: strictest ──
                if p75 >= PKG3_P75:
                    r3.append({**base, **price_fields,
                                "district":dist,"top_keywords":kw_str,
                                "reason":reason(bench,day,l2,p50,p75,p90,3,pm)})

                # ── Pkg 4: center districts ──
                if p75 >= PKG3_P75 and center:
                    r4.append({**base, **price_fields,
                                "district":dist,"district_type":"Center","top_keywords":kw_str,
                                "reason":reason(bench,day,l2,p50,p75,p90,4,pm)})

                # ── Pkg 5: non-center ──
                if p75 >= PKG3_P75 and not center:
                    r5.append({**base, **price_fields,
                                "district":dist,"district_type":"Rest","top_keywords":kw_str,
                                "reason":reason(bench,day,l2,p50,p75,p90,5,pm)})

            p1.extend(r1);p2.extend(r2);p3.extend(r3);p4.extend(r4);p5.extend(r5)

            rdf1 = pd.DataFrame(r1) if r1 else pd.DataFrame(columns=["l1_category","sku","merchant"])
            cats = (rdf1.groupby("l1_category").agg(items=("sku","count"),merchants=("merchant","nunique"))
                    .sort_values("items",ascending=False).reset_index().to_dict(orient="records")
                    ) if not rdf1.empty else []
            summary[city][day]={
                "total_skus":len(r1),"total_merchants":int(rdf1["merchant"].nunique()) if not rdf1.empty else 0,
                "categories":cats,
                "pkg1_count":len(r1),"pkg2_count":len(r2),"pkg3_count":len(r3),
                "pkg4_count":len(r4),"pkg5_count":len(r5),
            }
            print(f"    {day}: p1={len(r1):,}  p2={len(r2):,}  p3={len(r3):,}  p4={len(r4):,}  p5={len(r5):,}")

    htc_data = None
    if htc_df is not None:
        htc_data = [{k:(None if (isinstance(v,float) and np.isnan(v)) else v)
                     for k,v in row.items()} for row in htc_df.to_dict(orient="records")]

    htc_map=parse_htc_map()
    out={"summary":summary,"package1":p1,"package2":p2,"package3":p3,
         "package4":p4,"package5":p5,"keywords":kw_cache,"htc":htc_data or [],"htc_map":htc_map}

    with open(OUT_FILE,"w",encoding="utf-8") as f:
        json.dump(out,f,ensure_ascii=False,separators=(",",":"))

    gz_file = OUT_FILE + ".gz"
    with open(OUT_FILE,"rb") as f_in, gzip.open(gz_file,"wb",compresslevel=6) as f_out:
        f_out.write(f_in.read())

    size_kb=Path(OUT_FILE).stat().st_size/1024
    gz_kb=Path(gz_file).stat().st_size/1024
    print(f"\n✅  {OUT_FILE}  ({size_kb:.0f} KB)")
    print(f"✅  {gz_file}  ({gz_kb:.0f} KB)  ← commit this for GitHub Pages")
    print(f"    p3={len(p3):,} < p2={len(p2):,} < p1={len(p1):,}  order_ok={len(p3)<len(p2)<len(p1)}")
    print(f"    p4(center)={len(p4):,}  p5(rest)={len(p5):,}")

if __name__ == "__main__":
    print("🔄  Preparing data …\n")
    main()
