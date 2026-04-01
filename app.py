"""
app.py  —  Flash Sale SKU Recommendation Dashboard
Open:  http://127.0.0.1:5050
"""
import json, sys
from pathlib import Path
from flask import Flask, jsonify, send_from_directory, request

HOST = "127.0.0.1"; PORT = 5050
BASE = Path(__file__).parent
app  = Flask(__name__)

def load_data():
    f = BASE / "data.json"
    if not f.exists(): sys.exit("\n❌  data.json not found. Run: python prepare_data.py\n")
    with open(f, encoding="utf-8") as fh: return json.load(fh)

DATA = load_data()
DAYS = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
PKGS = {str(i): f"package{i}" for i in range(1,6)}

@app.route("/")
def index(): return send_from_directory(str(BASE), "index.html")

@app.route("/api/summary")
def api_summary(): return jsonify(DATA["summary"])

@app.route("/api/stats")
def api_stats():
    recs = DATA["package1"]
    return jsonify({"total_recommendations": len(recs),
                    "cities": sorted({r["city"] for r in recs}), "days": DAYS})

@app.route("/api/package/<pkg>/<day>")
def api_package_day(pkg, day):
    key = PKGS.get(pkg)
    if not key or day not in DAYS: return jsonify({"error":"invalid"}), 400
    city = request.args.get("city",""); l1 = request.args.get("l1","")
    rows = [r for r in DATA[key] if r["day"]==day]
    if city: rows = [r for r in rows if r["city"]==city]
    if l1:   rows = [r for r in rows if r["l1_category"]==l1]
    return jsonify(rows)

@app.route("/api/htc")
def api_htc(): return jsonify(DATA.get("htc",[]))

@app.route("/api/htc_map")
def api_htc_map(): return jsonify(DATA.get("htc_map",{}))

@app.route("/api/keywords")
def api_keywords(): return jsonify(DATA.get("keywords",{}))

@app.route("/api/l1list")
def api_l1list():
    return jsonify(sorted({r["l1_category"] for r in DATA["package1"]}))

if __name__ == "__main__":
    print(f"\n🚀  http://{HOST}:{PORT}  |  Ctrl+C to stop\n")
    app.run(host=HOST, port=PORT, debug=False)
