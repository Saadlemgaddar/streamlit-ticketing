# --- exact fields: agent, canal, thematique, action, total_code_promo, magasin, bu ---

from flask import Blueprint, current_app, jsonify, render_template
from ..extensions import mongo

analytics_bp = Blueprint("analytics", __name__, url_prefix="/analytics")

def _db():
    return mongo.cx.get_database(current_app.config["MONGO_DBNAME"])

TICKETS = "tickets"     # adapte si besoin
MAGASINS = "magasins"   # ta table d’admin avec champs "Magasin", "BU"

@analytics_bp.get("/")
def page():
    return render_template("analytics.html")

# 1) Contacts par BU (pie)
@analytics_bp.get("/api/by_bu")
def by_bu():
    db = _db()
    pipeline = [
        # nom du magasin normalisé
        {"$addFields": {
            "_magasin_norm": {"$toLower": {"$trim": {"input": {"$ifNull": ["$magasin", ""]}}}}
        }},
        # lookup magasins -> BU
        {"$lookup": {
            "from": MAGASINS,
            "let": {"m": "$_magasin_norm"},
            "pipeline": [
                {"$addFields": {"_Magasin_norm": {"$toLower": {"$trim": {"input": {"$ifNull": ["$Magasin", ""]}}}}}},
                {"$match": {"$expr": {"$eq": ["$_Magasin_norm", "$$m"]}}},
                {"$project": {"BU": 1, "_id": 0}}
            ],
            "as": "_m"
        }},
        # choisir BU: du lookup sinon champ 'bu' du ticket, sinon "Autres"
        {"$addFields": {
            "bu_final": {
                "$let": {
                    "vars": {"bu_lookup": {"$first": "$_m.BU"}},
                    "in": {
                        "$trim": {"input": {
                            "$ifNull": ["$$bu_lookup", {"$ifNull": ["$bu", "Autres"]}]
                        }}
                    }
                }
            }
        }},
        {"$group": {"_id": {"$cond": [{"$eq": ["$bu_final", ""]}, "Autres", "$bu_final"]}, "n": {"$sum": 1}}},
        {"$sort": {"n": -1}}
    ]
    rows = list(db[TICKETS].aggregate(pipeline))
    total = sum(r["n"] for r in rows) or 1
    return jsonify({
        "labels": [r["_id"] for r in rows],
        "values": [r["n"] for r in rows],
        "pct":    [round(100*r["n"]/total, 2) for r in rows],
        "total": total
    })

# 2) Traitement des contacts par agent (bar)
@analytics_bp.get("/api/by_agent")
def by_agent():
    db = _db()
    pipeline = [
        {"$addFields": {"agent_norm": {"$toUpper": {"$trim": {"input": {"$ifNull": ["$agent", "AUTRES"]}}}}}},
        {"$group": {"_id": "$agent_norm", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 10}
    ]
    rows = list(db[TICKETS].aggregate(pipeline))
    total = sum(r["n"] for r in rows) or 1
    return jsonify({
        "labels": [r["_id"] for r in rows],
        "values": [r["n"] for r in rows],
        "pct":    [round(100*r["n"]/total, 2) for r in rows],
        "total": total
    })

# 3) Répartition par canal (donut)
@analytics_bp.get("/api/by_canal")
def by_canal():
    db = _db()
    pipeline = [
        {"$addFields": {"canal_norm": {"$trim": {"input": {"$ifNull": ["$canal", "AUTRES"]}}}}},
        {"$group": {"_id": {"$cond": [{"$eq": ["$canal_norm", "" ]}, "AUTRES", "$canal_norm"]}, "n": {"$sum": 1}}},
        {"$sort": {"n": -1}}
    ]
    rows = list(db[TICKETS].aggregate(pipeline))
    total = sum(r["n"] for r in rows) or 1
    return jsonify({
        "labels": [r["_id"] for r in rows],
        "values": [r["n"] for r in rows],
        "pct":    [round(100*r["n"]/total, 2) for r in rows],
        "total": total
    })

# 4) Contacts par thématique (bar + %)
@analytics_bp.get("/api/by_thematique")
def by_thematique():
    db = _db()
    pipeline = [
        {"$addFields": {"th": {"$trim": {"input": {"$ifNull": ["$thematique", "Autres"]}}}}},
        {"$group": {"_id": {"$cond": [{"$eq": ["$th", ""]}, "Autres", "$th"]}, "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 8}
    ]
    rows = list(db[TICKETS].aggregate(pipeline))
    total = sum(r["n"] for r in rows) or 1
    return jsonify({
        "labels": [r["_id"] for r in rows],
        "values": [r["n"] for r in rows],
        "pct":    [round(100*r["n"]/total, 2) for r in rows],
        "total": total
    })

# 5) Tableau Actions / Montant (sum total_code_promo)
@analytics_bp.get("/api/actions_amount")
def actions_amount():
    db = _db()
    # total_code_promo peut être number ou string "123,45" -> on convertit
    amount = {
        "$toDouble": {
            "$replaceAll": {
                "input": {"$toString": {"$ifNull": ["$total_code_promo", 0]}},
                "find": ",", "replacement": "."
            }
        }
    }
    action = {"$trim": {"input": {"$ifNull": ["$action", "AUTRES"]}}}
    pipeline = [
        {"$addFields": {"_amount": amount, "_action": action}},
        {"$group": {"_id": {"$cond": [{"$eq": ["$_action", ""]}, "AUTRES", "$_action"]}, "amount": {"$sum": "$_amount"}}},
        {"$sort": {"amount": -1}}
    ]
    rows = list(db[TICKETS].aggregate(pipeline))
    total = round(sum(r["amount"] for r in rows), 2)
    out = [{"Action": r["_id"], "amount": round(r["amount"], 2)} for r in rows]
    return jsonify({"rows": out, "total": total})
