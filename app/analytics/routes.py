from flask import Blueprint, current_app, jsonify, render_template, request
from ..extensions import mongo
from datetime import datetime

analytics_bp = Blueprint("analytics", __name__, url_prefix="/analytics")

def _db():
    return mongo.cx.get_database(current_app.config["MONGO_DBNAME"])

TICKETS = "tickets"     # adapte si besoin
MAGASINS = "magasins"   # ta table d'admin avec champs "Magasin", "BU"

def build_filter_match_stage(filters=None):
    """Build MongoDB $match stage based on filters"""
    if not filters:
        return {}
    
    match_stage = {}
    
    # Agent filter (exact match, case insensitive)
    if filters.get("agent"):
        match_stage["agent"] = {"$regex": f"^{filters['agent']}$", "$options": "i"}
    
    # Canal filter (exact match)
    if filters.get("canal"):
        match_stage["canal"] = filters["canal"]
    
    # Thematique filter (exact match)
    if filters.get("thematique"):
        match_stage["thematique"] = filters["thematique"]
    
    # Action filter (exact match)
    if filters.get("action"):
        match_stage["action"] = filters["action"]
    
    # Magasin filter (case insensitive)
    if filters.get("magasin"):
        match_stage["magasin"] = {"$regex": f"^{filters['magasin']}$", "$options": "i"}
    
    # Total code promo range filter
    if filters.get("min_promo") or filters.get("max_promo"):
        promo_conditions = []
        if filters.get("min_promo"):
            try:
                min_val = float(filters["min_promo"])
                promo_conditions.append({
                    "$gte": [
                        {"$toDouble": {
                            "$replaceAll": {
                                "input": {"$toString": {"$ifNull": ["$total_code_promo", 0]}},
                                "find": ",", "replacement": "."
                            }
                        }},
                        min_val
                    ]
                })
            except ValueError:
                pass
        
        if filters.get("max_promo"):
            try:
                max_val = float(filters["max_promo"])
                promo_conditions.append({
                    "$lte": [
                        {"$toDouble": {
                            "$replaceAll": {
                                "input": {"$toString": {"$ifNull": ["$total_code_promo", 0]}},
                                "find": ",", "replacement": "."
                            }
                        }},
                        max_val
                    ]
                })
            except ValueError:
                pass
        
        if promo_conditions:
            if len(promo_conditions) == 1:
                match_stage["$expr"] = promo_conditions[0]
            else:
                match_stage["$expr"] = {"$and": promo_conditions}
    
    # Date range filter
    if filters.get("date_from") or filters.get("date_to"):
        date_conditions = []
        
        if filters.get("date_from"):
            try:
                date_from = datetime.strptime(filters["date_from"], "%Y-%m-%d")
                date_conditions.append({
                    "$gte": [
                        {"$dateFromString": {
                            "dateString": "$date_creation",
                            "onError": None
                        }},
                        date_from
                    ]
                })
            except ValueError:
                pass
        
        if filters.get("date_to"):
            try:
                date_to = datetime.strptime(filters["date_to"], "%Y-%m-%d")
                date_to = date_to.replace(hour=23, minute=59, second=59)  # End of day
                date_conditions.append({
                    "$lte": [
                        {"$dateFromString": {
                            "dateString": "$date_creation",
                            "onError": None
                        }},
                        date_to
                    ]
                })
            except ValueError:
                pass
        
        if date_conditions:
            # Combine with existing $expr if it exists
            existing_expr = match_stage.get("$expr")
            if existing_expr:
                if len(date_conditions) == 1:
                    match_stage["$expr"] = {"$and": [existing_expr, date_conditions[0]]}
                else:
                    match_stage["$expr"] = {"$and": [existing_expr] + date_conditions}
            else:
                if len(date_conditions) == 1:
                    match_stage["$expr"] = date_conditions[0]
                else:
                    match_stage["$expr"] = {"$and": date_conditions}
    
    return match_stage

def apply_bu_filter(pipeline, bu_filter):
    """Apply BU filter after lookup stage"""
    if bu_filter:
        pipeline.append({
            "$match": {
                "bu_final": {"$regex": f"^{bu_filter}$", "$options": "i"}
            }
        })
    return pipeline

@analytics_bp.get("/")
def page():
    return render_template("analytics.html")

# New endpoint to get filter options
@analytics_bp.get("/api/filter_options")
def filter_options():
    """Get available options for each filter"""
    db = _db()
    
    # Get unique agents
    agents = db[TICKETS].distinct("agent")
    agents = sorted([a for a in agents if a and str(a).strip()])
    
    # Get unique canaux
    canaux = db[TICKETS].distinct("canal")
    canaux = sorted([c for c in canaux if c and str(c).strip()])
    
    # Get unique thematiques
    thematiques = db[TICKETS].distinct("thematique")
    thematiques = sorted([t for t in thematiques if t and str(t).strip()])
    
    # Get unique actions
    actions = db[TICKETS].distinct("action")
    actions = sorted([a for a in actions if a and str(a).strip()])
    
    # Get unique magasins
    magasins = db[TICKETS].distinct("magasin")
    magasins = sorted([m for m in magasins if m and str(m).strip()])
    
    # Get unique BU values (from both tickets.bu and magasins lookup)
    bus_from_tickets = db[TICKETS].distinct("bu")
    bus_from_magasins = db[MAGASINS].distinct("BU")
    all_bus = set()
    all_bus.update([b for b in bus_from_tickets if b and str(b).strip()])
    all_bus.update([b for b in bus_from_magasins if b and str(b).strip()])
    bus = sorted(list(all_bus))
    
    # Get date range
    date_range = list(db[TICKETS].aggregate([
        {"$group": {
            "_id": None,
            "min_date": {"$min": "$date_creation"},
            "max_date": {"$max": "$date_creation"}
        }}
    ]))
    
    min_date = None
    max_date = None
    if date_range:
        min_date = date_range[0].get("min_date")
        max_date = date_range[0].get("max_date")
        
        # Convert to date strings if they're datetime objects
        if isinstance(min_date, datetime):
            min_date = min_date.strftime("%Y-%m-%d")
        elif isinstance(min_date, str):
            try:
                # Try to parse and reformat
                parsed = datetime.strptime(min_date.split()[0], "%Y-%m-%d")
                min_date = parsed.strftime("%Y-%m-%d")
            except:
                pass
                
        if isinstance(max_date, datetime):
            max_date = max_date.strftime("%Y-%m-%d")
        elif isinstance(max_date, str):
            try:
                # Try to parse and reformat
                parsed = datetime.strptime(max_date.split()[0], "%Y-%m-%d")
                max_date = parsed.strftime("%Y-%m-%d")
            except:
                pass
    
    # Get promo amount range
    promo_range = list(db[TICKETS].aggregate([
        {"$addFields": {
            "_promo_numeric": {
                "$toDouble": {
                    "$replaceAll": {
                        "input": {"$toString": {"$ifNull": ["$total_code_promo", 0]}},
                        "find": ",", "replacement": "."
                    }
                }
            }
        }},
        {"$group": {
            "_id": None,
            "min_promo": {"$min": "$_promo_numeric"},
            "max_promo": {"$max": "$_promo_numeric"}
        }}
    ]))
    
    min_promo = 0
    max_promo = 0
    if promo_range:
        min_promo = promo_range[0].get("min_promo", 0) or 0
        max_promo = promo_range[0].get("max_promo", 0) or 0
    
    return jsonify({
        "agents": agents,
        "canaux": canaux,
        "thematiques": thematiques,
        "actions": actions,
        "magasins": magasins,
        "bus": bus,
        "date_range": {
            "min_date": min_date,
            "max_date": max_date
        },
        "promo_range": {
            "min_promo": round(min_promo, 2),
            "max_promo": round(max_promo, 2)
        }
    })

# 1) Contacts par BU (pie) - Enhanced with filters
@analytics_bp.get("/api/by_bu")
def by_bu():
    db = _db()
    
    # Get filters from query parameters
    filters = {
        "agent": request.args.get("agent"),
        "canal": request.args.get("canal"),
        "thematique": request.args.get("thematique"),
        "action": request.args.get("action"),
        "magasin": request.args.get("magasin"),
        "min_promo": request.args.get("min_promo"),
        "max_promo": request.args.get("max_promo"),
        "date_from": request.args.get("date_from"),
        "date_to": request.args.get("date_to")
    }
    
    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None and v != ""}
    
    pipeline = []
    
    # Add filter stage if filters exist
    match_stage = build_filter_match_stage(filters)
    if match_stage:
        pipeline.append({"$match": match_stage})
    
    # Add the existing BU processing stages
    pipeline.extend([
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
        }}
    ])
    
    # Apply BU filter if specified
    bu_filter = request.args.get("bu")
    if bu_filter:
        pipeline.append({
            "$match": {
                "bu_final": {"$regex": f"^{bu_filter}$", "$options": "i"}
            }
        })
    
    # Complete the aggregation
    pipeline.extend([
        {"$group": {"_id": {"$cond": [{"$eq": ["$bu_final", ""]}, "Autres", "$bu_final"]}, "n": {"$sum": 1}}},
        {"$sort": {"n": -1}}
    ])
    
    rows = list(db[TICKETS].aggregate(pipeline))
    total = sum(r["n"] for r in rows) or 1
    return jsonify({
        "labels": [r["_id"] for r in rows],
        "values": [r["n"] for r in rows],
        "pct":    [round(100*r["n"]/total, 2) for r in rows],
        "total": total
    })

# 2) Traitement des contacts par agent (bar) - Enhanced with filters
@analytics_bp.get("/api/by_agent")
def by_agent():
    db = _db()
    
    # Get filters from query parameters
    filters = {
        "canal": request.args.get("canal"),
        "thematique": request.args.get("thematique"),
        "action": request.args.get("action"),
        "magasin": request.args.get("magasin"),
        "bu": request.args.get("bu"),
        "min_promo": request.args.get("min_promo"),
        "max_promo": request.args.get("max_promo"),
        "date_from": request.args.get("date_from"),
        "date_to": request.args.get("date_to")
    }
    
    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None and v != ""}
    
    pipeline = []
    
    # Add filter stage if filters exist
    match_stage = build_filter_match_stage(filters)
    if match_stage:
        pipeline.append({"$match": match_stage})
    
    # Handle BU filter (requires lookup for magasin-based BU)
    if filters.get("bu"):
        pipeline.extend([
            {"$addFields": {
                "_magasin_norm": {"$toLower": {"$trim": {"input": {"$ifNull": ["$magasin", ""]}}}}
            }},
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
            {"$match": {
                "bu_final": {"$regex": f"^{filters['bu']}$", "$options": "i"}
            }}
        ])
    
    # Complete the aggregation
    pipeline.extend([
        {"$addFields": {"agent_norm": {"$toUpper": {"$trim": {"input": {"$ifNull": ["$agent", "AUTRES"]}}}}}},
        {"$group": {"_id": "$agent_norm", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 10}
    ])
    
    rows = list(db[TICKETS].aggregate(pipeline))
    total = sum(r["n"] for r in rows) or 1
    return jsonify({
        "labels": [r["_id"] for r in rows],
        "values": [r["n"] for r in rows],
        "pct":    [round(100*r["n"]/total, 2) for r in rows],
        "total": total
    })

# 3) Répartition par canal (donut) - Enhanced with filters
@analytics_bp.get("/api/by_canal")
def by_canal():
    db = _db()
    
    # Get filters from query parameters
    filters = {
        "agent": request.args.get("agent"),
        "thematique": request.args.get("thematique"),
        "action": request.args.get("action"),
        "magasin": request.args.get("magasin"),
        "bu": request.args.get("bu"),
        "min_promo": request.args.get("min_promo"),
        "max_promo": request.args.get("max_promo"),
        "date_from": request.args.get("date_from"),
        "date_to": request.args.get("date_to")
    }
    
    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None and v != ""}
    
    pipeline = []
    
    # Add filter stage if filters exist
    match_stage = build_filter_match_stage(filters)
    if match_stage:
        pipeline.append({"$match": match_stage})
    
    # Handle BU filter
    if filters.get("bu"):
        pipeline.extend([
            {"$addFields": {
                "_magasin_norm": {"$toLower": {"$trim": {"input": {"$ifNull": ["$magasin", ""]}}}}
            }},
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
            {"$match": {
                "bu_final": {"$regex": f"^{filters['bu']}$", "$options": "i"}
            }}
        ])
    
    # Complete the aggregation
    pipeline.extend([
        {"$addFields": {"canal_norm": {"$trim": {"input": {"$ifNull": ["$canal", "AUTRES"]}}}}},
        {"$group": {"_id": {"$cond": [{"$eq": ["$canal_norm", "" ]}, "AUTRES", "$canal_norm"]}, "n": {"$sum": 1}}},
        {"$sort": {"n": -1}}
    ])
    
    rows = list(db[TICKETS].aggregate(pipeline))
    total = sum(r["n"] for r in rows) or 1
    return jsonify({
        "labels": [r["_id"] for r in rows],
        "values": [r["n"] for r in rows],
        "pct":    [round(100*r["n"]/total, 2) for r in rows],
        "total": total
    })

# 4) Contacts par thématique (bar + %) - Enhanced with filters
@analytics_bp.get("/api/by_thematique")
def by_thematique():
    db = _db()
    
    # Get filters from query parameters
    filters = {
        "agent": request.args.get("agent"),
        "canal": request.args.get("canal"),
        "action": request.args.get("action"),
        "magasin": request.args.get("magasin"),
        "bu": request.args.get("bu"),
        "min_promo": request.args.get("min_promo"),
        "max_promo": request.args.get("max_promo"),
        "date_from": request.args.get("date_from"),
        "date_to": request.args.get("date_to")
    }
    
    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None and v != ""}
    
    pipeline = []
    
    # Add filter stage if filters exist
    match_stage = build_filter_match_stage(filters)
    if match_stage:
        pipeline.append({"$match": match_stage})
    
    # Handle BU filter
    if filters.get("bu"):
        pipeline.extend([
            {"$addFields": {
                "_magasin_norm": {"$toLower": {"$trim": {"input": {"$ifNull": ["$magasin", ""]}}}}
            }},
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
            {"$match": {
                "bu_final": {"$regex": f"^{filters['bu']}$", "$options": "i"}
            }}
        ])
    
    # Complete the aggregation
    pipeline.extend([
        {"$addFields": {"th": {"$trim": {"input": {"$ifNull": ["$thematique", "Autres"]}}}}},
        {"$group": {"_id": {"$cond": [{"$eq": ["$th", ""]}, "Autres", "$th"]}, "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 8}
    ])
    
    rows = list(db[TICKETS].aggregate(pipeline))
    total = sum(r["n"] for r in rows) or 1
    return jsonify({
        "labels": [r["_id"] for r in rows],
        "values": [r["n"] for r in rows],
        "pct":    [round(100*r["n"]/total, 2) for r in rows],
        "total": total
    })

# 5) Tableau Actions / Montant (sum total_code_promo) - Enhanced with filters
@analytics_bp.get("/api/actions_montant")
def actions_montant_alias():
    db = _db()
    
    # Get filters from query parameters
    filters = {
        "agent": request.args.get("agent"),
        "canal": request.args.get("canal"),
        "thematique": request.args.get("thematique"),
        "magasin": request.args.get("magasin"),
        "bu": request.args.get("bu"),
        "date_from": request.args.get("date_from"),
        "date_to": request.args.get("date_to")
    }
    
    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None and v != ""}
    
    amount = {
        "$toDouble": {
            "$replaceAll": {
                "input": {"$toString": {"$ifNull": ["$total_code_promo", 0]}},
                "find": ",", "replacement": "."
            }
        }
    }
    
    pipeline = []
    
    # Add filter stage if filters exist
    match_stage = build_filter_match_stage(filters)
    if match_stage:
        pipeline.append({"$match": match_stage})
    
    # Handle BU filter
    if filters.get("bu"):
        pipeline.extend([
            {"$addFields": {
                "_magasin_norm": {"$toLower": {"$trim": {"input": {"$ifNull": ["$magasin", ""]}}}}
            }},
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
            {"$match": {
                "bu_final": {"$regex": f"^{filters['bu']}$", "$options": "i"}
            }}
        ])
    
    # Complete the aggregation
    pipeline.extend([
        {"$addFields": {"_amount": amount, "_action": {
            "$trim": {"input": {"$ifNull": ["$action", "AUTRES"]}}
        }}},
        {"$addFields": {"_action": {"$cond": [{"$eq": ["$_action", ""]}, "AUTRES", "$_action"]}}},
        {"$group": {"_id": "$_action", "amount": {"$sum": "$_amount"}}},
        {"$sort": {"amount": -1}}
    ])
    
    rows = list(db[TICKETS].aggregate(pipeline))
    actions  = [r["_id"] for r in rows]
    montants = [round(r["amount"], 2) for r in rows]
    total = round(sum(montants), 2)
    return jsonify({"actions": actions, "montants": montants, "total": total})


@analytics_bp.get("/api/total")
def total_tickets():
    db = _db()
    filters = {
        "agent": request.args.get("agent"),
        "canal": request.args.get("canal"),
        "thematique": request.args.get("thematique"),
        "action": request.args.get("action"),
        "magasin": request.args.get("magasin"),
        "bu": request.args.get("bu"),
        "date_from": request.args.get("date_from"),
        "date_to": request.args.get("date_to"),
        "min_promo": request.args.get("min_promo"),
        "max_promo": request.args.get("max_promo"),
    }
    filters = {k: v for k, v in filters.items() if v not in (None, "")}

    pipeline = []
    match_stage = build_filter_match_stage(filters)
    if match_stage:
        pipeline.append({"$match": match_stage})

    # Filtre BU si présent (même bloc que tes autres endpoints)
    if filters.get("bu"):
        pipeline += [
            {"$addFields": {
                "_magasin_norm": {"$toLower": {"$trim": {"input": {"$ifNull": ["$magasin", ""]}}}}
            }},
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
            {"$addFields": {
                "bu_final": {
                    "$let": {
                        "vars": {"bu_lookup": {"$first": "$_m.BU"}},
                        "in": {"$trim": {"input": {"$ifNull": ["$$bu_lookup", {"$ifNull": ["$bu", "Autres"]}]}}}
                    }
                }
            }},
            {"$match": {"bu_final": {"$regex": f"^{filters['bu']}$", "$options": "i"}}},
        ]

    pipeline.append({"$count": "n"})

    rows = list(db[TICKETS].aggregate(pipeline))
    total = rows[0]["n"] if rows else 0
    return jsonify({"total": total})
