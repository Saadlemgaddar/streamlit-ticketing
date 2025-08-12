# app/admin/routes.py
from flask import Blueprint, current_app, render_template, request, jsonify, session, redirect, abort
from functools import wraps
from bson.objectid import ObjectId
from ..extensions import mongo

admin_bp = Blueprint("admin", __name__, template_folder="../templates", url_prefix="/_admin")

def _db():
    return mongo.cx.get_database(current_app.config["MONGO_DBNAME"])

def coll(name: str):
    return _db()[name]

# --- Sécurisation très simple: clé dans l'URL une fois, puis session ---
def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        secret = (current_app.config.get("ADMIN_SECRET") or "").strip()
        # Première visite: /_admin?key=SECRET -> on set la session et on retire la query
        key = (request.args.get("key") or "").strip()
        if secret and key and key == secret:
            session["is_admin"] = True
            return redirect(request.path)  # même URL sans ?key
        # Accès si déjà validé en session
        if session.get("is_admin"):
            return fn(*args, **kwargs)
        # sinon: 404 pour ne pas révéler l'URL
        abort(404)
    return wrapper

# --- Page console ---
@admin_bp.get("/")
@admin_required
def dashboard():
    return render_template("admin.html")

# ================== CANAUX ==================
@admin_bp.get("/api/canaux")
@admin_required
def api_canaux_list():
    items = sorted({(r.get("canal") or "").strip()
                   for r in coll("canaux").find({}, {"_id": 0, "canal": 1}) if (r.get("canal") or "").strip()})
    return jsonify({"items": items})

@admin_bp.post("/api/canaux")
@admin_required
def api_canaux_add():
    canal = (request.json or {}).get("canal", "").strip()
    if not canal:
        return jsonify({"error": "canal requis"}), 400
    if coll("canaux").find_one({"canal": canal}):
        return jsonify({"error": "existe déjà"}), 409
    coll("canaux").insert_one({"canal": canal})
    return jsonify({"ok": True})

@admin_bp.delete("/api/canaux")
@admin_required
def api_canaux_del():
    canal = (request.args.get("canal") or "").strip()
    if not canal:
        return jsonify({"error": "canal requis"}), 400
    coll("canaux").delete_many({"canal": canal})
    return jsonify({"ok": True})

# ================== MAGASINS ==================
MAG_FIELDS = ["Magasin", "Code magasin", "Ville", "BU", "Region", "DR", "DM"]

@admin_bp.get("/api/magasins")
@admin_required
def api_magasins_list():
    rows = list(coll("magasins").find({}, {"_id": 1, **{f:1 for f in MAG_FIELDS}}))
    for r in rows:
        r["_id"] = str(r["_id"])
    return jsonify({"rows": rows})

@admin_bp.post("/api/magasins")
@admin_required
def api_magasins_add():
    data = request.json or {}
    doc = {f: (data.get(f) or "").strip() for f in MAG_FIELDS}
    if not doc["Magasin"]:
        return jsonify({"error": "Magasin requis"}), 400
    coll("magasins").insert_one(doc)
    return jsonify({"ok": True})

@admin_bp.put("/api/magasins/<oid>")
@admin_required
def api_magasins_update(oid):
    data = request.json or {}
    try:
        _id = ObjectId(oid)
    except:
        return jsonify({"error": "bad id"}), 400
    updates = {f: (data.get(f) or "").strip() for f in MAG_FIELDS if f in data}
    coll("magasins").update_one({"_id": _id}, {"$set": updates})
    return jsonify({"ok": True})

@admin_bp.delete("/api/magasins/<oid>")
@admin_required
def api_magasins_delete(oid):
    try:
        _id = ObjectId(oid)
    except:
        return jsonify({"error": "bad id"}), 400
    coll("magasins").delete_one({"_id": _id})
    return jsonify({"ok": True})

# ================== THÉMATIQUES ==================
THEM_FIELDS = ["Thematique", "Famille", "Sous famille", "Catégorie", "Sous catégorie ", "Actions"]

@admin_bp.get("/api/thematiques")
@admin_required
def api_them_list():
    rows = list(coll("thematiques").find({}, {"_id": 1, **{f:1 for f in THEM_FIELDS}}))
    for r in rows:
        r["_id"] = str(r["_id"])
    return jsonify({"rows": rows})

@admin_bp.post("/api/thematiques")
@admin_required
def api_them_add():
    data = request.json or {}
    doc = {f: (data.get(f) or "").strip() for f in THEM_FIELDS}
    if not doc["Thematique"]:
        return jsonify({"error": "Thematique requise"}), 400
    coll("thematiques").insert_one(doc)
    return jsonify({"ok": True})

@admin_bp.put("/api/thematiques/<oid>")
@admin_required
def api_them_update(oid):
    data = request.json or {}
    try:
        _id = ObjectId(oid)
    except:
        return jsonify({"error": "bad id"}), 400
    updates = {f: (data.get(f) or "").strip() for f in THEM_FIELDS if f in data}
    coll("thematiques").update_one({"_id": _id}, {"$set": updates})
    return jsonify({"ok": True})

@admin_bp.delete("/api/thematiques/<oid>")
@admin_required
def api_them_delete(oid):
    try:
        _id = ObjectId(oid)
    except:
        return jsonify({"error": "bad id"}), 400
    coll("thematiques").delete_one({"_id": _id})
    return jsonify({"ok": True})

# ================== AGENTS ==================
@admin_bp.get("/api/agents")
@admin_required
def api_agents_list():
    rows = list(coll("agents").find({}, {"_id": 1, "username": 1, "full_name": 1, "email": 1}))
    for r in rows:
        r["_id"] = str(r["_id"])
    return jsonify({"rows": rows})

@admin_bp.post("/api/agents")
@admin_required
def api_agents_add():
    data = request.json or {}
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    full_name = (data.get("full_name") or "").strip()
    email = (data.get("email") or "").strip()

    if not username or not password:
        return jsonify({"error": "username et password requis"}), 400
    if coll("agents").find_one({"username": username}):
        return jsonify({"error": "username existe déjà"}), 409

    coll("agents").insert_one({
        "username": username,
        "password": password,          # (hash en prod)
        "full_name": full_name or username,
        "email": email or "-"
    })
    return jsonify({"ok": True})

@admin_bp.put("/api/agents/<oid>")
@admin_required
def api_agents_update(oid):
    from bson.objectid import ObjectId
    try:
        _id = ObjectId(oid)
    except:
        return jsonify({"error": "bad id"}), 400

    data = request.json or {}
    updates = {}
    for k in ["username","password","full_name","email"]:
        if k in data and str(data[k]).strip() != "":
            updates[k] = str(data[k]).strip()
    if not updates:
        return jsonify({"error": "rien à mettre à jour"}), 400

    # si username change, vérifier unicité
    if "username" in updates:
        exists = coll("agents").find_one({"_id": {"$ne": _id}, "username": updates["username"]})
        if exists:
            return jsonify({"error": "username existe déjà"}), 409

    coll("agents").update_one({"_id": _id}, {"$set": updates})
    return jsonify({"ok": True})

@admin_bp.delete("/api/agents/<oid>")
@admin_required
def api_agents_del(oid):
    from bson.objectid import ObjectId
    try:
        _id = ObjectId(oid)
    except:
        return jsonify({"error": "bad id"}), 400
    coll("agents").delete_one({"_id": _id})
    return jsonify({"ok": True})
