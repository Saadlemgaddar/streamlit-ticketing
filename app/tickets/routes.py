# app/tickets/routes.py
from flask import Blueprint, current_app, render_template, request, redirect, url_for, flash, jsonify, send_file, g
from ..extensions import mongo
from datetime import datetime
import io
import pandas as pd
from pymongo import ReturnDocument

tickets_bp = Blueprint("tickets", __name__, template_folder="../templates")

EXPECTED_HEADERS = [
    'id','date_creation','agent','nom_prenom','id_client','num_cmd','canal',
    'thematique','famille','sous_famille','categorie','sous_categorie','action',
    'traitement','si_exceptionnel','code_promo','prix_pdts','mnt_commande','mnt_rembour',
    'mnt_gestco','total_code_promo','retour_magasin','commentaires',
    'date_cloture','cloture_by','statut','magasin','num_magasin','ville','bu','region','dr','dm'
]

def _db():
    return mongo.cx.get_database(current_app.config["MONGO_DBNAME"])

def _now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def require_user():
    if not g.get("user"):
        from flask import redirect, url_for
        return redirect(url_for("auth.login"))

def coll(name: str):
    return _db()[name]

def _numeric_id(v):
    """Convert various forms ('547', 547, '547.0') to int 547, else None."""
    try:
        s = str(v).strip()
        n = int(float(s))
        return n
    except Exception:
        return None

def ensure_ticket_sequence():
    """
    Ensure:
      - unique index exists on tickets.id
      - counters.tickets exists
      - counters.tickets.seq >= max numeric id in tickets
    Safe to call often.
    """
    # unique index (idempotent)
    try:
        coll("tickets").create_index("id", unique=True)
    except Exception:
        pass

    # compute max id present
    max_id = 0
    for r in coll("tickets").find({}, {"_id": 0, "id": 1}):
        n = _numeric_id(r.get("id"))
        if n is not None:
            max_id = max(max_id, n)

    c = coll("counters")
    cur = c.find_one({"_id": "tickets"})
    if cur is None:
        c.insert_one({"_id": "tickets", "seq": max_id})
    else:
        if int(cur.get("seq", 0)) < max_id:
            c.update_one({"_id": "tickets"}, {"$set": {"seq": max_id}})

def next_ticket_id() -> str:
    ensure_ticket_sequence()
    res = coll("counters").find_one_and_update(
        {"_id": "tickets"},
        {"$inc": {"seq": 1}},
        return_document=ReturnDocument.AFTER,
        upsert=True
    )
    return str(res["seq"])

def _find_ticket_by_id(id_value: str):
    ors = [{"id": str(id_value)}]
    nid = _numeric_id(id_value)
    if nid is not None:
        ors.append({"id": nid})
    return coll("tickets").find_one({"$or": ors}, {"_id": 0})

# put this near the top of routes.py
import pandas as pd
import numpy as np

NULLY = {"", "None", "none", "nan", "NaN", "_", "-"}

def parse_date_creation(series):
    s = pd.Series(series, dtype="string").str.strip()
    s = s.where(~s.isin(NULLY), pd.NA)

    # Pass 1: your app's ISO strings (no dayfirst here!)
    dc = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")

    # Pass 2: fallback for FR strings like 'dd/mm/YYYY HH:MM' (use dayfirst)
    m = dc.isna()
    if m.any():
        dc.loc[m] = pd.to_datetime(s[m], format="%d/%m/%Y %H:%M", errors="coerce", dayfirst=True)

    # Last chance: free parse for any leftovers
    m = dc.isna()
    if m.any():
        dc.loc[m] = pd.to_datetime(s[m], errors="coerce", dayfirst=True)

    return dc

def _slug_col(name: str) -> str:
    import unicodedata, re
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s.strip()).lower()
    return s

CANON_MAP = {
    "thematique": "Thematique",
    "famille": "Famille",
    "sous famille": "Sous Famille",
    "sous_famille": "Sous Famille",
    "categorie": "Categorie",
    "catégorie": "Categorie",
    "sous categorie": "Sous Categorie",
    "sous catégorie": "Sous Categorie",
    "actions": "Action",
    "action": "Action",
}

def _normalize_thematiques_columns(rows):
    import pandas as pd, unicodedata
    if not rows:
        return []
    df = pd.DataFrame(rows).fillna("")
    # rename headers to canonical
    rename_dict = {}
    for c in df.columns:
        slug = _slug_col(c)
        if slug in CANON_MAP:
            rename_dict[c] = CANON_MAP[slug]
    df = df.rename(columns=rename_dict)
    # ensure columns exist
    for col in ["Thematique", "Famille", "Sous Famille", "Categorie", "Sous Categorie", "Action"]:
        if col not in df.columns:
            df[col] = ""
    # normalize values
    def _canon_val(x: str) -> str:
        s = unicodedata.normalize("NFKC", str(x))
        return s.strip()
    for col in ["Thematique", "Famille", "Sous Famille", "Categorie", "Sous Categorie", "Action"]:
        df[col] = df[col].astype(str).map(_canon_val)
    return df.astype(str).to_dict(orient="records")

# ---------- APIs used by forms ----------

@tickets_bp.get("/api/canaux")
def api_canaux():
    rows = list(coll("canaux").find({}, {"_id":0, "canal":1}))
    canaux = sorted({(r.get("canal") or "").strip() for r in rows if (r.get("canal") or "").strip()})
    return jsonify(canaux)

@tickets_bp.get("/api/magasins")
def api_magasins():
    rows = list(coll("magasins").find({}, {"_id":0}))
    possible = ['Magasin', 'magasin', 'nom_magasin', 'nom', 'store_name']
    label = next((p for p in possible if rows and p in rows[0]), None)
    data = []
    for r in rows:
        lib = (r.get(label) or "").strip() if label else ""
        data.append({"label": lib, "row": r})
    data = [x for x in data if x["label"]]
    data.sort(key=lambda x: x["label"])
    return jsonify(data)

@tickets_bp.get("/api/thematiques")
def api_thematiques_root():
    rows = list(coll("thematiques").find({}, {"_id":0}))
    norm = _normalize_thematiques_columns(rows)
    thems = sorted({r["Thematique"].strip() for r in norm if r["Thematique"].strip()})
    return jsonify(thems)

@tickets_bp.get("/api/thematiques/children")
def api_thematiques_children():
    import unicodedata
    def _cv(s: str) -> str:
        s = unicodedata.normalize("NFKC", str(s or ""))
        return s.strip().casefold()
    def _eq(a, b) -> bool:
        return _cv(a) == _cv(b)

    q_t  = request.args.get("thematique", "") or ""
    q_f  = request.args.get("famille", "") or ""
    q_sf = request.args.get("sous_famille", "") or ""
    q_c  = request.args.get("categorie", "") or ""
    q_sc = request.args.get("sous_categorie", "") or ""

    rows = list(coll("thematiques").find({}, {"_id": 0}))
    norm = _normalize_thematiques_columns(rows)

    def uniq(vals):
        return sorted({(v or "").strip() for v in vals if (v or "").strip()})

    if q_t and not q_f:
        familles = uniq([r["Famille"] for r in norm if _eq(r["Thematique"], q_t)])
        return jsonify({"level": "famille", "values": familles})
    if q_t and q_f and not q_sf:
        sfs = uniq([r["Sous Famille"] for r in norm if _eq(r["Thematique"], q_t) and _eq(r["Famille"], q_f)])
        return jsonify({"level": "sous_famille", "values": sfs})
    if q_t and q_f and q_sf and not q_c:
        cats = uniq([r["Categorie"] for r in norm if _eq(r["Thematique"], q_t) and _eq(r["Famille"], q_f) and _eq(r["Sous Famille"], q_sf)])
        return jsonify({"level": "categorie", "values": cats})
    if q_t and q_f and q_sf and q_c and not q_sc:
        scats = uniq([r["Sous Categorie"] for r in norm if _eq(r["Thematique"], q_t) and _eq(r["Famille"], q_f) and _eq(r["Sous Famille"], q_sf) and _eq(r["Categorie"], q_c)])
        return jsonify({"level": "sous_categorie", "values": scats})
    if q_t and q_f and q_sf and q_c and q_sc:
        acts = uniq([r["Action"] for r in norm if _eq(r["Thematique"], q_t) and _eq(r["Famille"], q_f) and _eq(r["Sous Famille"], q_sf) and _eq(r["Categorie"], q_c) and _eq(r["Sous Categorie"], q_sc)])
        return jsonify({"level": "action", "values": acts})
    return jsonify({"level": "none", "values": []})

# ---------- Views ----------

@tickets_bp.route("/list")
def list_tickets():
    ru = require_user()
    if ru: return ru

    q = {}
    agent = request.args.get("agent")
    statut = request.args.get("statut")
    thematique = request.args.get("thematique")
    magasin = request.args.get("magasin")
    search = request.args.get("q","").strip().lower()
    dmin = request.args.get("dmin")
    dmax = request.args.get("dmax")

    if agent: q["agent"] = agent
    if statut: q["statut"] = statut
    if thematique: q["thematique"] = thematique
    if magasin: q["magasin"] = magasin

    rows = list(coll("tickets").find(q, {"_id":0}))
    df = pd.DataFrame(rows)
    if df.empty:
        return render_template("tickets_list.html", rows=[], agents=[], statuts=[], thems=[], magasins=[], search=search)

    # Date filter (parse with dayfirst=True; iso works too)
    if dmin or dmax:
        dc = parse_date_creation(df.get("date_creation"))
        df["date_creation"] = dc
        if dmin:
            df = df[df["date_creation"].dt.date >= pd.to_datetime(dmin).date()]
        if dmax:
            df = df[df["date_creation"].dt.date <= pd.to_datetime(dmax).date()]

    # Global search
    if search:
        t = search
        for col in ["nom_prenom","num_cmd","id_client","magasin","commentaires","id"]:
            if col not in df.columns:
                df[col] = ""
        mask = (
            df["nom_prenom"].astype(str).str.lower().str.contains(t, na=False) |
            df["num_cmd"].astype(str).str.lower().str.contains(t, na=False)  |
            df["id_client"].astype(str).str.lower().str.contains(t, na=False)|
            df["magasin"].astype(str).str.lower().str.contains(t, na=False)  |
            df["commentaires"].astype(str).str.lower().str.contains(t, na=False)|
            df["id"].astype(str).str.lower().str.contains(t, na=False)
        )
        df = df[mask]

    # Options filtres (pour select dans UI)
    for col in ["agent","statut","thematique","magasin"]:
        if col not in df.columns: df[col] = ""
    agents   = sorted([x for x in df["agent"].dropna().unique().tolist() if str(x).strip()])
    statuts  = sorted([x for x in df["statut"].dropna().unique().tolist() if str(x).strip()])
    thems    = sorted([x for x in df["thematique"].dropna().unique().tolist() if str(x).strip()])
    magasins = sorted([x for x in df["magasin"].dropna().unique().tolist() if str(x).strip()])

    # Sort by creation date desc if present
    dc = parse_date_creation(df.get("date_creation"))
    df["__dc"] = dc
    df = df.sort_values(by="__dc", ascending=False, na_position="last").drop(columns=["__dc"])

    display_cols = ["id","date_creation","agent","nom_prenom","magasin","thematique","statut","num_cmd","id_client"]
    for c in display_cols:
        if c not in df.columns: df[c] = ""

    # Format dates for display (no warning, blanks if NaT)
    dc = parse_date_creation(df.get("date_creation"))
    df["date_creation"] = dc.dt.strftime("%d/%m/%Y %H:%M").fillna("")

    # Money columns robust
    import numpy as np
    MONEY_COLS = ["prix_pdts","mnt_commande","mnt_rembour","mnt_gestco","total_code_promo"]
    for c in MONEY_COLS:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(
            df[c]
            .astype(str)
            .str.replace(',', '.', regex=False)                 # 12,3 -> 12.3
            .str.replace(r'^\s*[_-]\s*$', '', regex=True)       # "_" or "-" -> ""
            .replace({"None": "", "": np.nan}),
            errors="coerce"
        ).fillna(0.0)

    # Only for display in list
    for m in ["mnt_commande","total_code_promo"]:
        df[m] = df[m].map(lambda v: f"{v:.2f} MAD")

    rows = df[display_cols].to_dict(orient="records")
    return render_template("tickets_list.html",
                           rows=rows, agents=agents, statuts=statuts, thems=thems, magasins=magasins, search=search)

@tickets_bp.route("/create", methods=["GET","POST"])
def create_ticket():
    ru = require_user()
    if ru: return ru

    # canaux pour le select
    canaux_rows = list(coll("canaux").find({}, {"_id":0,"canal":1}))
    canaux = sorted({(r.get("canal") or "").strip() for r in canaux_rows if (r.get("canal") or "").strip()})
    if not canaux:
        canaux = ["Téléphone","Email","Chat","InApp"]

    if request.method == "POST":
        f = request.form

        # validations
        missing = []
        if not f.get("nom_prenom","").strip(): missing.append("Nom et Prénom client")
        if not f.get("canal",""): missing.append("Canal")
        if not f.get("statut",""): missing.append("Statut")
        if f.get("traitement") == "Exceptionnel" and not f.get("si_exceptionnel","").strip():
            missing.append("Motif pour traitement exceptionnel")
        for k in ["thematique","famille","sous_famille","categorie","sous_categorie","action"]:
            if not f.get(k,"").strip(): missing.append(k.replace("_"," ").title())

        if missing:
            flash("⚠️ Champs obligatoires manquants : " + ", ".join(missing), "danger")
            return render_template("ticket_form.html", mode="edit", vals=f, ticket_id=None, canaux=canaux, now=datetime.now())

        next_id = next_ticket_id()

        def ffloat(x):
            try: return float(x or 0)
            except: return 0.0

        now = datetime.now()
        total_code_promo = ffloat(f.get("mnt_rembour")) + ffloat(f.get("mnt_gestco"))

        doc = {
            'id': next_id,
            'date_creation': now.strftime("%Y-%m-%d %H:%M:%S"),
            'agent': g.user.get("username"),
            'nom_prenom': f.get("nom_prenom","").strip(),
            'id_client': f.get("id_client","").strip(),
            'num_cmd': f.get("num_cmd","").strip(),
            'canal': f.get("canal",""),
            'thematique': f.get("thematique",""),
            'famille': f.get("famille",""),
            'sous_famille': f.get("sous_famille",""),
            'categorie': f.get("categorie",""),
            'sous_categorie': f.get("sous_categorie",""),
            'action': f.get("action",""),
            'traitement': f.get("traitement","Normal"),
            'si_exceptionnel': f.get("si_exceptionnel","").strip(),
            'code_promo': f.get("code_promo","").strip(),
            'prix_pdts': ffloat(f.get("prix_pdts")),
            'mnt_commande': ffloat(f.get("mnt_commande")),
            'mnt_rembour': ffloat(f.get("mnt_rembour")),
            'mnt_gestco': ffloat(f.get("mnt_gestco")),
            'total_code_promo': total_code_promo,
            'retour_magasin': f.get("retour_magasin","").strip(),
            'commentaires': f.get("commentaires","").strip(),
            'date_cloture': "",
            'cloture_by': "",
            'statut': f.get("statut","Ouvert"),
            'magasin': f.get("magasin","").strip(),
            'num_magasin': f.get("num_magasin","").strip(),
            'ville': f.get("ville","").strip(),
            'bu': f.get("bu","").strip(),
            'region': f.get("region","").strip(),
            'dr': f.get("dr","").strip(),
            'dm': f.get("dm","").strip(),
        }
        coll("tickets").insert_one(doc)
        flash(f"✅ Ticket {next_id} créé avec succès !", "success")
        return redirect(url_for("tickets.list_tickets"))

    return render_template("ticket_form.html", mode="create", vals={}, canaux=canaux, now=datetime.now())

@tickets_bp.route("/edit/<id>", methods=["GET","POST"])
def edit_ticket(id):
    ru = require_user()
    if ru: return ru

    doc = _find_ticket_by_id(id)
    if not doc:
        flash("Ticket introuvable.", "warning")
        return redirect(url_for("tickets.list_tickets"))

    canaux_rows = list(coll("canaux").find({}, {"_id":0,"canal":1}))
    canaux = sorted({(r.get("canal") or "").strip() for r in canaux_rows if (r.get("canal") or "").strip()})
    if not canaux:
        canaux = ["Téléphone","Email","Chat","InApp"]

    if request.method == "POST":
        f = request.form
        cloture_action = "cloturer" in f

        missing = []
        if not f.get("nom_prenom","").strip(): missing.append("Nom et Prénom client")
        if not f.get("canal",""): missing.append("Canal")
        if not f.get("statut",""): missing.append("statut")
        if f.get("traitement") == "Exceptionnel" and not f.get("si_exceptionnel","").strip():
            missing.append("Motif pour traitement exceptionnel")
        for k in ["thematique","famille","sous_famille","categorie","sous_categorie","action"]:
            if not f.get(k,"").strip(): missing.append(k.replace("_"," ").title())
        if missing:
            flash("⚠️ Champs obligatoires manquants : " + ", ".join(missing), "danger")
            return render_template("ticket_form.html", mode="edit", vals=f, ticket_id=id, canaux=canaux, now=datetime.now())

        def ffloat(x):
            try: return float(x or 0)
            except: return 0.0

        updated = {**doc}
        updated.update({
            'nom_prenom': f.get("nom_prenom","").strip(),
            'id_client': f.get("id_client","").strip(),
            'num_cmd': f.get("num_cmd","").strip(),
            'canal': f.get("canal",""),
            'thematique': f.get("thematique",""),
            'famille': f.get("famille",""),
            'sous_famille': f.get("sous_famille",""),
            'categorie': f.get("categorie",""),
            'sous_categorie': f.get("sous_categorie",""),
            'action': f.get("action",""),
            'traitement': f.get("traitement","Normal"),
            'si_exceptionnel': f.get("si_exceptionnel","").strip(),
            'code_promo': f.get("code_promo","").strip(),
            'prix_pdts': ffloat(f.get("prix_pdts")),
            'mnt_commande': ffloat(f.get("mnt_commande")),
            'mnt_rembour': ffloat(f.get("mnt_rembour")),
            'mnt_gestco': ffloat(f.get("mnt_gestco")),
            'total_code_promo': ffloat(f.get("mnt_rembour")) + ffloat(f.get("mnt_gestco")),
            'retour_magasin': f.get("retour_magasin","").strip(),
            'commentaires': f.get("commentaires","").strip(),
            "statut": "Clôturé" if cloture_action else f.get("statut", "Ouvert"),
            'magasin': f.get("magasin","").strip(),
            'num_magasin': f.get("num_magasin","").strip(),
            'ville': f.get("ville","").strip(),
            'bu': f.get("bu","").strip(),
            'region': f.get("region","").strip(),
            'dr': f.get("dr","").strip(),
            'dm': f.get("dm","").strip(),
        })

        # cloture
        if updated["statut"] == "Clôturé":
            if not str(updated.get("date_cloture", "")).strip():
                updated["date_cloture"] = _now_iso()
            updated["cloture_by"] = g.user.get("username")
            updated.pop("heure_cloture", None)
        if cloture_action:
            updated["date_cloture"] = _now_iso()
            updated["cloture_by"] = g.user.get("username")
        elif str(updated.get("statut","")).lower() == "ouvert":
            updated["date_cloture"] = ""
            updated["cloture_by"] = ""

        # update by id regardless of stored type
        coll("tickets").update_one(
            {"$or": [{"id": str(id)}, {"id": _numeric_id(id)}]},
            {"$set": updated},
            upsert=False
        )
        flash(f"✅ Ticket {id} mis à jour avec succès.", "success")
        return redirect(url_for("tickets.list_tickets"))

    return render_template("ticket_form.html", mode="edit", vals=doc, now=datetime.now(), ticket_id=id, canaux=canaux)

@tickets_bp.route("/close/<id>", methods=["POST"])
def close_ticket(id):
    ru = require_user()
    if ru: return ru
    res = coll("tickets").update_one(
        {"$and": [
            {"$or": [{"id": str(id)}, {"id": _numeric_id(id)}]},
            {"statut": {"$ne": "Clôturé"}}
        ]},
        {"$set": {
            "statut": "Clôturé",
            "date_cloture": _now_iso(),
            "cloture_by": g.user.get("username")
        },
         "$unset": { "heure_cloture": "" }}
    )
    if res.matched_count == 0:
        flash("Déjà clôturé ou introuvable.", "warning")
    else:
        flash(f"Ticket {id} clôturé.", "success")
    return redirect(url_for("tickets.list_tickets"))

@tickets_bp.route("/export.csv")
def export_csv():
    ru = require_user()
    if ru: return ru
    rows = list(coll("tickets").find({}, {"_id":0}))
    df = pd.DataFrame(rows)
    csv = df.to_csv(index=False).encode("utf-8")
    return send_file(io.BytesIO(csv), mimetype="text/csv",
                     as_attachment=True, download_name="tickets.csv")

@tickets_bp.route("/analytics")
def analytics():
    ru = require_user()
    if ru: return ru
    rows = list(coll("tickets").find({}, {"_id":0}))
    df = pd.DataFrame(rows)
    if df.empty:
        stats = {"total": 0, "ouverts": 0, "by_statut": {}, "top_them": []}
    else:
        df["date_creation"] = pd.to_datetime(df["date_creation"], errors="coerce", dayfirst=True)
        total = len(df)
        ouverts = int((df["statut"]=="Ouvert").sum())
        by_statut = df["statut"].value_counts().to_dict()
        top_them = df["thematique"].value_counts().head(10).reset_index().values.tolist()
        stats = {"total": total, "ouverts": ouverts, "by_statut": by_statut, "top_them": top_them}
    return render_template("analytics.html", stats=stats)
