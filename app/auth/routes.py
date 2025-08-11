from flask import Blueprint, current_app, render_template, request, redirect, url_for, flash, make_response
from ..extensions import mongo
from ..utils.security import sign_token, verify_token

auth_bp = Blueprint("auth", __name__)

def _db():
    # <-- récupère explicitement la base "ticketing_db" depuis la config
    return mongo.cx.get_database(current_app.config["MONGO_DBNAME"])

def find_agent(username, password):
    coll = _db()["agents"]   # ✅ plus de None
    return coll.find_one({"username": username, "password": password}, {"_id": 0})

@auth_bp.before_app_request
def autologin_via_cookie():
    from flask import g, request
    g.user = None
    token = request.cookies.get("auth_token")
    if token and not getattr(request, "user_authenticated", False):
        payload = verify_token(token)
        if payload:
            user = find_agent(payload.get("username",""), payload.get("password",""))
            if user:
                g.user = user

@auth_bp.route("/", methods=["GET"])
def home_redirect():
    return redirect(url_for("tickets.list_tickets"))

@auth_bp.route("/login", methods=["GET","POST"])
def login():
    from flask import g
    if g.get("user"):
        return redirect(url_for("tickets.list_tickets"))

    if request.method == "POST":
        username = request.form.get("username","").strip()
        password = request.form.get("password","").strip()
        remember = request.form.get("remember") == "on"
        user = find_agent(username, password)
        if user:
            resp = make_response(redirect(url_for("tickets.list_tickets")))
            if remember:
                resp.set_cookie("auth_token", sign_token({"username":username, "password":password}),
                                max_age=15*24*3600, httponly=True, samesite="Lax")
            else:
                resp.delete_cookie("auth_token")
            return resp
        flash("Identifiants invalides.", "danger")
    return render_template("login.html")

@auth_bp.route("/logout")
def logout():
    resp = make_response(redirect(url_for("auth.login")))
    resp.delete_cookie("auth_token")
    return resp
