import os
from dotenv import load_dotenv
load_dotenv()

class Config:
    SECRET_KEY = os.environ.get("APP_SECRET", "change-me-in-env")
    MONGO_URI  = os.environ.get("MONGO_URI")
    MONGO_DBNAME = os.environ.get("MONGO_DB", "ticketing_db")
    WTF_CSRF_TIME_LIMIT = None
    ADMIN_SECRET = os.environ.get("ADMIN_SECRET")
