import os
from app import app

# Entry point for GCP App Engine/Cloud Run. 
# Note: Procfile is used for Cloud Run with gunicorn. 
# main.py provides a fallback for other GCP builders.
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
