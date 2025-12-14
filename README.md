# Poetry Annotation Tool

A Streamlit app for annotating poems from Poets.org. Annotators can tag themes, select moods, and mark sentiment on a 2D chart. All data syncs to Firebase.

## Setup

### Firebase

1. Create a project at [Firebase Console](https://console.firebase.google.com/)
2. Enable Cloud Storage
3. Download your service account key from Project Settings > Service accounts
4. Put the file in the project root folder

### Run Locally

```bash
pip install -r requirements.txt
streamlit run src/app.py
```

Then open http://localhost:8501

### Deploy to Streamlit Cloud

1. Push your code to GitHub (don't include `firebase_config.json`)
2. Go to [streamlit.io/cloud](https://streamlit.io/cloud) and create a new app
3. Point it to `src/app.py`
4. Add your Firebase credentials in Settings > Secrets using this format:

```toml
FIREBASE_TYPE = "service_account"
FIREBASE_PROJECT_ID = "your-project-id"
FIREBASE_PRIVATE_KEY_ID = "..."
FIREBASE_PRIVATE_KEY = "Private Key"
FIREBASE_CLIENT_EMAIL = "..."
FIREBASE_CLIENT_ID = "..."
FIREBASE_AUTH_URI = "https://accounts.google.com/o/oauth2/auth"
FIREBASE_TOKEN_URI = "https://oauth2.googleapis.com/token"
FIREBASE_AUTH_PROVIDER_CERT_URL = "https://www.googleapis.com/oauth2/v1/certs"
FIREBASE_CLIENT_CERT_URL = "..."
FIREBASE_STORAGE_BUCKET = "your-project-id.appspot.com"
```

## How It Works

Annotators log in and get assigned poems to annotate. For each poem, they:
- Pick mood tags (joy, sadness, anger, etc.)
- Pick theme tags (nature, love, death, etc.)
- Click on a 2D chart to set sentiment coordinates
- Save and move to the next poem

Admins can view progress, manage assignments, and download all annotations.
