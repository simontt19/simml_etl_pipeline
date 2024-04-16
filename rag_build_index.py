from sentence_transformers import SentenceTransformer
from annoy import AnnoyIndex
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin import storage
import os
import pickle
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import time
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
firebase_path = "react-simml-firebase-adminsdk-qrzgh-165d958a7c.json"
# Check if the model is already saved
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

# Initialize Firebase Admin using the service account
cred = credentials.Certificate(firebase_path)
firebase_admin.initialize_app(cred, {
    'storageBucket': "react-simml.appspot.com"
})
# Initialize Firestore and Storage instances
db = firestore.client()
bucket = storage.bucket()

def build_index():
    prompts_ref = db.collection('prompts')
    docs = prompts_ref.stream()

    embeddings = []
    document_ids = []
    
    for doc in docs:
        text = doc.get('title') + " " + doc.get('content')
        vec = text_to_vector(text)
        embeddings.append(vec)
        document_ids.append(doc.id)
    
    index = AnnoyIndex(len(embeddings[0]), 'angular')
    for i, vec in enumerate(embeddings):
        index.add_item(i, vec)
    index.build(10)
    
    # Save the index file locally
    index.save('index.ann')
    
    # Upload the index file to Firebase Storage
    blob = bucket.blob('index.ann')
    blob.upload_from_filename('index.ann')
    print("Index file uploaded to Firebase Storage")
    
    # Save the document_ids to Firestore
    index_ref = db.collection('index').document('annoy_index')
    index_ref.set({
        'document_ids': document_ids
    })
    print("Document IDs saved to Firestore")

# Function to convert text to vector
def text_to_vector(text):
    return model.encode(text)

if __name__ == "__main__":
    logger.info("Starting the script...")
    scheduler = BackgroundScheduler()
    scheduler.add_job(build_index, 'interval', minutes=5)
    scheduler.start()
    logger.info("Scheduler started. Running the main process every 5 minutes.")

    build_index()  # Run the main process immediately

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopping the script...")
        scheduler.shutdown()
        logger.info("Script stopped.")