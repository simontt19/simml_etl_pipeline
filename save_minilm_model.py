from sentence_transformers import SentenceTransformer

import pickle

# Load the model
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

# Save the loaded model instance to a file
with open('model.pkl', 'wb') as file:
    pickle.dump(model, file)