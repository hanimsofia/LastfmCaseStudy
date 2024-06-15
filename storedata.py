import requests
from pymongo import MongoClient

def fetch_and_store(api_url, collection_name):
    client = MongoClient("mongodb+srv://22102228:wfocD5eeH1NkRmPA@cluster0.ik0nqey.mongodb.net/")
    db = client["chartdb"]
    collection = db[collection_name]

    # Iterate through 200 pages
    for page in range(1, 201):
        modified_url = f"{api_url}&page={page}"
        try:
            response = requests.get(modified_url)
            response.raise_for_status()
            data = response.json()
            key = 'artists' if 'artists' in data else 'tracks'
            if key in data and key[:-1] in data[key]:
                collection.insert_many(data[key][key[:-1]])
                print(f"Data from page {page} successfully inserted into {collection_name}.")
            else:
                print(f"No valid data found on page {page}.")
        
        except requests.RequestException as e:
            print(f"Request error on page {page}: {e}")
        except Exception as e:
            print(f"An error occurred on page {page}: {e}")

# APIs
api_url_artists = "http://ws.audioscrobbler.com/2.0/?method=chart.gettopartists&api_key=5039cf510492176d995bc6b62b251c6c&format=json"
api_url_tracks = "http://ws.audioscrobbler.com/2.0/?method=chart.gettoptracks&api_key=5039cf510492176d995bc6b62b251c6c&format=json"

# Fetch and store data for artists and tracks
fetch_and_store(api_url_artists, "topartistscollection")
fetch_and_store(api_url_tracks, "toptrackscollection")
