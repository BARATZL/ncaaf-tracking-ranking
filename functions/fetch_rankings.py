import requests

def fetch_rankings_from_espn():
    """Fetch the latest college football rankings from ESPN API."""
    url = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings"
    response = requests.get(url)

    if not response.ok:
        raise Exception(f"❌ API error: {response.status_code}")
    
    print("✅ ESPN API connection successful.")
    return response.json()
