import requests

# Example of a GET request using requests
try:
    response = requests.get('https://api.getprojects.ai/api/v2/fetchServiceBasedAgencyListPublic/skills/javascript-developers/web-developers/?offset=0&limit=20')
    response.raise_for_status()  # Raise an exception for HTTP errors
    print(response.json())
except requests.exceptions.RequestException as e:
    print(f"Error: {e}")


