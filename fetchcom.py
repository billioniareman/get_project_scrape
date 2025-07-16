import requests

def fetch_data_from_api(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json().get('data')
        # Print the whole data for debugging (optional)
        # print(data)
        company_names = []
        if isinstance(data, dict) and 'agencyList' in data:
            for company in data['agencyList']:
                name = company.get('company_id')
                if name:
                    company_names.append(name)
            print(company_names)
        else:
            print("No agencyList found in response.")
    except Exception as e:
        print(f"Error fetching data from API: {e}")

if __name__ == "__main__":
    # Change the URL below to your desired API endpoint
    api_url = "https://dev-api.getprojects.ai/api/v2/fetchServiceBasedAgencyListPublic/skills/nodejs-developers/web-developers/?offset=0&limit=20"
    fetch_data_from_api(api_url)
