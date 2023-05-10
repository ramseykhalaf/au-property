import requests
import pandas as pd

# Set the URL of the Australian house price open data API.
url = "https://api.data.gov.au/data/dataset/1015085/resource/5d873904-50e5-489b-841b-46986382813b"

# Make a request to the API.
response = requests.get(url)

# Check the response status code.
if response.status_code == 200:
    # The request was successful.
    data = response.json()

    # Create a Pandas DataFrame from the JSON data.
    df = pd.DataFrame(data)

    # Save the DataFrame to a CSV file.
    df.to_csv("australian_house_prices.csv", index=False)
else:
    # The request failed.
    print("Request failed with status code:", response.status_code)
