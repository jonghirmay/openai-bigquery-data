from openai import OpenAI
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import bigquery
load_dotenv()

# initialize client for OPENAI API
api_key = os.getenv('OPENAI_API_KEY')
client = OpenAI(api_key=api_key)

#GCP credentials with json file path as key_path
key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PATH")
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

#initializes bigQuery client
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)


def getServices():
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant designed to provide the output in an Array."},
    		{"role": "user", "content": f"List the top ten industries that pcgstockholm.se has most project experience in. I want the params an object 'industry' and 'score'. Provide a 'score' between 0 and 1 with two decimals. If there are fewer than 10 industries, fill the remaining slots with an object where 'industry' is empty and 'score' is 0."
}
		],
        temperature=0,
        max_tokens=250
        
	)

    print(response.choices[0].message.content)
    

getServices()