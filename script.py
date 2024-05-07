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

def getDescription(domain):
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant designed to output in an Array"},
    		{"role": "user", "content": f"Provide a concise overview of {domain}, including a brief description and a summary of their primary services."}
		],		
        temperature=0,
        max_tokens=100
        
	)

    return response.choices[0].message.content
    
    
def getServices(domain):
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant designed to provide the output in an Array."},
    		{"role": "user", "content": f"List the top five services offered by {domain}, ranked by importance or popularity. I want the params in an object 'service' and 'score'. Provide a 'score' between 0 and 1 with two decimals. If there are fewer than five industries, fill the remaining slots with an object where 'service' is empty and 'score' is 0."
}
		],
        temperature=0,
        max_tokens=250
        
	)

    return response.choices[0].message.content


def getProjectExperience(domain):
        response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant designed to provide the output in an Array."},
    		{"role": "user", "content": f"List the top 10 industries that {domain} has most project experience in. I want the params an object 'industry' and 'score'. Provide a 'score' between 0 and 1 with two decimals. If there are fewer than 10 industries, fill the remaining slots with an object where 'industry' is empty and 'score' is 0."
}
		],
        temperature=0,
        max_tokens=250
        
	)
        return response.choices[0].message.content
    
    

def fetch_urls_update_bq():
    query = 'SELECT column FROM `project-id.dataset.table` LIMIT 1000'
    query_job = bq_client.query(query)
    results = query_job.result()
    
    for row in results:
        domain = row['companyUrl']
        print(domain)
        
        try:
            description = getDescription(domain)
            if description:
                print(f"Updating {domain} with description: {description}")
                update_query = f"""
                UPDATE `project-id.dataset.table`
                SET column = @description
                WHERE column = @domain
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                    bigquery.ScalarQueryParameter("description", "STRING", description),
                    bigquery.ScalarQueryParameter("domain", "STRING", domain),
                ])
                update_job = bq_client.query(update_query, job_config=job_config)  # Make an API request.
                update_job.result()  # Wait for the request to complete.
                print("Description updated.")
                
        except AttributeError as e:
            print(f"Error is {e}")
            
        try:
            topServices = getServices(domain)
            if topServices:
                print(f"Updating {domain} with topServices: {topServices}")
                update_query = f"""
                UPDATE `project-id.dataset.table`
                SET column = @topServices
                WHERE column = @domain
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                    bigquery.ScalarQueryParameter("topServices", "STRING", topServices),
                    bigquery.ScalarQueryParameter("domain", "STRING", domain),
                ])
                update_job = bq_client.query(update_query, job_config=job_config)  # Make an API request.
                update_job.result()  # Wait for the request to complete.
                print("topSkills updated.")
                
        except AttributeError as e:
            print(f"Error is {e}")
            
        try: 
            topProjectExperience = getProjectExperience(domain)
            if topProjectExperience:
                print(f"Updating {domain} with topProjectExperience: {topProjectExperience}")
                update_query = f"""
                UPDATE `project-id.dataset.table`
                SET column = @topProjectExperience
                WHERE column = @domain
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                    bigquery.ScalarQueryParameter("topProjectExperience", "STRING", topProjectExperience),
                    bigquery.ScalarQueryParameter("domain", "STRING", domain),
                ])
                update_job = bq_client.query(update_query, job_config=job_config)  # Make an API request.
                update_job.result()  # Wait for the request to complete.
                print("topSkills updated.")
                
            
        except AttributeError as e:
            print(f'Error is {e}')
            



    
if __name__ == '__main__':
    fetch_urls_update_bq()
		



