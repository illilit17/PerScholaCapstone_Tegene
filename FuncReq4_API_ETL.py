import requests
import pyspark
from pyspark.sql import SparkSession
import my_secrets

def json_data_from_api(url):
    try:
        r = requests.get(url) # https request - access url api
        r.raise_for_status()  # Raises an HTTPError for bad responses

        # check response status is 200 ok
        if r.status_code == 200:
            return r.json() # return json data
        else:
            raise Exception(f"Unexpected status code: {r.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

def main():
    # URL for the loan data API
    baseurl = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json' 
    # no further endpoints or next pages; list of flat dictonaries; no pagination necessary

    # Fetch data from the API
    data = json_data_from_api(baseurl)
    if data is None:
        print("Failed to fetch data from API. Exiting...")
        return

    # Create PySpark DataFrame from the fetched data
    spark = SparkSession.builder.getOrCreate()
    loan_df = spark.createDataFrame(data)
    
    # PySpark MySQL connection properties
    db = 'creditcard_capstone'
    tbl_loan = 'CDW_SAPP_loan_application'
    mysql_props = {
        "user": my_secrets.mysql_username,
        "password": my_secrets.mysql_password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    mysql_url = f"jdbc:mysql://localhost:3306/{db}"

    # Load data from SparkSQL DataFrame into the database
    loan_df.write.jdbc(url=mysql_url, table=tbl_loan, mode="overwrite", properties=mysql_props)

if __name__ == "__main__":
    main()
