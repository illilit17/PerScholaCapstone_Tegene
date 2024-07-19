import pyspark
import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
import mysql.connector
import my_secrets  # file containing username and pass saved in same file where python.exe is installed

def transform_customers_df(customers_df):
    # Datatype changes:
    # - 'CUST_PHONE' -- long --> str
    # - 'LAST_UPDATED' -- str --> timestamp
    # - 'SSN' -- long --> int
    customers_df = customers_df.withColumn('CUST_PHONE', customers_df.CUST_PHONE.cast("string"))
    customers_df = customers_df.withColumn('LAST_UPDATED', customers_df.LAST_UPDATED.cast("timestamp"))
    customers_df = customers_df.withColumn('SSN', customers_df.SSN.cast("integer"))

    # Format Corrections:
    # - 'CUST_PHONE' -- "XXXXXXX" --> "(XXX)XXX-XXXX"
    #     missing 3 digits? --> add them myself (as advised) via randomization 
    # - 'FIRST_NAME' --> title case
    # - 'LAST_NAME' --> title case
    # - 'MIDDLE_NAME' --> lower case
    # - 'STREET_NAME' --> "'STREET_NAME', 'APT_NO'" as 'FULL_STREET_ADDRESS'
    # - 'CREDIT_CARD_NO' --> 'Credit_card_no'
    customers_df = customers_df.withColumn('CUST_PHONE', psf.concat(customers_df.CUST_PHONE, psf.floor(psf.rand() * 900 + 100)))
    customers_df = customers_df.withColumn('CUST_PHONE', psf.format_string("(%s)%s-%s", psf.substring(customers_df.CUST_PHONE, 1, 3), psf.substring(customers_df.CUST_PHONE, 4, 3), psf.substring(customers_df.CUST_PHONE, 7, 4)))
    customers_df = customers_df.withColumn('FIRST_NAME', psf.initcap(customers_df.FIRST_NAME))
    customers_df = customers_df.withColumn('LAST_NAME', psf.initcap(customers_df.LAST_NAME))
    customers_df = customers_df.withColumn('MIDDLE_NAME', psf.lower(customers_df.MIDDLE_NAME))
    customers_df = customers_df.withColumn('FULL_STREET_ADDRESS', psf.concat_ws(' ', customers_df.APT_NO, customers_df.STREET_NAME))
    customers_df = customers_df.withColumn('Credit_card_no', customers_df.CREDIT_CARD_NO)

    # Final Selected Columns
    return customers_df.select('SSN', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'Credit_card_no', 'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_STATE', 'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL', 'LAST_UPDATED')

def transform_cc_df(cc_df):
    # Datatype changes:
    # - 'BRANCH_CODE' -- long --> int
    # - 'CUST_SSN'  -- long --> int
    # - 'TRANSACTION_ID' -- long --> int
    # - 'DAY' -- long --> str
    # - 'MONTH' -- long --> str
    # - 'YEAR' -- long --> str
    cc_df = cc_df.withColumn('BRANCH_CODE', cc_df.BRANCH_CODE.cast("integer"))
    cc_df = cc_df.withColumn('CUST_SSN', cc_df.CUST_SSN.cast("integer"))
    cc_df = cc_df.withColumn('TRANSACTION_ID', cc_df.TRANSACTION_ID.cast("integer"))
    cc_df = cc_df.withColumn('DAY', cc_df.DAY.cast("string"))
    cc_df = cc_df.withColumn('MONTH', cc_df.MONTH.cast("string"))
    cc_df = cc_df.withColumn('YEAR', cc_df.YEAR.cast("string"))

    # Format Corrections:
    # - 'TIMEID' --> "'YEAR''MONTH''DAY'" (so it looks like: YYYYMMDD)
    #     - first complete:
    #         - 'MONTH' --> "0'MONTH'" iff len('MONTH')==1
    #         - 'DAY' --> "0'DAY'" iff len('DAY')==1
    # - 'CREDIT_CARD_NO' --> 'CUST_CC_NO'
    cc_df = cc_df.withColumn('MONTH', psf.when(psf.length(cc_df.MONTH) == 1, psf.concat(psf.lit('0'), cc_df.MONTH)).otherwise(cc_df.MONTH))
    cc_df = cc_df.withColumn('DAY', psf.when(psf.length(cc_df.DAY) == 1, psf.concat(psf.lit('0'), cc_df.DAY)).otherwise(cc_df.DAY))
    cc_df = cc_df.withColumn('TIMEID', psf.format_string("%s%s%s", cc_df.YEAR, cc_df.MONTH, cc_df.DAY))
    cc_df = cc_df.withColumn('CUST_CC_NO', cc_df.CREDIT_CARD_NO)

    # Final Selected Columns
    return cc_df.select('CUST_CC_NO', 'TIMEID', 'CUST_SSN', 'BRANCH_CODE', 'TRANSACTION_TYPE', 'TRANSACTION_VALUE', 'TRANSACTION_ID')

def transform_branch_df(branch_df):
    # Datatype changes:
    # - 'BRANCH_CODE' -- long --> int
    # - 'BRANCH_ZIP' -- long --> str
    # - 'LAST_UPDATED' -- str --> timestamp
    branch_df = branch_df.withColumn('BRANCH_CODE', branch_df.BRANCH_CODE.cast("integer"))
    branch_df = branch_df.withColumn('BRANCH_ZIP', branch_df.BRANCH_ZIP.cast("string"))
    branch_df = branch_df.withColumn('LAST_UPDATED', branch_df.LAST_UPDATED.cast("timestamp"))

    # Format Corrections:
    # - 'BRANCH_PHONE' -- "XXXXXXXXXX" --> "(XXX)XXX-XXXX"
    # - 'BRANCH_ZIP'
    #     --> "0'BRANCH_ZIP'" iff len('BRANCH_ZIP')==4
    #     --> "99999" iff null
    branch_df = branch_df.withColumn('BRANCH_PHONE', psf.format_string("(%s)%s-%s", psf.substring(branch_df.BRANCH_PHONE, 1, 3), psf.substring(branch_df.BRANCH_PHONE, 4, 3), psf.substring(branch_df.BRANCH_PHONE, 7, 4)))
    branch_df = branch_df.withColumn('BRANCH_ZIP', psf.when(psf.length(branch_df.BRANCH_ZIP) == 4, psf.concat(psf.lit('0'), branch_df.BRANCH_ZIP)).when(psf.isnull(branch_df.BRANCH_ZIP), psf.lit('99999')).otherwise(branch_df.BRANCH_ZIP))

    # Final Selected Columns
    return branch_df.select('BRANCH_CODE', 'BRANCH_NAME', 'BRANCH_STREET', 'BRANCH_CITY', 'BRANCH_STATE', 'BRANCH_ZIP', 'BRANCH_PHONE', 'LAST_UPDATED')

def generate_db(db_name):
    # Connect to MySQL server, write and execute query, close connection
    connection = mysql.connector.connect(host='localhost', user=my_secrets.mysql_username, password=my_secrets.mysql_password)
    cursor = connection.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {db_name};")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
    cursor.close()
    connection.close()

def generate_table(db, tbl_name, tbl_schema):
    # Connect to database
    connection = mysql.connector.connect(database=db, user=my_secrets.mysql_username, password=my_secrets.mysql_password)
    cursor = connection.cursor()

    # Create table with given schema
    generatetable_query = f"CREATE TABLE IF NOT EXISTS {tbl_name}({tbl_schema});"
    cursor.execute(generatetable_query)

    # Close connection
    cursor.close()
    connection.close()

def main():
    # Initialize Spark session
    spark = SparkSession.builder.getOrCreate()

    # File paths
    customers_file = "cdw_sapp_customer.json"
    cc_file = "cdw_sapp_credit.json"
    branches_file = "cdw_sapp_branch.json"

    # Read JSON files as dataframes
    customers_df = spark.read.json(customers_file, multiLine=True)
    cc_df = spark.read.json(cc_file, multiLine=True)
    branch_df = spark.read.json(branches_file, multiLine=True)

    # Transform dataframes
    customers_df = transform_customers_df(customers_df)
    cc_df = transform_cc_df(cc_df)
    branch_df = transform_branch_df(branch_df)

    # Database and table names
    db = 'creditcard_capstone'
    tbl_cust = 'CDW_SAPP_CUSTOMER'
    tbl_cc = 'CDW_SAPP_CREDIT_CARD'
    tbl_branch = 'CDW_SAPP_BRANCH'

    # Table schemas
    sch_cust = """
        SSN INT,
        FIRST_NAME VARCHAR(50),
        MIDDLE_NAME VARCHAR(50),
        LAST_NAME VARCHAR(50),
        Credit_card_no VARCHAR(16),
        FULL_STREET_ADDRESS VARCHAR(50),
        CUST_CITY VARCHAR(50),
        CUST_STATE VARCHAR(2),
        CUST_COUNTRY VARCHAR(50),
        CUST_ZIP VARCHAR(5),
        CUST_PHONE VARCHAR(15),
        CUST_EMAIL VARCHAR(50),
        LAST_UPDATED TIMESTAMP,
        PRIMARY KEY (SSN)"""
    sch_cc = """
        CUST_CC_NO VARCHAR(16),
        TIMEID VARCHAR(8),
        CUST_SSN INT,
        BRANCH_CODE INT,
        TRANSACTION_TYPE VARCHAR(50),
        TRANSACTION_VALUE DOUBLE,
        TRANSACTION_ID INT,
        PRIMARY KEY (TRANSACTION_ID),
        FOREIGN KEY (CUST_SSN) REFERENCES CDW_SAPP_CUSTOMER(SSN),
        FOREIGN KEY (BRANCH_CODE) REFERENCES CDW_SAPP_BRANCH(BRANCH_CODE)"""
    sch_branch = """
        BRANCH_CODE INT,
        BRANCH_NAME VARCHAR(50),
        BRANCH_STREET VARCHAR(50),
        BRANCH_CITY VARCHAR(50),
        BRANCH_STATE VARCHAR(2),
        BRANCH_ZIP VARCHAR(5),
        BRANCH_PHONE VARCHAR(15),
        LAST_UPDATED TIMESTAMP,
        PRIMARY KEY (BRANCH_CODE)"""

    # Create database and tables
    generate_db(db)
    generate_table(db, tbl_cust, sch_cust)
    generate_table(db, tbl_branch, sch_branch)
    generate_table(db, tbl_cc, sch_cc)

    # PySpark MySQL connection properties
    mysql_props = {"user": my_secrets.mysql_username, "password": my_secrets.mysql_password, "driver": "com.mysql.cj.jdbc.Driver"}
    mysql_url = f"jdbc:mysql://localhost:3306/{db}"

    # Load transformed dataframes to tables
    customers_df.write.jdbc(url=mysql_url, table=tbl_cust, mode="append", properties=mysql_props)
    branch_df.write.jdbc(url=mysql_url, table=tbl_branch, mode="append", properties=mysql_props)
    cc_df.write.jdbc(url=mysql_url, table=tbl_cc, mode="append", properties=mysql_props)


if __name__ == "__main__":
    main()
