import mysql.connector
import my_secrets
import calendar
from datetime import datetime, timedelta, date
import os
import re

# Valid US states
usStates = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

inputDateFormat = "%m/%d/%Y"
dbDateFormat = "%Y%m%d"

def validateEmptyValue(input_value):
    while True:
        if input_value:
            break
        else:
            input_value = input(f"{'Invalid Entry: value can not be empty.':<80}")
    return input_value

def getInputMonthAndYear():
    
    input_month = input(f"""{"Month (1-12 - ex: '1' for January)":<60}""")
    # validate 
    while True:
        if input_month.isdigit() and int(input_month) in range(1,13):
            break
        else:
            input_month = input(f"{'Invalid Entry: Enter 1-12':<60}")

    input_month = input_month if len(input_month) == 2 else '0'+input_month 

    input_year = input(f"""{"Year (XXXX - ex: '2024')":<60}""") 
    # validate S
    while True:
        if input_year.isdigit() and len(input_year) == 4: 
            break
        else: 
            input_year = input(f"{'Invalid Entry: Year must contain exactly 4 digits.':<60}")

    return (input_month, input_year)

def validatePhoneNumber(phoneNumber):

    # Extract digits using regex
    digits = re.sub(r'\D', '', phoneNumber)
    
    if len(digits) != 10:
        return False
    
    return True

def formatPhoneNumber(phoneNumber):

    # Extract digits using regex
    digits = re.sub(r'\D', '', phoneNumber)
    
    if len(digits) != 10:
        return False
    
    # Format the phone number
    formattedPhoneNumber = f"({digits[:3]}){digits[3:6]}-{digits[6:]}"
    
    return formattedPhoneNumber

def validDate(dateStr, dateFormat="%Y%m%d"):
    try:
        datetime.strptime(dateStr, dateFormat)
        return True
    except ValueError:
        return False

def validEmail(email):
    # pattern for a valid email
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if re.match(pattern, email):
        return True
    else:
        return False
    
def getData(query, params):

    # ➢ Establish a connection to the DB
    connection = mysql.connector.connect(database='creditcard_capstone', user=my_secrets.mysql_username, password=my_secrets.mysql_password)
    cursor=connection.cursor()

    cursor.execute(query, params)
    records = cursor.fetchall() 

    # ➢ Close connection
    cursor.close()
    connection.close()

    return records

def updateCustomerDetail(ssn, field, value):
    # ➢ Establish a connection to the DB
    connection = mysql.connector.connect(database='creditcard_capstone', user=my_secrets.mysql_username, password=my_secrets.mysql_password)
    cursor=connection.cursor()

    # ➢ For the user with the given ssn, set values for the given column.
    update_query = "UPDATE cdw_sapp_customer SET {}=%s, LAST_UPDATED=NOW() WHERE ssn=%s".format(field)
    cursor.execute(update_query, (value, ssn))
    connection.commit()

    # ➢ Close connection before return statement
    cursor.close()
    connection.close()

    return True

def saveCustomerDetails(ssn, data):

    # ➢ Establish a connection to the DB
    connection = mysql.connector.connect(database='creditcard_capstone', user=my_secrets.mysql_username, password=my_secrets.mysql_password)
    cursor=connection.cursor()

    # ➢ For the user with the given ssn, set values for the given columns.
    setColumns = '=%s, '.join([k for k in data]) + '=%s '
    update_query = "UPDATE cdw_sapp_customer SET {}, LAST_UPDATED=NOW() WHERE ssn=%s".format(setColumns)
    params = tuple(data.values()) + (ssn,)
    cursor.execute(update_query, params)
    connection.commit()

    # ➢ Close connection before return statement
    cursor.close()
    connection.close()

    return True

def getMMonthName(month_number):
    if 1 <= month_number <= 12:
        return calendar.month_name[month_number]
    else:
        return "Invalid month number"

def getDueDate(month, year):
    billDate = date(year, month, 1)
    return (billDate + timedelta(weeks=5)).strftime("%Y/%m/%d")

def getFormattedDate(strDate, fromFormat="%Y%m%d", toFormat="%m/%d/%Y"):
    parsed_date = datetime.strptime(strDate, fromFormat)
    return parsed_date.strftime(toFormat)

def clearScreen():
    if os.name == 'nt':  # For Windows
        os.system('cls')
    else:  # For Unix/Linux/Mac
        os.system('clear')