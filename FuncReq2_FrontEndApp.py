import queries as q
import helper as h

def updateFirstName(ssn, field, updatedColumns):
    newValue = input("Enter new first name: ") # error proof
    updatedColumns[field] = newValue
    return True

def updateCustomerDetail(ssn, field, label, format, updatedColumns):
    input_value = input(f"{'Enter new {} {}'.format(label, format):<80}")
    
    newValue = input_value
    if field in (editOptions['1'][0], editOptions['3'][0]):
        input_value = h.validateEmptyValue(input_value)
        newValue = input_value.capitalize()
    elif field == editOptions['2'][0]:
        input_value = h.validateEmptyValue(input_value)
        newValue = input_value.lower()
    elif field == editOptions['4'][0]:
        while True:
            isValid = h.validatePhoneNumber(input_value)
            if isValid:
                break
            else:
                input_value = input(f"{'Invalid Entry: Phone number must contain exactly 10 digits.':<80}")
        newValue = h.formatPhoneNumber(input_value)
    elif field == editOptions['5'][0]:
        while True:
            isValid = h.validEmail(input_value)
            if isValid:
                break
            else:
                input_value = input(f"{'Invalid Entry: Email should be in XXXX@XXX.XXX format.':<80}")
        newValue = input_value
    elif field == editOptions['8'][0]:
        while True:
            if input_value.upper() in h.usStates:                
                break
            else:
                input_value = input(f"{'Invalid Entry: State must be a valid US State (abbreviated).':<80}")
        newValue = input_value.upper()
    elif field == editOptions['9'][0]:
        while True:
            if input_value.isdigit() and len(input_value) == 5:
                break
            else:
                input_value = input(f"{'Invalid Entry: Zip Code must contain exactly 5 digits.':<80}")
        newValue = input_value
    else:
        input_value = h.validateEmptyValue(input_value)
        newValue = input_value

    updatedColumns[field] = newValue
    return True

def saveCustomerDetails(ssn, data):
    h.updateCustomerDetail(ssn, data)
    return True

editOptions = {
    '1': ['FIRST_NAME', 'First Name', updateCustomerDetail, ''],
    '2': ['MIDDLE_NAME', 'Middle Name', updateCustomerDetail,''],
    '3': ['LAST_NAME', 'Last Name', updateCustomerDetail,''],
    '4': ['CUST_PHONE', 'Phone', updateCustomerDetail,'((XXX)XXX-XXXX)'],
    '5': ['CUST_EMAIL', 'Email', updateCustomerDetail,'XXXX@XXX.XXX'],
    '6': ['FULL_STREET_ADDRESS', 'Full Street Address', updateCustomerDetail, ''],
    '7': ['CUST_CITY', 'City', updateCustomerDetail,''],
    '8': ['CUST_STATE', 'State', updateCustomerDetail,'(XX)'],
    '9': ['CUST_ZIP', 'Zip Code', updateCustomerDetail, '(XXXXX - numbers only)'],
    '10': ['CUST_COUNTRY', 'Country', updateCustomerDetail, ''],
    '11': ['SAVE', 'Save', saveCustomerDetails, ''],
    '12': ['CANCEL', 'Cancel/Close', '', '']
}

# 2.1 Retrieve customer transactions for a given zip code and transaction month (descending order by transaction date)
def getCustomerTransactionsByZip():

    print("Enter zip code and transaction month and year to retrieve customer transactions\n")
    input_zip = input(f"""{"Zip Code (XXXXX - ex: '01234')":<60}""")
    # validate
    while True:
        if input_zip.isdigit() and len(input_zip) == 5:
            break
        else:
            input_zip = input(f"{'Invalid Entry: Zip Code must contain exactly 5 digits.':<60}")

    input_month, input_year = h.getInputMonthAndYear()

    records = h.getData(q.customerTransactionsByZip, (input_zip, input_month, input_year))
    
    # ➢ Print list of transactions
    if (len(records) > 0):
        print(f"\nTransactions in {input_month}/{input_year} for customers from zip code {input_zip}\n")
        print(f"{'Date':<20}{'Description':<20}{'Amount':>10}\tName")
        for row in records:
            print(f"{h.getFormattedDate(row[0]):<20}{row[1]:<20}{'${}'.format(row[2]):>10}\t{row[4]} {row[5]} {row[6]}")
        print()
    else:
        print(f"\n\n********** No records **********\n\n")

def displayCustomerDetails(records):
    
    # ➢ Print customer details
    if (len(records) > 0):
        print(f"\nCustomer details\n")
        row = records[0]
        print(f"{'First Name':<20}{row[0]:<20}")
        print(f"{'Middle Name':<20}{row[1]:<20}")
        print(f"{'Last Name':<20}{row[2]:<20}")
        print(f"{'Phone':<20}{row[3]:<20}")
        print(f"{'Email':<20}{row[4]:<20}")
        print(f"{'Full Street Address':<20}{row[5]:<20}")
        print(f"{'City':<20}{row[6]:<20}")
        print(f"{'State':<20}{row[7]:<20}")
        print(f"{'Zip Code':<20}{row[8]:<20}")
        print(f"{'Country':<20}{row[9]:<20}")
        print()
    else:
        print(f"\n\n********** No records **********\n\n")

def getCustomerDetailsByPhone():

    print("Enter customer phone number to retrieve customer details\n")
    
    input_phone = input(f"{'Phone Number (XXXXXXXXXX)':<60}")
    #validate
    while True:
        isValid = h.validatePhoneNumber(input_phone)
        if isValid:
            break
        else:
            input_phone = input(f"{'Phone number must contain exactly 10 digits.':<60}")

    input_phone = h.formatPhoneNumber(input_phone)
    return h.getData(q.customerDetails, (h.formatPhoneNumber(input_phone),))
    
# 2.2.1 Get the existing account details of a customer.
def getCustomerDetails():

    customerDetails = getCustomerDetailsByPhone()
    if customerDetails:
        displayCustomerDetails(customerDetails)   

# 2.2.2 Modify the existing account details of a customer.
def modifyCustomerDetails():

    records = getCustomerDetailsByPhone()
    if records:
        displayCustomerDetails(records)

        if len(records) > 0:
            customerRec = records[0]
            menu = ""
            print(f"{'Edit customer details':^150}")
            for k, v in editOptions.items():
                menuItem = '{}. {}'.format(k, v[1])
                # menu = menu + f"""{menuItem:<25}{"\n" if k=="6" else ""}"""
                menu = menu + f"{menuItem:<25}"
                if k=="6":
                    menu = menu + "\n"
            
            print(menu)
            updatedColumns = {}

            while True:
                choice = input(f"{'Please select an option (1-12)    ':>150}")
                if choice == '12': # Close
                    updatedColumns = {}
                    break
                if choice == '11': # Save
                    h.saveCustomerDetails(customerRec[10], updatedColumns)
                    customerDetails = h.getData(q.customerDetailsBySSN, (customerRec[10],))
                    displayCustomerDetails(customerDetails)
                    break
                elif choice in editOptions:
                    print()
                    option = editOptions.get(choice)
                    updateCustomerDetail(customerRec[10], option[0], option[1], option[3], updatedColumns)
                else:
                    print(f"{'Invalid Entry':>150}")

# 2.2.3 Generate a monthly bill for a credit card number for a given month and year.
def generateCreditCardMonthlyBill():

    print("Enter Credit Card number, month and year to generate monthly bill\n")
    input_cc = input(f"{'Credit Card number (enter 16 consecutive digits)':<60}")
    # validate 
    while True:
        if input_cc.isdigit() and len(input_cc) == 16:
            break
        else:
            input_cc = input(f"{'Credit Card number must contain exactly 16 digits.':<60}")

    input_month, input_year = h.getInputMonthAndYear()

    records = h.getData(q.creditCardMonthlyTransactions, (input_cc, input_month, input_year))

    # ➢ Print monthly bill for a credit card
    if (len(records) > 0):
        row0 = records[0] 
        print(f"\n\n{'Monthly Bill':^60}")
        print(f"\n{row0[12]}\n\n{row0[14]}\n{row0[15]}, {row0[16]} {row0[17]}")
        print(f"\n\n{row0[4]} {row0[5]} {row0[6]}\n{row0[7]}\n{row0[8]}, {row0[9]} {row0[10]}\n{row0[11]}")
        formatted_cc = ' '.join(row0[3][i:i+4] for i in range(0, len(row0[3]), 4))

        print(f"\n{' '.join(['Account#', formatted_cc]):>60}")
        print(f"{', '.join([h.getMMonthName(int(input_month)), input_year]):>60}")

        print(f"\n\nTransactions\n")
        print(f"{'Date':<20}{'Description':<20}{'Amount':>20}")

        total = 0
        for row in records:
            total += row[2]
            print(f"{h.getFormattedDate(row[0]):<20}{row[1]:<20}{'${}'.format(row[2]):>20}")
          
        print(f"\n{'Payment Due: ${}'.format(total):>60}")

    else:
        print(f"\n\n********** No records **********\n\n")

# 2.2.4 Retrieve transactions for a given customer and transaction periods in transaction date order 
def getCustomerTransactionsByPeriod():

    print("Enter customer phone number and transaction dates to retrieve customer transactions\n")

    input_phone = input(f"{'Phone Number (XXXXXXXXXX)':<60}")
    #validate
    while True:
        isValid = h.validatePhoneNumber(input_phone)
        if isValid:
            break
        else:
            input_phone = input(f"{'Phone number must contain exactly 10 digits.':<60}")

    input_phone = h.formatPhoneNumber(input_phone)

    input_start_date = input(f"{'Start Date (MM/DD/YYYY)':<60}")
    #validate
    while True:
        isValid = h.validDate(input_start_date, h.inputDateFormat)
        if isValid:
            break
        else:
            input_start_date = input(f"{'Invalid Entry: Enter Start Date with the correct format.':<60}")

    input_end_date = input(f"{'End Date (MM/DD/YYYY)':<60}")
    #validate
    while True:
        isValid = h.validDate(input_end_date, h.inputDateFormat)
        if isValid:
            break
        else:
            input_end_date = input(f"{'Invalid Entry: Enter End date with the correct format.':<60}")

    start_date = h.getFormattedDate(input_start_date, h.inputDateFormat, h.dbDateFormat)
    end_date = h.getFormattedDate(input_end_date, h.inputDateFormat, h.dbDateFormat)

    records = h.getData(q.customerTransactionsByPeriod, (input_phone, start_date, end_date))
    
    # ➢ Print list of transactions
    if (len(records) > 0):
        print(f"\nTransactions between {input_start_date} and {input_end_date} for {records[0][3]} {records[0][4]} {records[0][5]}\n")
        # print(f"\nTransactions between {input_start_date} and {input_end_date} for {input_phone}\n")
        print(f"{'Date':<20}{'Description':<20}{'Amount':>20}")
        for row in records:
            print(f"{h.getFormattedDate(row[0]):<20}{row[1]:<20}{'${}'.format(row[2]):>20}")
        print()
    else:
        print(f"\n\n********** No records **********\n\n")

def displayMenu():
    h.clearScreen()
    print(f"\n{'*****Credit Card Application Main Menu*****':^100}")
    print("1. Get Customer Details")
    print("2. Edit Customer Details")
    print("3. Get Customer Transactions by Zip Code, Month & Year")
    print("4. Get Customer Transactions by Period")
    print("5. Generate Monthly Bill for a Credit Card")
    print("6. Exit")

def defaultOption():
    input("Invalid option, please try again")

def main():
    menuOptions = {
        '1': getCustomerDetails,
        '2': modifyCustomerDetails,
        '3': getCustomerTransactionsByZip,
        '4': getCustomerTransactionsByPeriod,
        '5': generateCreditCardMonthlyBill,
    }
    
    while True:
        displayMenu()
        choice = input(f"{'Please select an option (1-6):     ':>50}")
        
        if choice == '6':
            print("Exiting the app. Goodbye!")
            break
        else:
            print()
            menuOptions.get(choice, defaultOption)()

        if choice in menuOptions:
            input("Press any key to continue")
# run app
if __name__ == "__main__":
    main()