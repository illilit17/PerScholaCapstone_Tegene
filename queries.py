    # ➢ Query database and retrieve list of transactions
customerTransactionsByZip = """
    SELECT cc.timeid, cc.transaction_type, cc.transaction_value, cc.cust_cc_no, c.first_name, c.middle_name, c.last_name
    FROM cdw_sapp_credit_card cc
    INNER JOIN cdw_sapp_customer c
        ON cc.cust_ssn = c.ssn
    WHERE c.cust_zip = %s 
        AND SUBSTRING(cc.timeid, 5, 2) = %s
        AND SUBSTRING(cc.timeid, 1, 4) = %s
    ORDER BY cc.timeid DESC;
"""

creditCardMonthlyTransactions = """
    SELECT cc.timeid, cc.transaction_type, cc.transaction_value, cc.cust_cc_no, c.first_name, c.middle_name, c.last_name,
        c.full_street_address, c.cust_city, c.cust_state, c.cust_zip, c.cust_country, b.branch_name, b.branch_phone, b.branch_street, 
        b.branch_city, b.branch_state, b.branch_zip
    FROM cdw_sapp_credit_card cc
    JOIN cdw_sapp_customer c
      ON cc.cust_ssn = c.ssn
	JOIN cdw_sapp_branch b
      ON b.branch_code = cc.branch_code
    WHERE cc.cust_cc_no = %s
        AND SUBSTRING(cc.timeid, 5, 2) = %s
        AND SUBSTRING(cc.timeid, 1, 4) = %s
    ORDER BY cc.timeid;
"""

# ➢ Query database and retrieve list of transactions
customerTransactionsByPeriod = """
    SELECT cc.timeid, cc.transaction_type, cc.transaction_value, c.first_name, c.middle_name, c.last_name
    FROM cdw_sapp_credit_card cc
    INNER JOIN cdw_sapp_customer c
        ON cc.cust_ssn = c.ssn
    WHERE c.cust_phone=%s 
      AND cc.timeid BETWEEN %s AND %s
    ORDER BY cc.timeid
"""

# ➢ Retrieve customer details
customerDetails = """
    SELECT first_name, middle_name, last_name, cust_phone, cust_email, full_street_address, cust_city, cust_state, cust_zip, cust_country, ssn
    FROM cdw_sapp_customer
    WHERE cust_phone=%s 
"""

customerDetailsBySSN = """
    SELECT first_name, middle_name, last_name, cust_phone, cust_email, full_street_address, cust_city, cust_state, cust_zip, cust_country, ssn
    FROM cdw_sapp_customer
    WHERE ssn=%s 
"""