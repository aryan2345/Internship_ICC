import pymysql

# Database connection configurations
source_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '1tal1csP!',
    'database': 'eicurrency1'
}

destination_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '1tal1csP!',
    'database': 'admincampaign'
}

# Connect to source database
source_conn = pymysql.connect(**source_db_config)
source_cursor = source_conn.cursor()

# Connect to destination database
destination_conn = pymysql.connect(**destination_db_config)
destination_cursor = destination_conn.cursor()

try:
    # Fetch data from the bank table
    source_cursor.execute("SELECT * FROM bank")
    banks = source_cursor.fetchall()

    # Fetch data from the product_binsegment table
    destination_cursor.execute("SELECT * FROM product_binsegment")
    product_binsegments = destination_cursor.fetchall()

    # Fetch data from the exception_list table
    source_cursor.execute("SELECT * FROM exception_list")
    exception_list = source_cursor.fetchall()

    for bank in banks:
        id, name, card_name, card_type, threshold_miles, threshold_amount, logtime, startDate, endDate, \
        card_image_path, status, card_text, product_bin, image_name, monthlyCap, yearlyCap, \
        domestic_formula, international_formula, country, currency, partnerMileConfig, timeMileConfig, \
        priceToPointFormula, max_redemptions, transactionCap, transaction_type, currency_code, bonusMiles, eu_formula = bank

        # Insert into product_binsegment table with hardcoded currency as 35
        product_binsegment_query = """
        INSERT INTO product_binsegment (currency, bin, segment_type, segment_no)
        VALUES (%s, %s, %s, %s)
        """
        destination_cursor.execute(product_binsegment_query, (35, card_type, 'Non-Segment', 0))

        # Get the last inserted id from product_binsegment
        last_inserted_id = destination_cursor.lastrowid

        # Check if the inserted bin already exists in the product_binsegment table
        existing_bin_segment = next((p for p in product_binsegments if p[1] == card_type), None)

        if existing_bin_segment:
            bin_value = f"{existing_bin_segment[0]}_0"
        else:
            bin_value = f"{last_inserted_id}_0"

        # Insert into product_rule table with hardcoded values and dynamic bin values
        product_rule_query = """
        INSERT INTO product_rule (
            bin, earning_type, rewards_capping, dome_currency_slab, dom_currency_capping, 
            int_currency_slab, int_currency_capping, eu_slab, eu_capping, mcc_slab, mcc_capping, 
            mid_slab, mid_capping, transaction_code, rule_section_grid, binPlusSegment, maker_checker
        ) VALUES (
            %s, 'Daily Earn', 'Yearly,Monthly,Credit limit,Product/BIN level', 'Non Slab', 'Yes', 
            'Non Slab', 'Yes', 'Non Slab', 'Yes', 'Non Slab', 'Yes', 
            'Non Slab', 'Yes', 'Transaction codes', 
            'Domestic Currency,International currency,EU,MCC,MID', %s, 0
        )
        """
        destination_cursor.execute(product_rule_query, (bin_value, card_type))

        # Get the last inserted id from product_rule
        last_product_rule_id = destination_cursor.lastrowid

        # Insert into rule_setupparameters table
        rule_setupparameters_query = """
        INSERT INTO rule_setupparameters (
            product_ruleId, bin, domesticEarnRate, intCurrencyRate, euRate, binPlusSegment
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        destination_cursor.execute(rule_setupparameters_query, (
            last_product_rule_id, bin_value, domestic_formula, international_formula, eu_formula, card_type
        ))

        # Get the last inserted id from product_rule
        last_product_rule_id_rule = destination_cursor.lastrowid

        # Insert into rule_setupparametersslab table
        for exception in exception_list:
            ex_id, cardType, ex_type, code, formula, monthlyCap, yearlyCap, comments = exception

            # Find the corresponding product_rule_id and bin_plus_segment from the bank and product_binsegment tables

            if cardType == card_type:
                print(cardType, " ", card_type)
            if cardType == card_type:
                if ex_type == 5:
                    # Insert MCC related data
                    rule_setupparametersslab_query = """
                    INSERT INTO rule_setupparametersslab (
                        rule_setupParametersId, mcc_no, mcc_name, mcc_earnRate, bin, binPlusSegment, maker_checker, comments
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    destination_cursor.execute(rule_setupparametersslab_query, (
                        last_product_rule_id_rule, code, comments, formula, bin_value, card_type, 1, comments
                    ))

                elif ex_type == 6:
                    # Insert MID related data
                    rule_setupparametersslab_query = """
                    INSERT INTO rule_setupparametersslab (
                        rule_setupParametersId, mid_no, mid_name, mid_earnRate, bin, binPlusSegment, maker_checker, comments
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    destination_cursor.execute(rule_setupparametersslab_query, (
                        last_product_rule_id_rule, code, comments, formula, bin_value, card_type, 1, comments
                    ))

    # Commit changes
    destination_conn.commit()

finally:
    # Close connections
    source_cursor.close()
    source_conn.close()
    destination_cursor.close()
    destination_conn.close()
