import pymysql
import argparse

# Database connection configurations
source_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '1tal1csP!',
    'database': 'ei'
}

destination_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '1tal1csP!',
    'database': 'admincampaign'
}

def transfer_data(currency_name):
    # Connect to source database
    source_conn = pymysql.connect(**source_db_config)
    source_cursor = source_conn.cursor()

    # Connect to destination database
    destination_conn = pymysql.connect(**destination_db_config)
    destination_cursor = destination_conn.cursor()

    try:
        # Open a file to write the queries
        with open('query.sql', 'w') as file:
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

                # Fetch the currency ID based on the passed currency_name
                destination_cursor.execute("SELECT id FROM currency WHERE name = %s;", (currency_name,))
                currency_record = destination_cursor.fetchone()

                # Check if currency is found
                if currency_record:
                    currency_id = currency_record[0]

                    # Insert into product_binsegment table
                    product_binsegment_query = f"""
                    INSERT INTO product_binsegment (currency, bin, segment_type, segment_no)
                    VALUES ({currency_id}, '{card_type}', 'Non-Segment', 0);
                    """
                    print("Writing query:", product_binsegment_query)
                    file.write(product_binsegment_query + '\n')
                    destination_cursor.execute(product_binsegment_query)

                    # Get the last inserted id from product_binsegment
                    last_inserted_id = destination_cursor.lastrowid

                    # Check if the inserted bin already exists in the product_binsegment table
                    existing_bin_segment = next((p for p in product_binsegments if p[1] == card_type), None)
                    bin_value = f"{existing_bin_segment[0]}_0" if existing_bin_segment else f"{last_inserted_id}_0"

                    # Insert into product_rule table
                    product_rule_query = f"""
                    INSERT INTO product_rule (
                        bin, earning_type, rewards_capping, dome_currency_slab, dom_currency_capping, 
                        int_currency_slab, int_currency_capping, eu_slab, eu_capping, mcc_slab, mcc_capping, 
                        mid_slab, mid_capping, transaction_code, rule_section_grid, binPlusSegment, maker_checker
                    ) VALUES (
                        '{bin_value}', 'Daily Earn', 'Yearly,Monthly,Credit limit,Product/BIN level', 'Non Slab', 'No', 
                        'Non Slab', 'No', 'Non Slab', 'No', 'Non Slab', 'No', 
                        'Non Slab', 'No', '{transaction_type}', 
                        'Domestic Currency,International currency,EU,MCC,MID', '{card_type}', 1
                    );
                    """
                    print("Writing query:", product_rule_query)
                    file.write(product_rule_query + '\n')
                    destination_cursor.execute(product_rule_query)

                    # Get the last inserted id from product_rule
                    last_product_rule_id = destination_cursor.lastrowid

                    # Insert into rule_setupparameters table
                    rule_setupparameters_query = f"""
                    INSERT INTO rule_setupparameters (
                        product_ruleId, bin, domesticEarnRate, intCurrencyRate, euRate, binPlusSegment
                    ) VALUES ({last_product_rule_id}, '{bin_value}', '{domestic_formula}', '{international_formula}', '{eu_formula}', '{card_type}');
                    """
                    print("Writing query:", rule_setupparameters_query)
                    file.write(rule_setupparameters_query + '\n')
                    destination_cursor.execute(rule_setupparameters_query)

                    # Get the last inserted id from rule_setupparameters
                    last_product_rule_id_rule = destination_cursor.lastrowid

                    # Insert into rule_setupparametersslab table based on exception_list
                    for exception in exception_list:
                        ex_id, cardType, ex_type, code, formula, monthlyCap, yearlyCap, comments = exception

                        if cardType == card_type:
                            if ex_type == 5:  # MCC related data
                                rule_setupparametersslab_query = f"""
                                INSERT INTO rule_setupparametersslab (
                                    rule_setupParametersId, mcc_no, mcc_name, mcc_earnRate, bin, binPlusSegment, maker_checker, comments
                                ) VALUES (
                                    {last_product_rule_id_rule}, '{code}', '{comments}', '{formula}', '{bin_value}', '{card_type}', 1, '{comments}'
                                );
                                """
                                print("Writing query:", rule_setupparametersslab_query)
                                file.write(rule_setupparametersslab_query + '\n')
                                destination_cursor.execute(rule_setupparametersslab_query)

                            elif ex_type == 6:  # MID related data
                                rule_setupparametersslab_query = f"""
                                INSERT INTO rule_setupparametersslab (
                                    rule_setupParametersId, mid_no, mid_name, mid_earnRate, bin, binPlusSegment, maker_checker, comments
                                ) VALUES (
                                    {last_product_rule_id_rule}, '{code}', '{comments}', '{formula}', '{bin_value}', '{card_type}', 1, '{comments}'
                                );
                                """
                                print("Writing query:", rule_setupparametersslab_query)
                                file.write(rule_setupparametersslab_query + '\n')
                                destination_cursor.execute(rule_setupparametersslab_query)
                else:
                    print(f"Currency '{currency_name}' not found in the database.")

        # Commit changes
        destination_conn.commit()

    finally:
        # Close connections
        source_cursor.close()
        source_conn.close()
        destination_cursor.close()
        destination_conn.close()

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Transfer data between databases.")
    parser.add_argument("currency_name", type=str, help="The name of the currency to use for the transfer.")

    # Parse the command-line arguments
    args = parser.parse_args()

    # Call the main function with the currency name
    transfer_data(args.currency_name)
