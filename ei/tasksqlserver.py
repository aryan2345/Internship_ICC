import xml.etree.ElementTree as ET
import pyodbc
from datetime import datetime, timedelta

# Parse the XML file
tree = ET.parse('response.xml')
root = tree.getroot()

# Establish database connection using pyodbc
connection = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=msqlburgan.c85oqibbmnk0.us-west-2.rds.amazonaws.com,1433;'
    'DATABASE=ei;'
    'UID=admin;'
    'PWD=PAssw0rd#$$'
)

# Create a cursor object using the connection
cursor = connection.cursor()

# SQL INSERT statement
sql_insert = """
INSERT INTO deal (deal_name, deal_image, deal_description, image_path, how_to_book, dates_to_remember, 
program_conditions, partner_conditions, points, status, price, merchant_id, deal_validity, 
category_id, is_active, merchant_productid, author_brand, availability, discount, subcategory_id, 
updatetime, rating, redeem_mode, brand_code, itemcode)
VALUES ('1',?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# SQL UPDATE statement
sql_update = """
UPDATE deal
SET deal_name = ?, deal_image = ?, deal_description = ?, image_path = ?, how_to_book = ?, 
dates_to_remember = ?, program_conditions = ?, partner_conditions = ?, points = ?, status = ?, 
price = ?, merchant_id = ?, deal_validity = ?, category_id = ?, is_active = ?, 
merchant_productid = ?, author_brand = ?, availability = ?, discount = ?, subcategory_id = ?, 
updatetime = ?, rating = ?, redeem_mode = ?, brand_code = ?
WHERE itemcode = ?
"""

# List to store dictionaries for each item
items_list = []

# Function to retrieve merchant_id from admin_info table
def get_merchant_id():
    cursor.execute("SELECT TOP 1 id FROM admin_info")
    result = cursor.fetchone()
    return result[0] if result else None

# Function to retrieve category_id from category table
def get_category_id():
    cursor.execute("SELECT TOP 1 id FROM category")
    result = cursor.fetchone()
    return result[0] if result else None

# Function to retrieve subcategory_id from subcategory table
def get_subcategory_id():
    cursor.execute("SELECT TOP 1 id FROM subcategory")
    result = cursor.fetchone()
    return result[0] if result else None

merchant_id = get_merchant_id()
category_id = get_category_id()
subcategory_id = get_subcategory_id()

# Calculate deal validity date (1 year from current date)
deal_validity_date = datetime.now() + timedelta(days=365)

# Function to calculate shipping charge based on weight
def calculate_shipping_charge(weight):
    weight = float(weight)
    if weight <= 5.0:
        return 15
    else:
        return 15 + (5 * (weight - 5))

# Iterate through each 'item' element
for item in root.findall('item'):
    item_dict = {}

    # Extract data into dictionary
    item_dict['serial'] = item.find('serial').text
    item_dict['sku'] = item.find('sku').text
    item_dict['itemCode'] = item.find('itemCode').text
    item_dict['title'] = item.find('title').text
    item_dict['link'] = item.find('link').text

    # Extract images into a list
    images_list = []
    for image in item.find('images').findall('item'):
        images_list.append(image.text)
    item_dict['images'] = images_list

    item_dict['brand'] = item.find('brand').text

    # Extract categories into a list
    categories_list = []
    for category in item.find('categories').findall('item'):
        categories_list.append(category.text)
    item_dict['categories'] = categories_list

    # Extract variants into a list
    variants_list = []
    for color in item.find('variants').findall('color'):
        for color_item in color.findall('item'):
            variants_list.append(color_item.text)
    item_dict['variants'] = variants_list

    item_dict['weight'] = item.find('weight').text

    # Extract availability from stock
    item_dict['availability'] = item.find('stock').find('availability').text

    # Extract prices into a dictionary
    prices_dict = {}
    prices_dict['old'] = item.find('prices').find('old').text
    prices_dict['new'] = item.find('prices').find('new').text
    item_dict['prices'] = prices_dict

    item_dict['description'] = item.find('description').text

    # Extract key features into a list
    key_features_list = []
    for feature in item.find('keyFeatures').findall('item'):
        key_features_list.append(feature.text)
    item_dict['keyFeatures'] = key_features_list

    # Extract warranties into a dictionary
    warranties_dict = {}
    included_warranty = item.find('warranties').find('included')
    if included_warranty is not None:
        included_warranty_list = []
        for included in included_warranty.findall('item'):
            included_warranty_list.append(included.text)
        warranties_dict['included'] = included_warranty_list

    # Extract extended warranties into a list if they exist
    extended_warranties_list = []
    extended_warranties = item.find('warranties').find('extended')
    if extended_warranties is not None:
        for warranty in extended_warranties.findall('item'):
            extended_warranties_list.append(warranty.text)
    warranties_dict['extended'] = extended_warranties_list

    item_dict['warranties'] = warranties_dict

    # Extract delivery time details into a dictionary
    delivery_time_dict = {}
    standard_delivery = item.find('deliveryTime').find('standard')
    if standard_delivery is not None:
        delivery_time_dict['standardDelivery'] = standard_delivery.text.strip()
    item_dict['deliveryTime'] = delivery_time_dict

    # Append the item dictionary to the list
    items_list.append(item_dict)

# Ensure subcategory_id has a valid value
subcategory_id = subcategory_id if subcategory_id is not None else category_id

# Insert or update each item in the database
for item in items_list:
    try:
        # Calculate points and adjust price for items priced at AED 499 and below
        price = float(item['prices']['new'])
        weight = float(item['weight'])  # Ensure weight is converted to float
        if price <= 499:
           weight *= 100
           weight *= 1.10
           shipping_charge = calculate_shipping_charge(weight)
        else:
            shipping_charge = None

        # Check if itemcode already exists in the database
        cursor.execute("SELECT COUNT(*) FROM deal WHERE itemcode = ?", (item['itemCode'],))
        exists = cursor.fetchone()[0] > 0

        # Concatenate and encode the deal_description as bytes
        # Concatenate the deal_description as a string
        deal_description = item['description'] + ' ' + ' '.join(item['keyFeatures'])

        if exists:
            cursor.execute(sql_update, (
                item['title'],  # deal_name
                item['images'][0] if item['images'] else None,  # deal_image (only the first image or None if no images)
                deal_description,  # deal_description
                None,  # image_path
                "Please keep the product redemption copy (sent to your e-mail) with you at the time of delivery. It may be required at the time of delivery.",  # how_to_book
                "The product will be delivered to you within seven days of redemption.<br>Pre-Order delivery terms will differ. Kindly refer the delivery terms mentioned above for details.",  # dates_to_remember
                "The price includes delivery charges in UAE.",  # program_conditions
                "The product will be addressed at your address through courier Service- For any queries regarding your purchase, <br>please contact: +97145539349 or you can email us at contact@iccfze.com.<br> The helpline can be accessed between 9.00 am to 5.00 pm. from Sunday to Thursday excluding national holidays.<br> Once the redemption is made they cannot be cancelled.",  # partner_conditions
                shipping_charge if shipping_charge is not None else 0,  # points (use 0 if shipping_charge is None)
                0,  # status
                price,  # price
                merchant_id,  # merchant_id (admin_info table primary key)
                deal_validity_date,  # deal_validity (1 year from current date)
                category_id,  # category_id (category table primary key)
                1,  # is_active
                item['sku'],  # merchant_productid
                None,  # author_brand
                "yes",  # availability
                10,  # discount
                subcategory_id,  # subcategory_id (subcategory table primary key)
                datetime.now(),  # updatetime
                0,  # rating
                "online",  # redeem_mode
                "brand_code",  # brand_code (replace with actual brand code if available)
                item['itemCode']  # itemcode (for WHERE clause)
            ))
        else:
            cursor.execute(sql_insert, (
                item['title'],  # deal_name
                item['images'][0] if item['images'] else None,  # deal_image (only the first image or None if no images)
                deal_description,  # deal_description
                None,  # image_path
                "Please keep the product redemption copy (sent to your e-mail) with you at the time of delivery. It may be required at the time of delivery.",  # how_to_book
                "The product will be delivered to you within seven days of redemption.<br>Pre-Order delivery terms will differ. Kindly refer the delivery terms mentioned above for details.",  # dates_to_remember
                "The price includes delivery charges in UAE.",  # program_conditions
                "The product will be addressed at your address through courier Service- For any queries regarding your purchase, <br>please contact: +97145539349 or you can email us at contact@iccfze.com.<br> The helpline can be accessed between 9.00 am to 5.00 pm. from Sunday to Thursday excluding national holidays.<br> Once the redemption is made they cannot be cancelled.",  # partner_conditions
                shipping_charge if shipping_charge is not None else 0,  # points (use 0 if shipping_charge is None)
                0,  # status
                price,  # price
                merchant_id,  # merchant_id (admin_info table primary key)
                deal_validity_date,  # deal_validity (1 year from current date)
                category_id,  # category_id (category table primary key)
                1,  # is_active
                item['sku'],  # merchant_productid
                None,  # author_brand
                "yes",  # availability
                10,  # discount
                subcategory_id,  # subcategory_id (subcategory table primary key)
                datetime.now(),  # updatetime
                0,  # rating
                "online",  # redeem_mode
                "brand_code",  # brand_code (replace with actual brand code if available)
                item['itemCode']  # itemcode (for WHERE clause)
            ))
        # Commit transaction
        connection.commit()
    except Exception as e:
        print(f"Error processing item {item['itemCode']}: {e}")
        # Rollback transaction on error
        connection.rollback()

# Close the cursor and connection
cursor.close()
connection.close()
