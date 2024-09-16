import xml.etree.ElementTree as ET

# Parse the XML file
tree = ET.parse('response.xml')
root = tree.getroot()

# List to store dictionaries for each item
items_list = []

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

print(items_list[0])
print(items_list[3732])
