import pandas as pd
import mysql.connector as connector
import os

if __name__ == '__main__':
    """Categorizing fuels because it is not always useful to have the specific fuel types specified:
    (i.e. there are several forms of coal specified, but we just have one coal subcategory under the
    fossil fuel category)

    The csv was created by manually categorizing each fuel type. 
    """

    csv_file_path = 'ElectricityInfo/fuel_types.csv'
    fuel_types = pd.read_csv(csv_file_path, usecols=[0, 3, 4])

    # Using local variable for password
    db_password = os.environ.get('DB_PASSWORD')
    conn = connector.connect(
            host='localhost',
            user='root',
            password='Rocinante1615',
            database='power_plants_db'
        )
    cursor = conn.cursor()

    cat_query = """
        INSERT INTO fuel_categories (categoryName)
        VALUES (%s);
    """
    subcat_query = """
        INSERT INTO fuel_subcategories (subcategoryName, category_id)
        VALUES (%s, %s);
    """
    fuel_subcat_query = """
        UPDATE fuels
        SET subcategory_id = %s
        WHERE id = %s;
    """
    used_categories = []
    used_subcategories = []
    name_key_subcat_pairs = {}
    name_key_cat_pairs = {}
    for _, item in fuel_types.iterrows():
        category = item["Category"]
        subcategory = item["Subcategory"]
        fuel_id = item["id"]
        if category not in used_categories:
            cursor.execute(cat_query, (category,))
            used_categories.append(category)
            name_key_cat_pairs[category] = len(used_categories)
        if subcategory not in used_subcategories:
            cursor.execute(subcat_query, (subcategory, name_key_cat_pairs[category]))
            used_subcategories.append(subcategory)
            name_key_subcat_pairs[subcategory] = len(used_subcategories)
        cursor.execute(fuel_subcat_query, (name_key_subcat_pairs[subcategory], fuel_id))
    
    conn.commit()
    cursor.close()
    conn.close()
    
