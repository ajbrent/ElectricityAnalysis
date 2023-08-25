import pandas as pd
import mysql.connector as connector
import os

if __name__ == '__main__':
    """Simply adding longitude and latitude to each plant where available."""
    # Using local variable for password
    db_password = os.environ.get('DB_PASSWORD')
    
    plant_file_path = "./Power_Plants.csv"
    used_cols = ['Plant_Code', 'Longitude', 'Latitude']
    plant_df = pd.read_csv(plant_file_path, usecols=used_cols)

    conn = connector.connect(
            host='localhost',
            user='root',
            password=db_password,
            database='power_plants_db'
        )
    cursor = conn.cursor()

    update_loc_query = """
        UPDATE plants
        SET Latitude = %s, Longitude = %s
        WHERE id = %s;
    """
    count=0
    for _, row in plant_df.iterrows():
        query_input = (row['Latitude'], row['Longitude'], int(row['Plant_Code']))
        print(query_input)
        print(count)
        count+=1
        cursor.execute(update_loc_query, query_input)

    conn.commit()
    cursor.close()
    conn.close()