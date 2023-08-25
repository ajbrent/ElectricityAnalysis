import pandas as pd
import mysql.connector as connector
import os

"""
This is the best way to deal with inputting individual power plants.

In this case I used it to fill in blanks for most of the remaining power plants.

Found here: https://atlas.eia.gov/datasets/eia::power-plants/explore
"""

if __name__ == "__main__":
    # Using local variable for password
    db_password = os.environ.get('DB_PASSWORD')
    
    plant_file_path = "./Power_Plants.csv"
    plant_df = pd.read_csv(plant_file_path)

    conn = connector.connect(
            host='localhost',
            user='root',
            password=db_password,
            database='power_plants_db'
        )
    cursor = conn.cursor()
    plant_id_query = """
        SELECT plants.id
        FROM plants
        JOIN counties ON plants.county_id = counties.id
        WHERE countyName = 'NA';
    """
    cursor.execute(plant_id_query)
    plant_ids = cursor.fetchall()
    plant_ids = [plant_id[0] for plant_id in plant_ids]
    print(len(plant_ids))

    get_state_symbol_query = """
        SELECT symbol
        FROM states
        WHERE stateName = %s;
    """

    get_county_id_query = """
        SELECT id
        FROM counties
        WHERE countyName = %s AND state = %s;
    """

    insert_county_query = """
        INSERT INTO counties (countyName, state)
        VALUES (%s, %s);
    """

    update_county_query = """
        UPDATE plants
        SET county_id = %s
        WHERE id = %s;
    """
    missed_ids = 0
    for id in plant_ids:
        print(id)
        if plant_df[plant_df['Plant_Code'] == id].empty:
            missed_ids += 1
            continue
        
        state_county = plant_df[plant_df['Plant_Code'] == id][['County', 'State']]

        if state_county.isna().any().any():
            missed_ids+=1
            continue

        plant_county = plant_df[plant_df['Plant_Code'] == id]["County"].values
        plant_state = plant_df[plant_df['Plant_Code'] == id]["State"].values
        
        if plant_county.size == 0 or plant_state.size == 0:
            missed_ids += 1
            continue

        plant_county = plant_county[0]
        plant_state = plant_state[0]

        cursor.execute(get_state_symbol_query, (plant_state,))
        state_symbol = cursor.fetchone()[0]

        cursor.execute(get_county_id_query, (plant_county, state_symbol))
        county_id = cursor.fetchone()
        if county_id is None:
            cursor.execute(insert_county_query, (plant_county, state_symbol))
            cursor.execute(get_county_id_query, (plant_county, state_symbol))
            county_id = cursor.fetchone()
        county_id = county_id[0]
        
        cursor.execute(update_county_query, (county_id, id))
    
    print(f"Plants left without counties: {missed_ids}")
    
    conn.commit()
    cursor.close()
    conn.close()

        
        
