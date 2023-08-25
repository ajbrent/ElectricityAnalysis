import os
import mysql.connector as connector
import requests

if __name__=="__main__":
    """Initially, made the generation quantity too small.
    Fixed that, then replaced all values that maxed out the
    value at 8 characters."""
    # Using local variable for password
    db_password = os.environ.get('DB_PASSWORD')
    api_key = os.environ.get('EIA_API_KEY')

    try:
        conn = connector.connect(
            host='localhost',
            user='root',
            password=db_password,
            database='power_plants_db'
        )
        cursor = conn.cursor()
        
        query = """
        SELECT DISTINCT plant_id
        FROM production
        WHERE grossGeneration = 999999.99"""
        cursor.execute(query)
        plant_ids = cursor.fetchall()
        plant_ids = [plant_id[0] for plant_id in plant_ids]
        i=0
        for plant_id in plant_ids:
            print(plant_id)
            req = (f"https://api.eia.gov/v2/electricity/facility-fuel/data/?"
            f"frequency=monthly&data[0]=gross-generation&"
            f"facets[plantCode][]={plant_id}&sort[0][column]=period&sort[0][direction]=asc&offset=0&length=5000&api_key={api_key}")
            response = requests.get(req).json()["response"]["data"]
            for item in response:
                period = item["period"]
                month_year = "".join([period, "-01"])
                gross_gen = item["gross-generation"]
                fuel_abbrev = item["fuel2002"]
                if fuel_abbrev=="ALL":
                    continue
                cursor.execute("SELECT id FROM fuels WHERE symbol = %s;", (fuel_abbrev,))
                fuel_id = cursor.fetchone()[0]
                update_gg_query = """
                    UPDATE production
                    SET grossGeneration = %s
                    WHERE monthYear = %s AND plant_id = %s AND fuel_id = %s;
                    """
                update_gg_input = (gross_gen, month_year, plant_id, fuel_id)
                cursor.execute(update_gg_query, update_gg_input)
            
            conn.commit()
            print(i)
            i+=1
        cursor.close()
        conn.close()

    except Exception:
        print(f"Exception: {Exception}")