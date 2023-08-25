import mysql.connector as connector
import requests
import time
import os

if __name__=="__main__":
    """Another way to make sure counties are inputted properly.
    Doing both this and fix_counties assigned the vast majority of power plants
    to a county (there are some typos though).
    """
    # Using local variable for password
    db_password = os.environ.get('DB_PASSWORD')
    api_key = os.environ.get('EIA_API_KEY')
    try:
        conn = connector.connect(
            host='localhost',
            user=db_password,
            password=db_password,
            database='power_plants_db'
        )
        cursor = conn.cursor()
        
        query = """
            SELECT DISTINCT plants.id
            FROM plants
            JOIN counties ON plants.county_id = counties.id
            WHERE countyName = 'NA';"""
        cursor.execute(query)
        plant_ids = cursor.fetchall()
        plant_ids = [plant_id[0] for plant_id in plant_ids]
        i=0
        for plant_id in plant_ids:
            print(plant_id)
            req = (f"https://api.eia.gov/v2/electricity/operating-generator-capacity/data/?"
            f"frequency=monthly&data[0]=county&facets[plantid][]={plant_id}&sort[0][column]=period"
            f"&sort[0][direction]=desc&offset=0&length=5000&api_key={api_key}")

            response = requests.get(req).json()
            if "response" not in response:
                continue
            if "data" not in response["response"]:
                print(response["response"])
                continue
            response = response["response"]["data"]

            for item in response:
                county = item["county"]
                state = item["state"]
                if county is None:
                    pass
                else:
                    get_county_id_query = """
                        SELECT id
                        FROM counties
                        WHERE countyName = %s;
                    """
                    cursor.execute(get_county_id_query, (county,))
                    county_id = cursor.fetchone()
                    if county_id is None:
                        insert_county_query = """
                            INSERT INTO counties (countyName, state)
                            VALUES %s, %s;
                        """
                        cursor.execute(insert_county_query, (county, state))
                        cursor.execute(get_county_id_query, (county,))
                        county_id = cursor.fetchone()
                    county_id = county_id[0]
                    replace_county_query = """
                        UPDATE plants
                        SET county_id = %s
                        WHERE id = %s;
                    """
                    cursor.execute(replace_county_query, (county_id, plant_id))
                    break
            
            conn.commit()
            time.sleep(1)
            print(i)
            i+=1
        cursor.close()
        conn.close()

    except connector.Error as err:
        print(f"mysql error: {err}")
    
    except Exception as err:
        print(f"different exception {err}")