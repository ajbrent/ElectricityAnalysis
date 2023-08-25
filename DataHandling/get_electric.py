
import requests
import time
import mysql.connector
import json
from datetime import datetime
import os

"""Individual generator data from the EIA API.

Split up by plant and generator id and put into a MySQL database.
"""

def get_plant_ids(api_key):
    """Collecting the plant_ids to breakup my API requests."""
    req = f"https://api.eia.gov/v2/electricity/operating-generator-capacity/facet/plantid?api_key={api_key}"
    try:
        response =  requests.get(req).json()["response"]
        plant_list = response["facets"]
        plant_list = [item["id"] for item in plant_list]
        return plant_list

    except Exception:
        """Retry when failure occurs.
        Could add limit to how many times this happens, but simply watching worked fine for me.
        """
        print("Error, retrying... ")
        time.sleep(0.5)
        return get_plant_ids(api_key)

def get_generator(api_key, cursor, plant_id):
    """Getting each generator, associated info, and time periods of activity."""
    print(plant_id)
    req = (f"https://api.eia.gov/v2/electricity/operating-generator-capacity/data/"
    f"?frequency=monthly&data[0]=county&data[1]=nameplate-capacity-mw&"
    f"data[2]=net-summer-capacity-mw&data[3]=net-winter-capacity-mw&data[4]=operating-year-month&"
    f"facets[plantid][]={plant_id}&sort[0][column]=period&sort[0][direction]=asc&offset=0&api_key={api_key}")

    try:
        response = requests.get(req)
        if response.status_code != 200:
            print(f"Request for {plant_id} failed with status code {response.status_code}")
            return 1
        
        try:
            response = json.loads(response.text)

            if "error" in response:
                print(response["error"])
                return 1
            if response["response"]["total"] == 0:
                print(f"No results for {plant_id}")
                return 1
            response = response["response"]["data"] 
            state = response[0]["stateid"]
            
            plant_name = response[0]["plantName"]
            county = response[0]["county"]
            
            gen_status = response[0]["status"]
            period = "".join([response[0]["period"], "-01"])
            current_start = period
            
            gen_status_dict = {}
            current_start_dict = {}
            for item in response:
                
                gen_id = str(item["generatorid"])
                sector = item["sector"]
                entityid = item["entityid"]
                entity_name = item["entityName"]
                
                tech_name = item["technology"]
                ba_code = str(item["balancing_authority_code"])
                fuel_code = item["energy_source_code"]
                
                op_ym = "".join([item["operating-year-month"], "-01"])
                name_cap = item["nameplate-capacity-mw"]
                sum_cap = item["net-summer-capacity-mw"]
                winter_cap = item["net-winter-capacity-mw"]
                period = "".join([item["period"], "-01"])
                period = datetime.strptime(period, "%Y-%m-%d")
                gen_status = item["status"]
                if gen_id not in gen_status_dict:
                    gen_status_dict[gen_id] = []

                    cursor.execute("SELECT id FROM technologies WHERE techName = %s;", (tech_name,))
                    tech_id = cursor.fetchone()[0]
                    cursor.execute("SELECT id FROM balancing_auths WHERE BACode = %s;", (ba_code,))
                    ba_id = cursor.fetchone()
                    if ba_id is None:
                        if state=="HI":
                            cursor.execute("""SELECT id FROM balancing_auths WHERE BACode = "HECO";""")
                            ba_id = cursor.fetchone()[0]
                    else:
                        ba_id = ba_id[0]
                    cursor.execute("SELECT id FROM fuels WHERE symbol = %s", (fuel_code,))
                    fuel_id = cursor.fetchone()[0]
                    cursor.execute("SELECT id FROM sectors WHERE sectorCode = %s", (sector,))
                    sector_id = cursor.fetchone()[0]
                    proc_input = (gen_id,
                                    fuel_id,
                                    tech_id,
                                    entityid,
                                    entity_name,
                                    sector_id, 
                                    ba_id,
                                    state, 
                                    county,
                                    plant_id, 
                                    plant_name, 
                                    op_ym, 
                                    name_cap, 
                                    sum_cap, 
                                    winter_cap)
                    print(proc_input)
                    cursor.callproc("insert_gen", proc_input)
                period_item = (gen_status, period)
                gen_status_dict[gen_id].append(period_item)
                j = len(gen_status_dict[gen_id])-1

                # If api sorts correctly, this shouldn't be entered, but just in case
                while j>=0 and period_item[1]<gen_status_dict[gen_id][j][1]:
                    gen_status_dict[gen_id][j+1] = gen_status_dict[gen_id][j]
                    gen_status_dict[gen_id][j] = period_item

            for key in gen_status_dict.keys():
                status_list = []
                start_period = gen_status_dict[key][0][1].strftime("%Y-%m-%d")
                current_period = start_period
                current_status = gen_status_dict[key][0][0]
                for item in gen_status_dict[key][1:]:
                    item = (item[0], item[1].strftime("%Y-%m-%d"))
                    if item[0] != current_status:
                        status_list.append((key, current_status, start_period, current_period, plant_id))
                        current_status = item[0]
                        start_period = item[1]
                    current_period = item[1]
                status_list.append((key, current_status, start_period, current_period, plant_id))
                print(status_list)
                for item in status_list:
                    cursor.execute("""INSERT IGNORE INTO gen_dates (gen_id, genstatus, startDate, endDate, plant_id)
                                    VALUES (%s, %s, %s, %s, %s);""", 
                                    item)
                
            return 0
        except json.JSONDecodeError:
            print("Invalid Json response.") 
            return 1   
    except Exception as err:
        print(f"Exception: {err}")
        time.sleep(0.5)
        return get_generator(api_key, cursor, plant_id)


if __name__ == "__main__":
    """Putting it all together and iterating across power plants.
    
    Printing useful info along the way to track progress, and check where EIA may have missing info.
    """
    # Using local variable for password
    db_password = os.environ.get('DB_PASSWORD')
    api_key = os.environ.get('EIA_API_KEY')
    plant_ids = get_plant_ids(api_key)
    print(plant_ids)
    
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password=db_password,
            database='power_plants_db'
        )
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT plant_id FROM gen_dates;")
        used_plants = cursor.fetchall()
        used_plants = [plant[0] for plant in used_plants]
        plant_ids = [item for item in plant_ids if item not in used_plants]

        i=0
        j=0
        print(len(plant_ids))
        for plant_id in plant_ids:
            j += get_generator(api_key, cursor, plant_id)
            if i%10 == 0:
                conn.commit() 
                print(f"Last saved plant_id: {plant_id} at index: {i+j}")
            print(i)
            time.sleep(0.5)
            i+=1

        print("DONE")
        conn.commit()
        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        conn.commit()

        

    
    