import requests
import mysql.connector as connector
import os

"""Inserting monthly production for each plant along with accomanying information."""

def get_plants(api_key):
    """Gathering each power plant code to split up api_calls."""
    req = f"https://api.eia.gov/v2/electricity/facility-fuel/facet/plantCode?api_key={api_key}"
    pps = requests.get(req).json()["response"]["facets"]
    return pps

def insert_other_fuels(api_key, cursor):
    """Fuel types included heare are not included in generator inventory, so this adds them."""
    req = f"https://api.eia.gov/v2/electricity/facility-fuel/facet/fuel2002?api_key={api_key}"

    response = requests.get(req).json()["response"]["facets"]
    for item in response:
        cursor.execute("INSERT IGNORE INTO fuels (symbol, fuelDescription) VALUES (%s, %s);", (item["id"], item["name"]))
    print("Other fuels added.")


def get_prod(api_key, pp_code, pp_name, cursor):
    """Filtering by each power plant one at a time and gathering production numbers."""
    print(pp_code)
    req = (f"https://api.eia.gov/v2/electricity/facility-fuel/data/?"
           f"frequency=monthly&data[0]=generation&data[1]=gross-generation&"
           f"facets[plantCode][]={pp_code}&sort[0][column]=period&sort[0][direction]=asc&offset=0&length=5000&api_key={api_key}")
    try:
        response = requests.get(req).json()
        if "error" in response:
            print(response["error"])
            return 1
        if response["response"]["total"] == 0:
            print(f"No results for {pp_code}")
            return 1
        
        data = response["response"]["data"]
        
        
        state = data[0]["state"]
        cursor.execute("SELECT county_id FROM plants WHERE id=%s;", (pp_code,))
        county_id = cursor.fetchone()
        if county_id is None:
            cursor.execute("""SELECT id FROM counties WHERE state = %s AND countyName = "NA";""", (state,))
            county_id = cursor.fetchone()
        county_id = county_id[0]

        for item in data:
            month_year = "".join([item["period"], "-01"])

            fuel_abbrev = item["fuel2002"]
            if fuel_abbrev=="ALL":
                continue
        
            cursor.execute("SELECT id FROM fuels WHERE symbol = %s;", (fuel_abbrev,))
            fuel_id = cursor.fetchone()[0]

            net_gen = item["generation"]
            gross_gen = item["gross-generation"]
            net_gen_units = item["generation-units"]
            gross_gen_units = item["gross-generation-units"]

            proc_input = (county_id, month_year, pp_code, pp_name, fuel_id, net_gen, gross_gen, net_gen_units, gross_gen_units)
            print(proc_input)
            cursor.callproc("insert_prod", proc_input)
        return 0
    except Exception:
        print(f"Exception: {Exception}")
        return 1
    
    
if __name__ == "__main__":
    """Gathering power plants, storing fuels, checking for already plants alread in db (optional),
    and inserting productio numbers.
    """
    # Using local variable for password
    db_password = os.environ.get('DB_PASSWORD')
    api_key = os.environ.get('EIA_API_KEY')
    power_plants = get_plants(api_key)

    try:
        conn = connector.connect(
            host='localhost',
            user='root',
            password=db_password,
            database='power_plants_db'
        )
        cursor = conn.cursor()

        insert_other_fuels(api_key, cursor)
        conn.commit()

        # Makes process faster by only going to power plants that have not yet been added to db.
        cursor.execute("SELECT DISTINCT plant_id FROM production;")
        finished_ids = cursor.fetchall()
        finished_ids = [item[0] for item in finished_ids]
        print(len(power_plants))
        valid_plants = [item for item in power_plants if item["id"] not in finished_ids]
        print(len(valid_plants))
        ###
        i=0
        for plant in valid_plants:
            pp_code = plant["id"]
            pp_name = plant["name"]
            status_ = get_prod(api_key, pp_code, pp_name, cursor)
            conn.commit()
            print(i)
            print(status_)
            i+=1
        
        cursor.close()
        conn.close()
        print("Done")
    
    except connector.Error as err:
        print(f"ERROR: {err}")


    
