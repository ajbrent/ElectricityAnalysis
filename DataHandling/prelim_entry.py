
import requests
import time
import mysql.connector

"""Certain fields do not change much over time and have a smaller amount of unique values.
Inserting those values in database before production and generators to speed up the process,
and avoid using some unnecessary 'INSERT IGNORE' statements."""

my_api_key = 'YwZSKLEZ46U9l1VyBoTRDNFbizaQsUG4slCcVM0U'

def get_facets(category, facet, api_key):
    """Getting filtered values for each facet."""
    req = f"https://api.eia.gov/v2/electricity/{category}/facet/{facet}?api_key={api_key}"

    response = requests.get(req).json()["response"]
    facet_num = response["totalFacets"]
    facets = response["facets"]
    items = []
    for i in range(facet_num):
        if not facets[i]['id'] is None:
            if not 'name' in facets[i]:
                if facet == "technology":
                    items.append((facets[i]['id'], ))
            else:
                items.append((facets[i]['id'], facets[i]['name']))
    return items


def insert_items(api_key, cursor, routines):
    """Running the insert procedures (defined in ./ElectricSQLScripts/power_plants_procedures.sql)
    to insert each value from get_facets.
    """
    for rout in routines:
        items = get_facets(rout["category"], rout["facet"], api_key)
        for item in items:
            print(item)
            cursor.callproc(rout["procedure"], item)
        time.sleep(1)

if __name__=='__main__':
    """Using the list of dictionaries to iterate each api path to get data for database insertions."""
    routines = [
        {"category": "facility-fuel", "facet": "state", "procedure": "insert_state"},
        {"category": "operating-generator-capacity", "facet": "technology", "procedure": "insert_tech"},
        {"category": "operating-generator-capacity", "facet": "energy_source_code", "procedure": "insert_fuel"},
        {"category": "operating-generator-capacity", "facet": "sector", "procedure": "insert_sector"},
        {"category": "operating-generator-capacity", "facet": "status", "procedure": "insert_status"},
        {"category": "operating-generator-capacity", "facet": "balancing_authority_code", "procedure": "insert_ba"}
    ]
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "Rocinante1615",
        "database": "power_plants_db"
    }
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    my_api_key = 'YwZSKLEZ46U9l1VyBoTRDNFbizaQsUG4slCcVM0U'
    insert_items(my_api_key, cursor, routines)
    conn.commit()
    cursor.close()
    conn.close()