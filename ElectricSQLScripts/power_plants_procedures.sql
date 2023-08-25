USE power_plants_db;

DELIMITER //
CREATE PROCEDURE insert_state(IN state_symbol CHAR(2), IN state_name VARCHAR(30))
BEGIN
	INSERT INTO states (symbol, stateName)
    SELECT state_symbol, state_name
    FROM DUAL
    WHERE NOT EXISTS (
		SELECT 1 FROM states
        WHERE symbol = state_symbol OR stateName = state_name
	);
END //

CREATE PROCEDURE insert_tech(IN tech_name VARCHAR(50))
BEGIN
	INSERT INTO technologies (techName)
    VALUES (tech_name);
END //

CREATE PROCEDURE insert_sector(IN sector_code VARCHAR(30), IN sector_name VARCHAR(30))
BEGIN
	INSERT INTO sectors (sectorCode, sectorName)
    SELECT sector_code, sector_name
    FROM DUAL
    WHERE NOT EXISTS (
		SELECT 1 FROM sectors
        WHERE sectorCode = sector_code
	);
END //

CREATE PROCEDURE insert_status(IN status_id CHAR(2), IN status_description VARCHAR(100))
BEGIN
	INSERT INTO genstatus (id, statusDescription)
    SELECT status_id, status_description
    FROM DUAL
    WHERE NOT EXISTS (
		SELECT 1 FROM genstatus
        WHERE id = status_id
	);
END //

CREATE PROCEDURE insert_fuel(IN fuel_symbol VARCHAR(4), IN fuel_description VARCHAR(50))
BEGIN
	INSERT INTO fuels (symbol, fuelDescription)
    SELECT fuel_symbol, fuel_description
    FROM DUAL
    WHERE NOT EXISTS (
		SELECT 1 FROM fuels
        WHERE symbol = fuel_symbol
	);
END //

CREATE PROCEDURE insert_ba(IN ba_code CHAR(7), IN ba_name VARCHAR(150))
BEGIN
	INSERT INTO balancing_auths (BACode, BAName)
    SELECT ba_code, ba_name
    FROM DUAL
    WHERE NOT EXISTS (
		SELECT 1 FROM balancing_auths
        WHERE BACode = ba_code
	);
END //

CREATE PROCEDURE insert_gen(
	IN gen_id CHAR(6),
    IN fuel_id INT,
    IN techID INT, 
    IN entityID INT,
    IN entity_name VARCHAR(80),
    IN sectorID INT,
    IN ba_id INT,
	IN state_symbol CHAR(2),
    IN county VARCHAR(50),
    IN plantID INT,
    IN plant_name VARCHAR(100),
    IN op_year_month DATE,
    IN name_capacity DECIMAL(6, 2),
    IN summer_capacity DECIMAL(6, 2),
    IN winter_capacity DECIMAL(6, 2)
)
BEGIN
	DECLARE plant_exists INT;
    DECLARE countyID INT;
    DECLARE currentCounty VARCHAR(50);
    
	SET county = COALESCE(county, 'NA');
    SET ba_id = COALESCE(ba_id, 95);
    
	INSERT IGNORE INTO counties (state, countyName)
    VALUES (state_symbol, county);
    
    SELECT COUNT(*) INTO plant_exists
    FROM plants
    WHERE id = plantID;
    
    IF plant_exists = 0 THEN
		INSERT INTO plants (id, plantName, county_id)
		SELECT plantID, plant_name, id
		FROM counties
		WHERE countyName = county AND state=state_symbol;
	ELSE
		SELECT countyName INTO currentCounty
        FROM counties
        JOIN plants ON counties.id = plants.county_id
        WHERE plants.id = plantID;
        
		IF `county` <> 'NA' AND `currentCounty` = 'NA' THEN
			SELECT id INTO countyID
            FROM counties
            WHERE countyName=county AND state=state_symbol;
            
            UPDATE plants
            SET county_id = countyID
            WHERE id=plantID;
		END IF;
    END IF;
    
    INSERT IGNORE INTO entities (id, entityName, sectorid)
    VALUES (entityID, entity_name, sectorID);
    
    INSERT IGNORE INTO generators (gen_id, plant_id, entity_id, tech_id, energy_source_id, balance_id, namePlateCapacity_MW, summerCapacity_MW, winterCapacity_MW, opYearMonth)
    VALUES (gen_id, plantID, entityID, techID, fuel_id, ba_id, name_capacity, summer_capacity, winter_capacity, op_year_month);
END//

CREATE PROCEDURE insert_prod(
	IN county_id INT,
	IN prod_date DATE, 
    IN plantID INT, 
    IN plant_name VARCHAR(100),
    IN fuelID INT,
    IN net_gen DECIMAL(14, 2),
    IN gross_gen DECIMAL(14, 2), 
    IN net_gen_units VARCHAR(30),
    IN gross_gen_units VARCHAR(30)
)
BEGIN
    INSERT IGNORE INTO plants (id, plantName, county_id)
    VALUES (plantID, plant_name, county_id);
    
    INSERT IGNORE INTO production (monthYear, plant_id, fuel_id, netGeneration, grossGeneration, netGenUnits, grossGenUnits)
    VALUES (prod_date, plantID, fuelID, net_gen, gross_gen, net_gen_units, gross_gen_units);
END//
DELIMITER ;

