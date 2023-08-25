CREATE DATABASE power_plants_db;

USE power_plants_db;

CREATE TABLE states (
	symbol CHAR(2) PRIMARY KEY,
    stateName VARCHAR(30)
);

CREATE TABLE counties (
	id INT PRIMARY KEY AUTO_INCREMENT,
    state CHAR(2),
    countyName VARCHAR(50) DEFAULT 'NA',
    CONSTRAINT fk_county_state  
		FOREIGN KEY (state) REFERENCES states (symbol),
    CONSTRAINT unique_county_constraint UNIQUE (state, countyName)
);

CREATE TABLE fuel_categories (
	id INT PRIMARY KEY AUTO_INCREMENT,
    categoryName VARCHAR(20)
);

CREATE TABLE fuel_subcategories (
	id INT PRIMARY KEY AUTO_INCREMENT,
    subcategoryName VARCHAR(20),
    category_id INT,
    CONSTRAINT fk_fuel_categories FOREIGN KEY (category_id) REFERENCES fuel_categories (id)
);

CREATE TABLE fuels (
	id INT PRIMARY KEY AUTO_INCREMENT,
	symbol VARCHAR(4),
    fuelDescription VARCHAR(50),
    subcategory_id INT,
    CONSTRAINT fk_subcat_fuel
		FOREIGN KEY (subcategory_id)
			REFERENCES fuel_subcategories(id),
    CONSTRAINT unique_fuel_symbol UNIQUE (symbol)
);

CREATE TABLE balancing_auths (
	id INT PRIMARY KEY AUTO_INCREMENT,
	BACode CHAR(7),
    BAName VARCHAR(150),
    CONSTRAINT unique_ba_code UNIQUE (BACode)
);

CREATE TABLE sectors (
	id INT PRIMARY KEY AUTO_INCREMENT,
    sectorCode VARCHAR(30),
    sectorName VARCHAR(30)
);

CREATE TABLE entities (
	id INT PRIMARY KEY,
    entityName VARCHAR(80),
    sectorid INT,
    CONSTRAINT UNIQUE (entityName),
    CONSTRAINT fk_entity_sector
		FOREIGN KEY (sectorid) REFERENCES sectors (id)
);

CREATE TABLE plants (
	id INT PRIMARY KEY,
    plantName VARCHAR(100),
    county_id INT,
    Latitude DECIMAL(9, 6),
    Longitude DECIMAL(9, 6),
    CONSTRAINT fk_plant_county
		FOREIGN KEY (county_id) REFERENCES counties (id)
);
-- Might do this after inserting bulk of data
CREATE INDEX idx_plants_county_id
ON plants (county_id);

CREATE TABLE technologies (
	id INT PRIMARY KEY AUTO_INCREMENT,
    techName VARCHAR(50)
);

CREATE TABLE genstatus (
	id CHAR(2) PRIMARY KEY,
    statusDescription VARCHAR(100)
);

CREATE TABLE generators (
	id CHAR(6) PRIMARY KEY,
    plant_id INT,
    entity_id INT,
    tech_id INT,
    energy_source_id INT,
    balance_id INT,
    namePlateCapacity_MW DECIMAL (6, 2),
    summerCapacity_MW DECIMAL (6, 2),
    winterCapacity_MW DECIMAL (6, 2),
    opYearMonth DATE,
    CONSTRAINT fk_gen_plant
		FOREIGN KEY (plant_id) REFERENCES plants (id),
    CONSTRAINT fk_gen_entity
		FOREIGN KEY (entity_id) REFERENCES entities (id),
	CONSTRAINT fk_gen_tech 
		FOREIGN KEY (tech_id) REFERENCES technologies (id),
	CONSTRAINT fk_gen_fuel
		FOREIGN KEY (energy_source_id) REFERENCES fuels (id),
	CONSTRAINT fk_gen_ba
		FOREIGN KEY (balance_id) REFERENCES balancing_auths (id)
);
-- Again, probably better after first bulk of data is in db.
CREATE INDEX idx_npc
ON generators (nameplateCapacity_MW);
CREATE INDEX idx_sc
ON generators (summerCapacity_MW);
CREATE INDEX idx_wc
ON generators (winterCapacity_MW);
CREATE INDEX idx_entity
ON generators (entity_id);

CREATE TABLE gen_dates (
	gen_id CHAR(6),
    genstatus CHAR(2),
    startDate DATE,
    endDate DATE,
    CONSTRAINT fk_gen_date
		FOREIGN KEY (gen_id) REFERENCES generators (id),
	CONSTRAINT fk_gstart_date
		FOREIGN KEY (genstatus) REFERENCES genStatus (id),
	CONSTRAINT unique_row UNIQUE (gen_id, genstatus, startDate, endDate)
);
CREATE INDEX idx_gen_date
ON gen_dates (gen_id);
CREATE INDEX idx_start_date
ON gen_dates (startDate);
CREATE INDEX idx_end_date
ON gen_dates (endDate);

CREATE TABLE production (
	monthYear DATE,
	plant_id INT,
    fuel_id INT,
    netGeneration DECIMAL(14, 2),
    grossGeneration DECIMAL(14, 2),
    netGenUnits VARCHAR(30),
    grossGenUnits VARCHAR(30),
    CONSTRAINT fk_prod_plant
		FOREIGN KEY (plant_id) REFERENCES plants (id),
	CONSTRAINT fk_prod_fuel
		FOREIGN KEY (fuel_id) REFERENCES fuels (id),
	CONSTRAINT unique_prod_entry
		UNIQUE (monthYear, plant_id, fuel_id)
);
-- Again for optimization of queries: probably better after initial insert.
CREATE INDEX idx_netGen
ON production (netGeneration);
CREATE INDEX idx_grossGen
ON production (grossGeneration);
CREATE INDEX idx_prod_monthYear
ON production (monthYear);
CREATE INDEX idx_plants_county_id
ON plants (county_id);
CREATE INDEX idx_production_plant
ON production (plant_id);

