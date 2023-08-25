USE power_plants_db;

-- Everything is in megawatthours since this returns zero
SELECT COUNT(*) FROM production
WHERE netGenUnits <> 'megawatthours' OR grossGenUnits <> 'megawatthours';

-- Total output by state 2021
SELECT states.symbol, SUM(netGeneration) AS stateGeneration
FROM states
JOIN counties ON states.symbol = counties.state
JOIN plants ON counties.id = plants.county_id
JOIN (
	SELECT plant_id, netGeneration
    FROM production
	WHERE YEAR(production.monthYear)=2021
) AS filtered_production
ON plants.id = filtered_production.plant_id
GROUP BY states.symbol
ORDER BY stateGeneration DESC;

-- Total output by state with primary fuel source
WITH fuel_state_sums AS (
	SELECT 
		counties.state AS state,
		fuel_subcategories.subcategoryName AS fuel,
        SUM(netGeneration) AS totalGeneration
    FROM plants
    JOIN (
		SELECT plant_id, fuel_id, netGeneration
        FROM production
        WHERE YEAR(monthYear)=2021
	) AS year_prod ON plants.id = year_prod.plant_id
    JOIN counties ON plants.county_id = counties.id
    JOIN fuels ON year_prod.fuel_id = fuels.id
    JOIN fuel_subcategories ON fuels.subcategory_id = fuel_subcategories.id
    GROUP BY counties.state, fuels.symbol
)
SELECT 
	maxed_rows.state,
	fuel AS `Primary Fuel Source`,
    ROUND((totalGeneration/stateGen)*100, 1) AS `Percentage of State Production %`,
    stateGen AS `Total State Generation` 
FROM (
	SELECT state, MAX(totalGeneration) AS maxGen
    FROM fuel_state_sums
    GROUP BY state
) AS maxed_rows 
JOIN (
	SELECT
		state,
        fuel,
        totalGeneration,
        SUM(totalGeneration) OVER (PARTITION BY state) AS stateGen
	FROM fuel_state_sums
)summed_gen ON maxed_rows.maxGen = summed_gen.totalGeneration AND maxed_rows.state = summed_gen.state
ORDER BY stateGen DESC;

SELECT plant_id, plantName, countyName, state
FROM plants
JOIN production ON plants.id=production.plant_id
JOIN (
	SELECT id
    FROM fuels
    WHERE symbol='OOG'
) AS sc_fuels ON production.fuel_id = sc_fuels.id
JOIN counties ON plants.county_id = counties.id;

-- Plant production numbers each month
WITH prod_sum AS (
	SELECT 
		monthYear,
        plant_id,
        fuel_id,
        netGeneration
	FROM production
)
SELECT 
	monthYear,
    state,
    countyName,
    plantName,
	Longitude,
    Latitude,
    subcategoryName,
    netGeneration
FROM prod_sum
JOIN fuels ON prod_sum.fuel_id = fuels.id
JOIN (
	SELECT *
    FROM fuel_subcategories
    WHERE subcategoryName <> 'Storage'
) AS valid_subcategories ON fuels.subcategory_id = valid_subcategories.id
JOIN plants ON prod_sum.plant_id = plants.id
JOIN counties ON plants.county_id = counties.id
JOIN states ON counties.state = states.symbol
ORDER BY monthYear;

-- Total production for each energy subcategory in 2021 (not readable but easier for tableau)
SELECT state, subcategoryName, SUM(netGeneration) AS netGenSum
FROM (
	SELECT plant_id, fuel_id, netGeneration
	FROM production 
) AS year_prod
JOIN fuels ON year_prod.fuel_id = fuels.id
JOIN (
	SELECT *
	FROM fuel_subcategories
	WHERE subcategoryName <> 'Storage'
) AS no_storage_subcat ON fuels.subcategory_id = no_storage_subcat.id
JOIN plants ON year_prod.plant_id = plants.id
JOIN counties ON plants.county_id = counties.id
JOIN states ON counties.state = states.symbol
GROUP BY state, subcategory_id;

-- Table of energy by state and category
CREATE TEMPORARY TABLE fuel_state_sums AS (
	SELECT state, subcategoryName, SUM(netGeneration) AS netGenSum
    FROM (
		SELECT plant_id, fuel_id, netGeneration
        FROM production
        WHERE YEAR(monthYear)=2021 
	) AS year_prod
	RIGHT JOIN fuels ON year_prod.fuel_id = fuels.id
	JOIN (
		SELECT *
        FROM fuel_subcategories
        WHERE subcategoryName <> 'Storage'
    ) AS no_storage_subcat ON fuels.subcategory_id = no_storage_subcat.id
	JOIN plants ON year_prod.plant_id = plants.id
	JOIN counties ON plants.county_id = counties.id
	JOIN states ON counties.state = states.symbol
	GROUP BY state, subcategory_id
);
SET SESSION group_concat_max_len = 1000000;

SELECT GROUP_CONCAT(
	DISTINCT CONCAT('COALESCE(MAX(CASE WHEN subcategoryName = ''', subcategoryName, ''' THEN netGenSum END), 0) AS `', TRIM(subcategoryName), '`')
    SEPARATOR ', ') INTO @sql
FROM fuel_subcategories
WHERE subcategoryName <> 'Storage';

SET @sql = CONCAT( '
	SELECT state, ', @sql, '
    FROM fuel_state_sums
    GROUP BY state
');

PREPARE fuel_state_pivot FROM @sql;
EXECUTE fuel_state_pivot;
DEALLOCATE PREPARE fuel_state_pivot;
DROP TEMPORARY TABLE fuel_state_sums;

-- Total production by county 2021
SELECT state, countyName, SUM(netGeneration) as genSum, YEAR(monthYear) as prod_year
FROM (
	SELECT monthYear, plant_id, netGeneration
    FROM production
    WHERE YEAR(monthYear) = 2021
) AS year_county_net
JOIN plants ON year_county_net.plant_id = plants.id
JOIN counties ON plants.county_id = counties.id
GROUP BY state;

-- Production by month and year
SELECT monthYear, SUM(netGeneration), subcategoryName
FROM (
	SELECT monthYear, plant_id, fuel_id, netGeneration
    FROM production
) AS date_fuel_net
JOIN fuels ON date_fuel_net.fuel_id = fuels.id
JOIN (
	SELECT * 
    FROM fuel_subcategories
    WHERE subcategoryName <> 'Storage'
) AS valid_subcats ON fuels.subcategory_id = valid_subcats.id
GROUP BY monthYear, subcategoryName;

-- Production Totals
SELECT subcategoryName, SUM(netGeneration)
FROM (
	SELECT plant_id, fuel_id, netGeneration
    FROM production
) AS date_fuel_net
JOIN fuels ON date_fuel_net.fuel_id = fuels.id
JOIN (
	SELECT * 
    FROM fuel_subcategories
    WHERE subcategoryName <> 'Storage'
) AS valid_subcats ON fuels.subcategory_id = valid_subcats.id
GROUP BY subcategoryName, YEAR(monthYear);

-- Looking at generators that have started operation each year
SELECT opYear, COUNT(*)
FROM (
	SELECT YEAR(opYearMonth) AS opYear, gen_id, plant_id
    FROM generators
) AS gen_years
GROUP BY opYear
ORDER BY opYear;

-- Nameplate capacity per entity as of 2021-12
SELECT entity_id, entityName, SUM(namePlateCapacity_MW) AS totalCapacity, sectorid, sectorName
FROM (
	SELECT gen_id, plant_id
    FROM gen_dates
    WHERE (genstatus = 'OP' OR genstatus = 'SB') AND startDate <= '2021-12-01' AND endDate >= '2021-12-01'
) AS gd
JOIN (
	SELECT gen_id, plant_id, entity_id, namePlateCapacity_MW
    FROM generators
) AS gens ON gd.gen_id = gens.gen_id AND gd.plant_id = gens.plant_id
JOIN entities
ON gens.entity_id = entities.id
JOIN sectors
ON entities.sectorid = sectors.id
GROUP BY entityName
ORDER BY totalCapacity DESC;

-- Utility scale plants sorted by capacity (top 10)
WITH filtered_cap AS (
	SELECT gens.gen_id, gens.plant_id, entity_id, namePlateCapacity_MW, energy_source_id AS fuel_id
    FROM (
		SELECT gen_id, plant_id, energy_source_id, entity_id, namePlateCapacity_MW
        FROM generators
	) AS gens
	JOIN (
		SELECT gen_id, plant_id, genstatus
        FROM gen_dates
        WHERE startDate <= '2021-12-01' AND endDate >= '2021-12-01'
	) AS used_status ON gens.gen_id = used_status.gen_id AND gens.plant_id = used_status.plant_id
    WHERE genstatus = 'OP' OR genstatus = 'SB'
)
SELECT 
	plant_id,
	plantName,
    entityName,
    subcategoryName,
    state,
    countyName,
    SUM(namePlateCapacity_MW) AS subcatCapacity,
    SUM(SUM(namePlateCapacity_MW)) OVER (PARTITION BY plant_id) AS plantCapacity
FROM filtered_cap
JOIN plants ON filtered_cap.plant_id = plants.id
JOIN fuels ON filtered_cap.fuel_id = fuels.id
JOIN fuel_subcategories ON fuels.subcategory_id = fuel_subcategories.id
JOIN entities ON filtered_cap.entity_id = entities.id
JOIN counties ON plants.county_id = counties.id
JOIN states ON counties.state = states.symbol
GROUP BY plant_id, subcategory_id
ORDER BY plantCapacity DESC
LIMIT 10;

-- Capacity of each type 2021-12-01
SELECT YEAR(check_date), subcategoryName, SUM(COALESCE(namePlateCapacity_MW, 0))
FROM (
	SELECT gen_id, plant_id, check_date
	FROM gen_dates
	WHERE genstatus = 'OP' AND startDate <= '2021-12-01' AND endDate >= '2021-12-01'
) AS poss_gen_dates
JOIN generators ON poss_gen_dates.gen_id = generators.gen_id AND poss_gen_dates.plant_id = generators.plant_id
RIGHT JOIN fuels ON generators.energy_source_id = fuels.id
JOIN fuel_subcategories ON fuels.subcategory_id = fuel_subcategories.id
GROUP BY subcategoryName;

