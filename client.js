'use strict';

const {Client, HazelcastJsonValue} = require('hazelcast-client');

// This example shows how to work with Hazelcast maps.
// @param client - a {@link HazelcastClient} client.
//
async function mapExample(client){
    const cities = await client.getMap('cities');
    await cities.put(1, new HazelcastJsonValue(JSON.stringify({ country: "United Kingdom", city: "London", population: 9_540_576})));
    await cities.put(2, new HazelcastJsonValue(JSON.stringify({ country: "United Kingdom", city: "Manchester", population: 2_770_434})));
    await cities.put(3, new HazelcastJsonValue(JSON.stringify({ country: "United States", city: "New York", population: 19_223_191})));
    await cities.put(4, new HazelcastJsonValue(JSON.stringify({ country: "United States", city: "Los Angeles", population: 3_985_520})));
    await cities.put(5, new HazelcastJsonValue(JSON.stringify({ country: "Turkey", city: "Ankara", population: 5_309_690})));
    await cities.put(6, new HazelcastJsonValue(JSON.stringify({ country: "Turkey", city: "Istanbul", population: 15_636_243})));
    await cities.put(7, new HazelcastJsonValue(JSON.stringify({ country: "Brazil", city: "Sao Paulo", population: 22_429_800})));
    await cities.put(8, new HazelcastJsonValue(JSON.stringify({ country: "Brazil", city: "Rio de Janeiro", population: 13_635_274})));

    const mapSize = await cities.size();
    console.log(`'cities' map now contains ${mapSize} entries.`);

    console.log("--------------------");
}

// This example shows how to work with Hazelcast SQL queries.
//
// @param client - a {@link HazelcastClient} client.
//
async function sqlExample(hzClient) {

    const sqlService = await hzClient.getSql();
    await createMappingForCapitals(sqlService);

    await clearCapitals(sqlService);

    await populateCapitals(sqlService);

    await selectAllCapitals(sqlService);

    await selectCapitalNames(sqlService);
}

async function createMappingForCapitals(sqlService){
    console.log("Creating a mapping...");
    // See: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps
    const mappingQuery = `
        CREATE OR REPLACE MAPPING capitals TYPE IMap 
        OPTIONS(
            'keyFormat'='varchar',
            'valueFormat'='varchar'
        );`;
    await sqlService.execute(mappingQuery);
    console.log("The mapping has been created successfully.");
    console.log("--------------------");
}

async function clearCapitals(sqlService){
    console.log("Deleting data via SQL...");
    const deleteQuery = "DELETE FROM capitals";
    await sqlService.execute(deleteQuery);
    console.log("The data has been deleted successfully.");
    console.log("--------------------");
}

async function populateCapitals(sqlService){
    console.log("Inserting data via SQL...");
    const insertQuery = `
        INSERT INTO capitals
        VALUES ('Australia', 'Canberra'),
               ('Croatia', 'Zagreb'),
               ('Czech Republic', 'Prague'),
               ('England', 'London'),
               ('Turkey', 'Ankara'),
               ('United States', 'Washington, DC');`;
    await sqlService.execute(insertQuery);
    console.log("The data has been inserted successfully.");
    console.log("--------------------");
}

async function selectAllCapitals(sqlService){
    console.log("Retrieving all the data via SQL...");
    const sqlResultAll = await sqlService
        .execute("SELECT * FROM capitals", [], {returnRawResult: true});
    for await (const row of sqlResultAll) {
        const country = row.getObject(0);
        const city = row.getObject(1);
        console.log(`${country} - ${city}`);
    }
    console.log("--------------------");
}

async function selectCapitalNames(sqlService){
    console.log("Retrieving a city name via SQL...");
    const sqlResultRecord = await sqlService
        .execute( "SELECT __key, this FROM capitals WHERE __key = ?", ["United States"], {returnRawResult: true});
    for await (const row of sqlResultRecord) {
        const country = row.getObject('__key');
        const city = row.getObject('this');
        console.log(`Country name: ${country}; Capital name: ${city}`);
    }
    console.log("--------------------");
}

async function jsonSerializationExample(hzClient) {

    const sqlService = hzClient.getSql();

    await createMappingForCountries(sqlService);

    await populateCountriesWithMap(hzClient);

    await selectAllCountries(sqlService);

    await createMappingForCities(sqlService);

    await populateCities(hzClient);

    await selectCitiesByCountry(sqlService, "AU");

    await selectCountriesAndCities(sqlService);
}

async function createMappingForCountries(sqlService) {
    // see: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps#json-objects
    console.log("Creating mapping for countries...");

    const mappingQuery = `
        CREATE OR REPLACE MAPPING country(
            __key VARCHAR, 
            isoCode VARCHAR, 
            country VARCHAR
        )
        TYPE IMap OPTIONS(
            'keyFormat' = 'varchar',
            'valueFormat' = 'json-flat'
        );`;

    await sqlService.execute(mappingQuery);
    console.log("Mapping for countries has been created.");
    console.log("--------------------");
}

async function populateCountriesWithMap(hzClient) {
    // see: https://docs.hazelcast.com/hazelcast/5.1/data-structures/creating-a-map#writing-json-to-a-map
    console.log("Populating 'countries' map with JSON values...");

    const countries = await hzClient.getMap("country");
    await countries.put("AU", {"isoCode": "AU", "country": "Australia"});
    await countries.put("EN", {"isoCode": "EN", "country": "England"});
    await countries.put("US", {"isoCode": "US", "country": "United States"});
    await countries.put("CZ", {"isoCode": "CZ", "country": "Czech Republic"});
    console.log("The 'countries' map has been populated.");
    console.log("--------------------");
}

async function selectAllCountries(sqlService) {
    const query = "SELECT c.country from country c";
    console.log("Select all countries with sql = " + query);

    const sqlResult = await sqlService.execute(query);
    for await (const row of sqlResult) {
        console.log(`country = ${row.country}`);
    }
    console.log("--------------------");
}

async function createMappingForCities(sqlService) {
    //see: https://docs.hazelcast.com/hazelcast/5.1/sql/mapping-to-maps#json-objects
    console.log("Creating mapping for cities...");

    const mappingQuery = `
        CREATE OR REPLACE MAPPING city(
            __key INT, 
            country VARCHAR, 
            city VARCHAR, 
            population BIGINT
        ) 
        TYPE IMap OPTIONS (
            'keyFormat' = 'int',
            'valueFormat' = 'json-flat'
        );`;

    await sqlService.execute(mappingQuery);
    console.log("Mapping for cities has been created.");
    console.log("--------------------");
}

async function populateCities(hzClient) {
    // see: https://docs.hazelcast.com/hazelcast/5.1/data-structures/creating-a-map#writing-json-to-a-map
    console.log("Populating 'city' map with JSON values...");

    const cities = await hzClient.getMap("city");
    await cities.put(1, {"country": "AU", "city": "Canberra", "population": 467_194});
    await cities.put(2, {"country": "CZ", "city": "Prague", "population": 1_318_085});
    await cities.put(3, {"country": "EN", "city": "London", "population": 9_540_576});
    await cities.put(4, {"country": "US", "city": "Washington, DC", "population": 7_887_965});
    console.log("The 'city' map has been populated.");
    console.log("--------------------");
}

async function selectCitiesByCountry(sqlService, country) {
    const query = "SELECT city, population FROM city where country=?";
    console.log("Select city and population with sql = " + query);

    const sqlResult = await sqlService.execute(query, [country]);
    for await (const row of sqlResult) {
        console.log(`city = ${row.city}, population = ${row.population}`);
    }
    console.log("--------------------");
}

async function selectCountriesAndCities(sqlService) {
    const query = `
        SELECT c.isoCode, c.country, t.city, t.population
        FROM country c
        JOIN city t
        ON c.isoCode = t.country`;
    console.log("Select country and city data in query that joins tables");

    let rows = [];
    const sqlResult = await sqlService.execute(query);
    for await (const row of sqlResult) {
        rows.push({
            "iso": row.isoCode,
            "country": row.country,
            "city": row.city,
            "population": parseInt(row.population)
        });
    }
    console.table(rows);
    console.log("--------------------");
}

//   This example shows how to work with Hazelcast maps, where the map is
//   updated continuously.
//
//   @param client - a {@link HazelcastClient} client.
//
async function nonStopMapExample(client) {
    console.log("Now, `map` will be filled with random entries.");
    const map = await client.getMap('map');

    let iterationCounter = 0;
    while (true) {
        const randomKey = Math.floor(Math.random() * 100000);
        await map.put('key' + randomKey, 'value' + randomKey);
        await map.get('key' + Math.floor(Math.random() * 100000))
        if (++iterationCounter === 10) {
            iterationCounter = 0;
            const size = await map.size();
            console.log(`Current map size: ${size}`);
        }
    }
}

(async () => {
    try {
        const client = await Client.newHazelcastClient(
    {
                network: {
                    hazelcastCloud: {
                        discoveryToken: 'YOUR_DISCOVERY_TOKEN'
                    }
                },
                clusterName: 'YOUR_CLUSTER_NAME'
            }
        );
        console.log("Connection Successful!");

        await mapExample(client);

        // await sqlExample(client);

        // await jsonSerializationExample(client);

        // await nonStopMapExample(client)

        client.shutdown();
    } catch (err) {
        console.error('Error occurred:', err);
    }
})();
