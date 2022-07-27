const { Client, HazelcastJsonValue } = require('hazelcast-client');
const fs = require('fs');
const path = require('path')

async function mapExample(map) {
    console.log("Now the 'map' will be filled with city entries.");
    const citiesData = [
        { country: 'United Kingdom', name: 'London', population: 9_540_576 },
        { country: 'United Kingdom', name: 'Manchester', population: 1_890_976 },
        { country: 'United States', name: 'New York', population: 8_890_976 },
        { country: 'United States', name: 'Los Angeles', population: 3_840_376 },
        { country: 'Turkey', name: 'Istanbul', population: 1_890_912 },
        { country: 'Turkey', name: 'Ankara', population: 1_890_112 },
        { country: 'Brazil', name: 'Sao Paulo', population: 3_235_376 },
        { country: 'Brazil', name: 'Rio de Janeiro', population: 2_890_976 }
    ];
    await map.putAll(citiesData.map((map, index) => {
        return [index, new HazelcastJsonValue(JSON.stringify(map))];
    }));

    var mapSize = await map.size();
    console.log(`'Map' now contains ${mapSize} entries.`);
    console.log("--------------------");
}

async function sqlExample(hzClient) {
    console.log("Creating a mapping...");
    // See: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps
    const mappingQuery = `
        CREATE OR REPLACE MAPPING cities TYPE IMap 
        OPTIONS(
            'keyFormat'='varchar',
            'valueFormat'='varchar'
        );`;
    await hzClient.getSql().execute(mappingQuery);
    console.log("The mapping has been created successfully.");
    console.log("--------------------");

    console.log("Deleting data via SQL...");
    const deleteQuery = "DELETE FROM cities";
    await hzClient.getSql().execute(deleteQuery);
    console.log("The data has been deleted successfully.");
    console.log("--------------------");

    console.log("Inserting data via SQL...");
    const insertQuery = `
        INSERT INTO cities
        VALUES ('Australia', 'Canberra'),
               ('Croatia', 'Zagreb'),
               ('Czech Republic', 'Prague'),
               ('England', 'London'),
               ('Turkey', 'Ankara'),
               ('United States', 'Washington, DC');`;
    await hzClient.getSql().execute(insertQuery);
    console.log("The data has been inserted successfully.");
    console.log("--------------------");

    console.log("Retrieving all the data via SQL...");
    const sqlResultAll = await hzClient.getSql()
        .execute("SELECT * FROM cities", [], { returnRawResult: true });
    for await (const row of sqlResultAll) {
        const country = row.getObject(0);
        const city = row.getObject(1);
        console.log(`${country} - ${city}`);
    }
    console.log("--------------------");

    console.log("Retrieving a city name via SQL...");
    const sqlResultRecord = await hzClient.getSql()
        .execute("SELECT __key AS country, this AS city FROM cities WHERE __key = ?", ["United States"]);
    for await (const row of sqlResultRecord) {
        const country = row.country;
        const city = row.city;
        console.log(`Country name: ${country}; City name: ${city}`);
    }
    console.log("--------------------");
    process.exit(0);
}

async function jsonSerializationExample(hzClient) {
    await createMappingForCountries(hzClient);

    await populateCountriesMap(hzClient);

    await selectAllCountries(hzClient);

    await createMappingForCities(hzClient);

    await populateCityMap(hzClient);

    await selectCitiesByCountry(hzClient, "AU");

    await selectCountriesAndCities(hzClient);

    process.exit(0);
}

async function createMappingForCountries(hzClient) {
    // see: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps#json-objects
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

    await hzClient.getSql().execute(mappingQuery);
    console.log("Mapping for countries has been created.");
    console.log("--------------------");
}

async function populateCountriesMap(hzClient) {
    // see: https://docs.hazelcast.com/hazelcast/5.0/data-structures/creating-a-map#writing-json-to-a-map
    console.log("Populating 'countries' map with JSON values...");

    const countries = await hzClient.getMap("country");
    await countries.set("AU", { "isoCode": "AU", "country": "Australia" });
    await countries.set("EN", { "isoCode": "EN", "country": "England" });
    await countries.set("US", { "isoCode": "US", "country": "United States" });
    await countries.set("CZ", { "isoCode": "CZ", "country": "Czech Republic" });
    console.log("The 'countries' map has been populated.");
    console.log("--------------------");
}

async function selectAllCountries(hzClient) {
    const query = "SELECT c.country from country c";
    console.log("Select all countries with sql = " + query);

    const sqlResult = await hzClient.getSql().execute(query);
    for await (const row of sqlResult) {
        console.log(`country = ${row.country}`);
    }
    console.log("--------------------");
}

async function createMappingForCities(hzClient) {
    //see: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps#json-objects
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

    await hzClient.getSql().execute(mappingQuery);
    console.log("Mapping for cities has been created.");
    console.log("--------------------");
}

async function populateCityMap(hzClient) {
    // see: https://docs.hazelcast.com/hazelcast/5.0/data-structures/creating-a-map#writing-json-to-a-map
    console.log("Populating 'city' map with JSON values...");

    const cities = await hzClient.getMap("city");
    cities.set(1, { "country": "AU", "city": "Canberra", "population": 354644 });
    cities.set(2, { "country": "CZ", "city": "Prague", "population": 1227332 });
    cities.set(3, { "country": "EN", "city": "London", "population": 8174100 });
    cities.set(4, { "country": "US", "city": "Washington, DC", "population": 601723 });
    console.log("The 'city' map has been populated.");
    console.log("--------------------");
}

async function selectCitiesByCountry(hzClient, country) {
    const query = "SELECT city, population FROM city where country=?";
    console.log("Select city and population with sql = " + query);

    const sqlResult = await hzClient.getSql().execute(query, [country]);
    for await (const row of sqlResult) {
        console.log(`city = ${row.city}, population = ${row.population}`);
    }
    console.log("--------------------");
}

async function selectCountriesAndCities(hzClient) {
    const query = `
        SELECT c.isoCode, c.country, t.city, t.population
        FROM country c
        JOIN city t
        ON c.isoCode = t.country`;
    console.log("Select country and city data in query that joins tables");

    let rows = [];
    const sqlResult = await hzClient.getSql().execute(query);
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

async function nonStopMapExample(map) {
    console.log("Now, `map` will be filled with random entries.");

    let iterationCounter = 0;
    while (true) {
        const randomKey = Math.floor(Math.random() * 100000);
        await map.put('key' + randomKey, 'value' + randomKey);
        await map.get('key' + randomKey);
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
                        discoveryToken: 'YOUR_CLUSTER_DISCOVERY_TOKEN'
                    },
                    ssl: {
                        enabled: true,
                        sslOptions: {
                            ca: [fs.readFileSync(path.resolve(path.join(__dirname, 'ca.pem')))],
                            cert: [fs.readFileSync(path.resolve(path.join(__dirname, 'cert.pem')))],
                            key: [fs.readFileSync(path.resolve(path.join(__dirname, 'key.pem')))],
                            passphrase: 'YOUR_SSL_PASSWORD',
                            rejectUnauthorized: false
                        }
                    }
                },
                clusterName: 'YOUR_CLUSTER_NAME',
                properties: {
                    'hazelcast.client.cloud.url': 'YOUR_DISCOVERY_URL',
                    'hazelcast.client.statistics.enabled': true,
                    'hazelcast.client.statistics.period.seconds': 1,
                }
            }
        );
        const map = await client.getMap('map');
        console.log("Connection Successful!");

        await mapExample(map);

        // await sqlExample(client);

        // await jsonSerializationExample(client);

        // await nonStopMapExample(map)

    } catch (err) {
        console.error('Error occurred:', err);
    }
})();
