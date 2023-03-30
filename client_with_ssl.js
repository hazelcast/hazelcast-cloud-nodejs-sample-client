// Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

const { Client } = require('hazelcast-client');
const fs = require('fs');
const path = require('path');
const printf = require('printf');

async function createMapping(client) {
    console.log("Creating the mapping...");
    // Mapping is required for your distributed map to be queried over SQL.
    // See: https://docs.hazelcast.com/hazelcast/latest/sql/mapping-to-maps
    const mappingQuery = `
        CREATE OR REPLACE MAPPING 
        cities (
            __key INT,                                        
            country VARCHAR,
            city VARCHAR,
            population INT) TYPE IMAP
        OPTIONS ( 
            'keyFormat' = 'int',
            'valueFormat' = 'compact',
            'valueCompactTypeName' = 'city')`;
    await client.sqlService.execute(mappingQuery);
    console.log("OK.");
}

async function populateCities(client) {
    console.log("\nInserting cities into 'cities' map...");
    const insertQuery = `
            INSERT INTO cities 
            (__key, city, country, population) VALUES
            (1, 'London', 'United Kingdom', 9540576),
            (2, 'Manchester', 'United Kingdom', 2770434),
            (3, 'New York', 'United States', 19223191),
            (4, 'Los Angeles', 'United States', 3985520),
            (5, 'Istanbul', 'Türkiye', 15636243),
            (6, 'Ankara', 'Türkiye', 5309690),
            (7, 'Sao Paulo ', 'Brazil', 22429800)`;

    try {
        await client.sqlService.execute(insertQuery);
        console.log("OK.");
    } catch (error) {
        console.log("FAILED.", error)
    }


    // Let's also add a city as object.
    console.log("Putting a city into 'cities' map...");
    const cities = await client.getMap("cities");
    await cities.put(parseInt(8), new City("Rio de Janeiro", "Brazil", 13634274));
    console.log("OK.");
}

async function fetchCities(client) {
    console.log("Fetching cities via SQL...");
    const sqlResultAll = await client.sqlService
        .execute("SELECT __key, this FROM cities", [], { returnRawResult: true });

    console.log("OK.");
    console.log("--Results of 'SELECT __key, this FROM cities'");
    printf("| %4s | %20s | %20s | %15s |\n", "id", "country", "city", "population");

    for await (const row of sqlResultAll) {
        const id = row.getObject("__key");
        const city = row.getObject("this");
        printf("| %4d | %20s | %20s | %15d |\n", id, city.country, city.cityName, city.population)
    }

    console.log("\n!! Hint !! You can execute your SQL queries on your Viridian cluster over the management center. \n 1. Go to 'Management Center' of your Hazelcast Viridian cluster. \n 2. Open the 'SQL Browser'. \n 3. Try to execute 'SELECT * FROM cities'.\n");
}

class City {
    constructor(city, country, population) {
        this.city = city;
        this.country = country;
        this.population = population;
    }
}

class CitySerializer {
    getClass() {
        return City;
    }

    getTypeName() {
        return 'city';
    }

    read(reader) {
        const population = reader.readInt32('population');
        const city = reader.readString('city');
        const country = reader.readString('country');
        return new City(city, country, population);
    }

    write(writer, value) {
        writer.writeInt32('population', value.population);
        writer.writeString('city', value.city);
        writer.writeString('country', value.country);
    }
}

/* A sample application that configures a client to connect to an Hazelcast Viridian cluster
*  over TLS, and to then insert and fetch data with SQL, thus testing that the connection to 
*  the Hazelcast Viridian cluster is successful.
* 
*  See: https://docs.hazelcast.com/cloud/get-started
*/
(async () => {
    try {
        const client = await Client.newHazelcastClient(
            {
                network: {
                    hazelcastCloud: {
                        discoveryToken: 'YOUR_CLUSTER_DISCOVERY_TOKEN '
                    },
                    ssl: {
                        enabled: true,
                        sslOptions: {
                            ca: [fs.readFileSync(path.resolve(path.join(__dirname, 'ca.pem')))],
                            cert: [fs.readFileSync(path.resolve(path.join(__dirname, 'cert.pem')))],
                            key: [fs.readFileSync(path.resolve(path.join(__dirname, 'key.pem')))],
                            passphrase: 'YOUR_CLUSTER_PASSWORD',
                            checkServerIdentity: () => null
                        }
                    }
                },
                clusterName: 'YOUR_CLUSTER_NAME',                
                serialization: {
                    compact: {
                        serializers: [new CitySerializer()]
                    }
                }
            }
        );
        console.log("Connection Successful!");

        await createMapping(client);
        await populateCities(client);
        await fetchCities(client);

        await client.shutdown();
    } catch (err) {
        console.error('Error occurred:', err);
    }
})();
