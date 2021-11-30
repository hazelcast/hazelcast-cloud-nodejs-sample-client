'use strict';

const {Client} = require('hazelcast-client');

function createClientConfig() {
    return {
        network: {
            hazelcastCloud: {
                discoveryToken: 'YOUR_CLUSTER_DISCOVERY_TOKEN'
            }
        },
        clusterName: 'YOUR_CLUSTER_NAME',
        properties: {
            'hazelcast.client.cloud.url': 'YOUR_DISCOVERY_URL',
            'hazelcast.client.statistics.enabled': true,
            'hazelcast.client.statistics.period.seconds': 1,
        }
    }
}

function mapExample(map) {
    (async () => {
        console.log("Now, `map` will be filled with random entries.");

        var iterationCounter = 0;
        while (true) {
            const randomKey = Math.floor(Math.random() * 100000);
            await map.put('key' + randomKey, 'value' + randomKey);
            await map.get('key' + randomKey);
            if (++iterationCounter === 10) {
                iterationCounter = 0;
                map.size().then((size) => console.log(`map size: ${size}`));
            }
        }
    })();
}

function sqlExample(hzClient) {
    (async () => {
        console.log("Creating a mapping...");
        // See: https://docs.hazelcast.com/hazelcast/5.0/sql/mapping-to-maps
        const mappingQuery = "CREATE OR REPLACE MAPPING cities TYPE IMap OPTIONS " +
            "('keyFormat'='varchar','valueFormat'='varchar')";
        await hzClient.getSql().execute(mappingQuery);
        console.log("The mapping has been created successfully.");
        console.log("--------------------");

        console.log("Deleting data via SQL...");
        const deleteQuery = "DELETE FROM cities";
        await hzClient.getSql().execute(deleteQuery);
        console.log("The data has been deleted successfully.");
        console.log("--------------------");

        console.log("Inserting data via SQL...");
        const insertQuery = "INSERT INTO cities VALUES " +
            "('Australia','Canberra')," +
            "('Croatia','Zagreb')," +
            "('Czech Republic','Prague')," +
            "('England','London')," +
            "('Turkey','Ankara')," +
            "('United States','Washington, DC');";
        await hzClient.getSql().execute(insertQuery);
        console.log("The data has been inserted successfully.");
        console.log("--------------------");

        console.log("Retrieving all the data via SQL...");
        const sqlResultAll = await hzClient.getSql()
            .execute("SELECT * FROM cities", [], {returnRawResult: true});
        for await (const row of sqlResultAll) {
            let country = row.getObject(0);
            let city = row.getObject(1);
            console.log("%s - %s", country, city);
        }
        console.log("--------------------");

        console.log("Retrieving a city name via SQL...");
        const sqlResultRecord = await hzClient.getSql()
            .execute("SELECT __key AS country, this AS city FROM cities WHERE __key = ?", ["United States"]);
        for await (const row of sqlResultRecord) {
            let country = row.country;
            let city = row.city;
            console.log("Country name: %s; City name: %s", country, city);
        }
        console.log("--------------------");
        process.exit(0)
    })();
}

(async () => {
    try {
        const client = await Client.newHazelcastClient(createClientConfig());
        const map = await client.getMap('map');
        await map.put("key", "value")
        const res = await map.get("key")
        if (res !== "value") {
            throw new Error("Connection failed, check your configuration.")
        }
        console.log("Connection Successful!");

        // the 'mapExample' is an example with an infinite loop inside, so if you'd like to try other examples,
        // don't forget to comment out the following line
        mapExample(map);

        // sqlExample(client);

    } catch (err) {
        console.error('Error occurred:', err);
    }
})();
