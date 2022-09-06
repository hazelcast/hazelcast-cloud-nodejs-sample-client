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

//   This example shows how to work with Hazelcast maps, where the map is
//   updated continuously.
//
//   @param client - a {@link HazelcastClient} client.
//
async function nonStopMapExample(client) {
    console.log("Now, the map named `map` will be filled with random entries.");
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

// This is boilerplate application that configures client to connect Hazelcast
// Cloud cluster.
// see: https://docs.hazelcast.com/cloud/nodejs-client
//
(async () => {
    try {
        const client = await Client.newHazelcastClient(
            {
                network: {
                    hazelcastCloud: {
                        discoveryToken: 'YOUR_CLUSTER_DISCOVERY_TOKEN'
                    }
                },
                clusterName: 'YOUR_CLUSTER_NAME',
                properties: {
                    'hazelcast.client.cloud.url': 'YOUR_DISCOVERY_URL',
                    'hazelcast.client.statistics.enabled': true,
                    'hazelcast.client.statistics.period.seconds': 5,
                }
            }
        );
        console.log("Connection Successful!");

        await mapExample(client);

        // await nonStopMapExample(client)

        await client.shutdown();
    } catch (err) {
        console.error('Error occurred:', err);
    }
})();
