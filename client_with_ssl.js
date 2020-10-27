'use strict';

const {Client} = require('hazelcast-client');
const fs = require('fs');
const path = require('path')

function createClientConfig() {
    return {
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
        console.log("Now, `map` will be filled with random entries.");
        while (true) {
            const randomKey = Math.floor(Math.random() * 100000);
            await map.put('key' + randomKey, 'value' + randomKey)
            await map.get('key' + randomKey);
            if (randomKey % 10 === 0) {
                map.size().then((size) => console.log(`map size: ${size}`));
            }
        }
    } catch (err) {
        console.error('Error occurred:', err);
    }
})();