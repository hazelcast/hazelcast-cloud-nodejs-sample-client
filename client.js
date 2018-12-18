var Client = require('hazelcast-client').Client;
var ClientConfig = require('hazelcast-client').Config.ClientConfig;

function createClientConfig() {
    var fs = require('fs');
    var cfg = new ClientConfig();
    cfg.networkConfig.cloudConfig.enabled = true;
    cfg.networkConfig.cloudConfig.discoveryToken = 'YOUR_CLUSTER_DISCOVERY_TOKEN ';
    cfg.networkConfig.redoOperation = true;
    cfg.networkConfig.connectionAttemptLimit = 10;
    cfg.groupConfig.name = 'YOUR_CLUSTER_NAME';
    cfg.groupConfig.password = 'YOUR_CLUSTER_PASSWORD';
    cfg.properties['hazelcast.client.cloud.url'] = 'YOUR_DISCOVERY_URL';
    return cfg;
}

var cfg = createClientConfig();

Client.newHazelcastClient(cfg).then(function (client) {
    var map = client.getMap("testMap")

    map.put('key', 'value').then(function () {
        return map.get('key');
    }).then((res) => {
        console.log(res);
        client.shutdown();
    });
});
