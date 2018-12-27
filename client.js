var async = require('async');
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
    cfg.properties['hazelcast.client.statistics.enabled'] = true;
    cfg.properties['hazelcast.client.statistics.period.seconds'] = 1;
    return cfg;
}

var cfg = createClientConfig();

Client.newHazelcastClient(cfg).then(function (hazelcastClient) {
    var client = hazelcastClient;
    var map;
    hazelcastClient.getMap('map').then(function (mp) {
      map = mp;

      map.put('key', 'value').then(function () {
          return map.get('key');
      }).then((res) => {
          if(res === 'value')
          {
              console.log("Connection Successful!");
              console.log("Now, `map` will be filled with random entries.");

              async.whilst(() => {
                return true;
              },(next) => {
                var randomKey = Math.floor(Math.random() * 100000);
                map.put('key' + randomKey, 'value' + randomKey).then(function () {
                    map.get('key' + randomKey);
                    if (randomKey % 10 == 0) {
                        map.size().then((size) => console.log(`map size: ${size}`));
                    }
                    next();
                });
              },(err) => {
                client.shutdown();
              });
          }
          else {
              throw new Error("Connection failed, check your configuration.");
          }
      });
    });
});
