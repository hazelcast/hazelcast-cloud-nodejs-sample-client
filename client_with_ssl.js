var async = require('async');
var Client = require('hazelcast-client').Client;
var ClientConfig = require('hazelcast-client').Config.ClientConfig;
var path = require('path')

function createClientConfig() {
    var fs = require('fs');

    var caFile = path.resolve(path.join(__dirname, 'ca.pem'));
    var certFile = path.resolve(path.join(__dirname, 'cert.pem'));
    var keyFile = path.resolve(path.join(__dirname, 'key.pem'));

    var cfg = new ClientConfig();
    
    cfg.networkConfig.sslConfig.enabled = true;
    cfg.networkConfig.sslConfig.sslOptions = {
        rejectUnauthorized: false,
        ca: [fs.readFileSync(caFile)],
        cert: [fs.readFileSync(certFile)],
        key: [fs.readFileSync(keyFile)],
        passphrase: 'YOUR_SSL_PASSWORD'
    };

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

              var iterationCounter = 0;
              async.whilst(() => {
                return true;
              },(next) => {
                var randomKey = Math.floor(Math.random() * 100000);
                map.put('key' + randomKey, 'value' + randomKey).then(function () {
                    map.get('key' + randomKey);
                    if (++iterationCounter === 10) {
                        iterationCounter = 0;
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
