var ELASTICSEARCH = require('elasticsearch');
var	CONFIG = require('./config.js');

var ES_CLIENT;

var ES_CLIENT = new ELASTICSEARCH.Client({
	host: CONFIG.ELASTICSEARCH.URL 
	requestTimeout: CONFIG.ELASTICSEARCH.REQUEST_TIMEOUT 
		//log: 'trace'  //Disabled ES console logs (traces)
});

console.log('ES_CLIENT CREATED');

function dumpESBulk(obj, BULK_AR, cb) {
  ES_CLIENT.bulk({
      body: BULK_AR
    }).then(function(resp) {
        cb(null);
    },
    function(err) {
      cb(err);
    });
}

module.exports = dumpESBulk;