var ELASTICSEARCH = require('elasticsearch');
var	CONFIG = require('./config.js');
var util = require('util');

var ES_CLIENT;

ES_CLIENT = new ELASTICSEARCH.Client({
	host: 'localhost:9200', 
	//requestTimeout: '1m'
		//log: 'trace'  //Disabled ES console logs (traces)
});

console.log('ES_CLIENT created');

function dumpESBulk(BULK_AR, cb) {
  console.log('Reached dumpESBulk');
  var br = (JSON.parse(JSON.stringify(BULK_AR)));
  //console.log(br);

  ES_CLIENT.bulk({
      body: br
    }).then(function(resp) {

        //console.log('DUMP DONE SID');
        util.log('Dump done');
        cb(null);
    },
    function(err) {
        console.log('DUMP NOT POSSIBLE');
      util.log(err.message);
      util.log('Dump faileddone till id = ' + end);
      cb(err);

    });
}

module.exports = dumpESBulk;

////Run func
(function() {
  if (require.main === module) {

    var startId = process.argv[2];
    var endId = process.argv[3];
    var timeoutTime = process.argv[4];

    if (!startId || !endId || !timeoutTime) {
      util.log("startID, endID, timeoutTime not specified properly");
      process.exit(1);
    }

    timeoutTime = timeoutTime * 1000; //(converting from user provided seconds to system taking ms)


    util.log("Starting to dump from id = " + startId + "  endId = " + endId + "  timeoutTime = " + timeoutTime + "ms");
    dumpES(startId, endId, timeoutTime, function(err, r) {
      if (err) {
        util.log("Some error while generating report");
        util.log(err && err.message);
        process.exit(1);
      } else {
        util.log("Process exited successfully. Report generated");
        process.exit(0);
      }
    });


    process.on('uncaughtException', function log(err) {
      util.log("Uncaught Error: " + err.message);
      util.log(err.stack);
      process.exit(1);
    });
  }
}());
