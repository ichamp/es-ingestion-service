var elasticsearch = require('elasticsearch');
var	config = require('./config.js');

var esClient;

esClient = new elasticsearch.Client({
	host: 'localhost:9200', 
	requestTimeout: timeoutTime
		//log: 'trace'  //Disabled ES console logs (traces)
});


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
