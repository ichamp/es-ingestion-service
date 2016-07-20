//Reading bulk from mysql streaming. pausing stream, dumping ES, resuming streaming.

//Migration script from DB to ES for provider-logs

//Doing all things in one file as this script is meant for migrating once only.

/*
This script generates reports about bus ticketing using ElasticSearch queries(aggregations)

How to run:
node migration_DB_to_ES_provider_logs <startId> <endId> <timeoutTime>
example: node migration_DB_to_ES_provider_logs 1 1000000 120
timeoutTime is in seconds

The script will migrate provider logs from mySQL to ElasticSearch for selected Id's
startDate
*/
"use strict";

global.__base = __dirname + '/../../';
var mysql = require('mysql');
var util = require('util');
var elasticsearch = require('elasticsearch');

//var es_config = require(global.__base + 'cfg/ticket-config').es_configs.provider_logs;

var ES_TYPE_LOG = {
  index: "provider-logs", //Alias to allow dynamic reindexing. actual index mapping is provider-logs-v1
  type: "logs"
};

//var PENDING_REQUEST = 0;  //debug points

var COUNTER = 0;
var BULK_SIZE = 1000;
var BULK_AR = [];

var START = 0;
var END = 0;

var QUERY;
var QUERYSTR;

var CLIENT;
var CONNECTION;



function getText(blob) {
  if (blob === null)
    return "na";

  var bufferBase64 = blob.toString('utf-8');
  return bufferBase64;
}

function processRow(row, forceDump, cb) {

  if (forceDump === true) {
    CONNECTION.pause(); //trial
    dumpESBulk(BULK_AR, START, END, true);
    COUNTER = 0;
    START = 0;
    END = 0;
    BULK_AR.splice(0, BULK_AR.length);
  } else {

    var dumpObj = {
      "providerName": row.provider_name || "na",
      "method": row.method || "na",
      "url": row.url || "na",
      "argsJson": getText(row.args_json),
      "responseCode": row.response_code || "na",
      "responseBody": getText(row.response_body), //row['response_body'] || "na",
      "createdAt": row.created_at,
      "updatedAt": row.updated_at,
      "isRequest": row.is_request || "-1",
      "requestHeaders": row.request_headers || "na",
      "requestBody": getText(row.request_body), //row['request_body'] || "na",
      "responseTime": row.response_time
    };

    if (START === 0) {
      START = row.id;
      //console.log("start is " + START);
    }
    END = row.id;
    BULK_AR.push({
      index: {
        _index: ES_TYPE_LOG.index,
        _type: ES_TYPE_LOG.type,
        _id: row.id
      }
    });
    BULK_AR.push(dumpObj);
    COUNTER++;

    //util.log(util.inspect(BULK_AR));

    if (COUNTER === BULK_SIZE) {
      CONNECTION.pause(); //trial
      dumpESBulk(BULK_AR, START, END, false);
      COUNTER = 0;
      START = 0;
      END = 0;
      BULK_AR.splice(0, BULK_AR.length);
    } else {
      //populate BULK_AR
      if (START === 0) {
        START = row.id;
        //console.log("start is " + START);
      }
    }
    cb();
  }
}



function dumpESBulk(BULK_AR, start, end, forceDump) {
  //PENDING_REQUEST++;  //debug points
  var br = (JSON.parse(JSON.stringify(BULK_AR)));
  //console.log("PENDING = " + PENDING_REQUEST);

  CLIENT.bulk({
    body: br
  }).then(function(resp) {

    //var line = start + '__' + end;
    //util.log("SUCCESS " + line);
    CONNECTION.resume();
    //PENDING_REQUEST--;  //debug points
    if (forceDump) {
      util.log("Dump done till id = " + end);
      process.exit(0);
    }

  }, function(err) {

    util.log(err.message);
    var line = start + '__' + end;
    util.log("FAILURE " + line);
    CONNECTION.resume();
    //PENDING_REQUEST--;  //debug points
    if (forceDump) {
      util.log("Dump done till id = " + end);
      process.exit(0);
    }
  });
}


function dumpES(startId, endId, timeoutTime) {


  CLIENT = new elasticsearch.Client({
    host: 'localhost:9200', //es_config.host,
    requestTimeout: timeoutTime
      //log: 'trace'  //Disabled ES console logs (traces)
  });

  CONNECTION = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'paytm@197',
    database: 'ticketing_development'
  });

  CONNECTION.connect();


  QUERYSTR = 'SELECT * FROM provider_logs where id >= ' + startId + " AND id <= " + endId;
  util.log(QUERYSTR);
  QUERY = CONNECTION.query(QUERYSTR);
  QUERY
    .on('error', function(err) {
      // Handle error, an 'end' event will be emitted after this as well
    })
    .on('fields', function(fields) {
      // the field packets for the rows to follow
    })
    .on('result', function(row) {
      //CONNECTION.pause();
      processRow(row, false, function() {
        //CONNECTION.resume();
      });
    })
    .on('end', function() {
      // all rows have been received
      processRow(true, true, function() { //To do forceful dump
        //CONNECTION.resume();
      });
    });
}


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
