//performance_monitor_logging.js

var ES = require('elasticsearch');
var util = require('util');
//var moment = require('moment');

var CONFIG = require('./../config');
var ES_OPS = require('./es_ops');

var ES_INDEX = {
	index: CONFIG.ELASTICSEARCH.INDEX,
	type: CONFIG.ELASTICSEARCH.TYPE
};

var esClient = ES_OPS.connect(CONFIG.ELASTICSEARCH.URL);

function getNPrintCount(){
	var body = {};
	ES_OPS.count(esClient, ES_INDEX, body, function(err, res){
		if(err){
			console.log('Error in running query');
		} else {
			//console.log('haha');
			//console.log(moment.now());
			timestamp = new Date().getTime();
			util.log('Count is => ' + res  +  '  Time = ' + timestamp);
		}
	})
}


setInterval(getNPrintCount, 1000);