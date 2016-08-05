var http = require('http');
var util = require('util');
var debug = require('debug')('handler');

var FN_DUMP_ES = require('./es_bulk');
var CONFIG = require('./config');
var QUEUE = require('./queue');


http.globalAgent.maxSockets = 50;

function copy_array(arr){
	var ar = [];
	return JSON.parse(JSON.stringify(arr));
}

var PARALLELISM = 0;

var handler = {

	syncAr: [],
	counter: 0,

	byteSize: 0,

	concurrency: 0,

	max_concurrency: CONFIG.ELASTICSEARCH.BULK_CONCURRENCY,

	channel : null,

	channelSet : false,

	timeoutTimer : 0,

	timeoutConfigTime: 10000,

	processSingle: function(channel, data) {
		debug('entered processSingle');
		//var handler = this;

		if(handler.channelSet === false){
			handler.channelSet = true;
			handler.channel = channel;
		}

		data.content = data.content.toString();

		handler.syncAr[handler.counter] = data;
		handler.counter++;
		handler.byteSize += data.content.length;
		debug('byteSize = ' + handler.byteSize);

		if (CONFIG.FLAGS.BULK_DECISION == 'LENGTH' && handler.counter == CONFIG.ELASTICSEARCH.BULK_SIZE) {
			debug('Adding bulk thread by LENGTH clause');
			handler.addThreadRequest();

		} else if (CONFIG.FLAGS.BULK_DECISION == 'MEMORY' && (handler.byteSize >= CONFIG.ELASTICSEARCH.BULK_SIZE_MB * 1000000) ) {
			debug('Adding bulk thread by MEMORY clause');
			handler.addThreadRequest();
		}

		clearTimeout(handler.timeoutTimer);
		handler.timeoutTimer = setTimeout(handler.addThreadRequest, handler.timeoutConfigTime);

	},

	addThreadRequest: function() {
		debug('entered addThreadRequest');
		//var handler = this;
		debug('below is the data send for enqueuing');
		debug(JSON.stringify(handler.syncAr));

		QUEUE.enqueue(JSON.stringify(handler.syncAr));

		handler.cleanupBatch();

		while ((handler.concurrency < handler.max_concurrency) && QUEUE.length()) {
			handler.executeBulkThread();
		}
	},

	executeBulkThread: function(channel) {
		debug('entered executeBulkThread');
		//var handler = this;

			var data = QUEUE.dequeue();

			try {
				data = JSON.parse(data);
			} catch (err) {
				util.log('Error in parsing json from string');
				console.log(err);
			}
			
			debug('DATA SEEMS EXISTING as below');
			debug(data);
			if (data) {

				var bulkAr = [];
				data.forEach(function(singleData) {
					bulkAr.push(singleData.content);

					var buf = new Buffer(singleData.content, "utf-8");
					singleData.content = buf;
				});

				handler.concurrency++;

				util.log('PARALLELISM = ' + handler.concurrency);
				util.log('Queue length = ' + QUEUE.length());

				FN_DUMP_ES(data, bulkAr, function(err, res) {
					if (err) {
						util.log(err.message);
						util.log('NACKING data of length => ' + data.length);
						for (var i = 0; i < data.length; i++) {
							handler.channel.reject(data[i], true);
						}

						handler.concurrency--;
						util.log('PARALLELISM DECREASED = ' + handler.concurrency);
						//handler.executeBulkThread();

					} else {
						util.log('ACKING data of length => ' + data.length);
						for (var i = 0; i < data.length; i++) {
							handler.channel.ack(data[i]);
						}

						handler.concurrency--;
						util.log('PARALLELISM DECREASED = ' + handler.concurrency);
					}
				});
			}
		
	},

	cleanupBatch: function(){
		debug('entered cleanupBatch');
		//var handler = this;

		handler.syncAr.splice(0,handler.syncAr.length);
		handler.counter = 0;
		handler.byteSize = 0;

	}
};

module.exports = handler;
