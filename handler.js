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

	processSingle: function(channel, data) {
		debug('entered processSingle');
		var self = this;

		if(self.channelSet === false){
			self.channelSet = true;
			self.channel = channel;
		}

		data.content = data.content.toString();

		self.syncAr[self.counter] = data;
		self.counter++;
		self.byteSize += data.content.length;
		debug('byteSize = ' + self.byteSize);

		if (CONFIG.FLAGS.BULK_DECISION == 'LENGTH' && self.counter == CONFIG.ELASTICSEARCH.BULK_SIZE) {
			debug('Adding bulk thread by LENGTH clause');
			self.addThreadRequest();

		} else if (CONFIG.FLAGS.BULK_DECISION == 'MEMORY' && (self.byteSize >= CONFIG.ELASTICSEARCH.BULK_SIZE_MB * 1000000) ) {
			debug('Adding bulk thread by MEMORY clause');
			self.addThreadRequest();
		}
	},

	addThreadRequest: function() {
		debug('entered addThreadRequest');
		var self = this;

		debug('below is the data send for enqueuing');
		debug(JSON.stringify(self.syncAr));

		QUEUE.enqueue(JSON.stringify(self.syncAr));

		self.cleanupBatch();

		while ((self.concurrency < self.max_concurrency) && QUEUE.length()) {
			self.executeBulkThread();
		}
	},

	executeBulkThread: function(channel) {
		debug('entered executeBulkThread');
		var self = this;

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

				self.concurrency++;

				util.log('PARALLELISM = ' + self.concurrency);
				util.log('Queue length = ' + QUEUE.length());

				FN_DUMP_ES(data, bulkAr, function(err, res) {
					if (err) {
						util.log(err.message);
						util.log('NACKING data of length => ' + data.length);
						for (var i = 0; i < data.length; i++) {
							self.channel.reject(data[i], true);
						}

						self.concurrency--;
						util.log('PARALLELISM DECREASED = ' + self.concurrency);
						//self.executeBulkThread();

					} else {
						util.log('ACKING data of length => ' + data.length);
						for (var i = 0; i < data.length; i++) {
							self.channel.ack(data[i]);
						}

						self.concurrency--;
						util.log('PARALLELISM DECREASED = ' + self.concurrency);
					}
				});
			}
		
	},

	cleanupBatch: function(){
		debug('entered cleanupBatch');
		var self = this;

		self.syncAr.splice(0,self.syncAr.length);
		self.counter = 0;
		self.byteSize = 0;
	}
};

module.exports = handler;
