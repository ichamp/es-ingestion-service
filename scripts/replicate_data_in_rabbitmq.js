// Access the callback-based API
var amqp = require('amqplib/callback_api');

var CONFIG = require('./../config');
//var HANDLER = require('./handler');

CONFIG.RABBITMQ.QUEUE_NAME = "dump10";
CONFIG.RABBITMQ.PREFETCH_COUNT = 10000;
console.log('printing config object from subscriber.js');
console.log(CONFIG);

var amqpConn = null;

function start() {
	console.log('Hi sid');
	amqp.connect(CONFIG.RABBITMQ.CONNECT_STRING, function(err, conn) {
		if (err) {
			console.error("[AMQP]", err.message);
			return setTimeout(start, 1000);
		}
		conn.on("error", function(err) {
			if (err.message !== "Connection closing") {
				console.error("[AMQP] conn error", err.message);
			}
		});
		conn.on("close", function() {
			console.error("[AMQP] reconnecting");
			return setTimeout(start, 1000);
		});
		console.log("[AMQP] connected");
		amqpConn = conn;
		whenConnected();
	});
}

function whenConnected() {
	startPublisher();
	startWorker();
}


var pubChannel = null;
var offlinePubQueue = [];

// A worker that acks messages only if processed successfully
function startWorker() {
	console.log('SID started worker')
	amqpConn.createChannel(function(err, ch) {
		if (closeOnErr(err)) return;
		ch.on("error", function(err) {
			console.error("[AMQP] channel error", err.message);
		});
		ch.on("close", function() {
			console.log("[AMQP] channel closed");
		});

		console.log('setting prefetch value SID');
		ch.prefetch(CONFIG.RABBITMQ.PREFETCH_COUNT);
		ch.assertQueue("q_catalog_refiner", {
			durable: true
		}, function(err, _ok) {
			if (closeOnErr(err)) return;
			ch.consume(CONFIG.RABBITMQ.QUEUE_NAME, processMsg, {
				noAck: false
			});
			console.log("Worker is started");
		});

		function processMsg(msg){
			//console.log('Processing single message SID');
			//console.log(msg.content.toString());
			//console.log('GET IT PROCESSED FROM HANDLER');
			//ch.ack(msg);
			//console.log(JSON.stringify(ch));
			//ch.ack(msg);

			//var id = msg.content.toString();
			//console.log(id);

			//HANDLER.processSingle(ch, msg);

			//publish("exchange_jobs", "key_jobs", new Buffer("work work work" + GCOUNT++));
			console.log('Sid reached process Msg');
			publish("exchange_jobs", "key_jobs", msg.content);
			ch.ack(msg);
		}

		/*
		function processMsg(msg) {
			work(msg, function(ok) {
				console.log('REACHED CB OF WORK()');
				try {
					if (ok){
						//console.log('ACKED MSG ' + msg.content.toString());
						ch.ack(msg);
						console.log('ACKED MSG ' + msg.content.toString());
						console.log(JSON.stringify(msg));

					}
					else{
						console.log('NACKED msg ' + msg.content.toString());
						ch.reject(msg, true);
					}
				} catch (e) {
					closeOnErr(e);
				}
			});
		}*/
	});
}

function work(msg, cb) {
	console.log("PDF processing of ", msg.content.toString());
	//cb(false);
	cb(true);
}


function closeOnErr(err) {
	if (!err) return false;
	console.error("[AMQP] error", err);
	amqpConn.close();
	return true;
}

start();



var pubChannel = null;
var offlinePubQueue = [];

function startPublisher() {
	amqpConn.createConfirmChannel(function(err, ch) {
		if (closeOnErr(err)) return;
		ch.on("error", function(err) {
			console.error("[AMQP] channel error", err.message);
		});
		ch.on("close", function() {
			console.log("[AMQP] channel closed");
		});

		pubChannel = ch;
		while (true) {
			var m = offlinePubQueue.shift();
			if (!m) break;
			publish(m[0], m[1], m[2]);
		}
	});
}


function publish(exchange, routingKey, content) {
	try {
		pubChannel.publish(exchange, routingKey, content, {
				persistent: true
			},
			function(err, ok) {
				if (err) {
					console.error("[AMQP] publish", err);
					offlinePubQueue.push([exchange, routingKey, content]);
					pubChannel.connection.close();
				}
			});
	} catch (e) {
		console.error("[AMQP] publish", e.message);
		offlinePubQueue.push([exchange, routingKey, content]);
	}
}

