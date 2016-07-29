// Access the callback-based API
var amqp = require('amqplib/callback_api');

var CONFIG = require('./config');
var HANDLER = require('./handler');

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
	//startPublisher();
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
		ch.assertQueue("CONFIG.RABBITMQ.QUEUE_NAME", {
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
			//console.log('reached processMsg in subscriber.js');
			HANDLER.processSingle(ch, msg);
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

