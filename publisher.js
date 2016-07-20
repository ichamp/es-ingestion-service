// Access the callback-based API
var amqp = require('amqplib/callback_api');
var amqpConn = null;

function start() {
	console.log('Hi sid');
	console.log(process.env.CLOUDAMQP_URL);
	process.env.CLOUDAMQP_URL = 'amqp://guest:guest@localhost:5672';
	amqp.connect(process.env.CLOUDAMQP_URL + "?heartbeat=60", function(err, conn) {
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
	//startWorker();
}


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

// A worker that acks messages only if processed successfully
function startWorker() {
	amqpConn.createChannel(function(err, ch) {
		if (closeOnErr(err)) return;
		ch.on("error", function(err) {
			console.error("[AMQP] channel error", err.message);
		});
		ch.on("close", function() {
			console.log("[AMQP] channel closed");
		});

		ch.prefetch(10);
		ch.assertQueue("jobs", {
			durable: true
		}, function(err, _ok) {
			if (closeOnErr(err)) return;
			ch.consume("jobs", processMsg, {
				noAck: false
			});
			console.log("Worker is started");
		});


		function processMsg(msg) {
			console.log('Reached processMsg');
			work(msg, function(ok) {
				try {
					if (ok)
						ch.ack(msg);
					else
						ch.reject(msg, true);
				} catch (e) {
					closeOnErr(e);
				}
			});
		}
	});
}

function work(msg, cb) {
	console.log("PDF processing of ", msg.content.toString());
	cb(true);
}



function closeOnErr(err) {
	if (!err) return false;
	console.error("[AMQP] error", err);
	amqpConn.close();
	return true;
}

var GCOUNT = 1;

setInterval(function() {
	publish("exchange_jobs", "key_jobs", new Buffer("work work work" + GCOUNT++));
}, 1000);

start();
