var AMQPStats = require('amqp-stats');

// var stats = new AMQPStats({
//   username: "AMQP_USERNAME", // default: guest
//   password: "AMQP_PASSWORD", // default: guest
//   hostname: "AMQP_HOSTNAME",  // default: localhost:55672
//   protocol: "HTTP_OR_HTTPS"  // default: http
// });

//'amqp://guest:guest@localhost:5672?heartbeat=60'

//var stats = new AMQPStats();


var stats = new AMQPStats({
  username: "guest", // default: guest
  password: "guest", // default: guest
  hostname: "localhost:5672",  // default: localhost:55672
  protocol: "HTTP"  // default: http
});


stats.overview(function(err, res, data){
  if (err) { 
  	console.log(err); 
  	throw err; 
  }
  console.log('data: ', data);
});