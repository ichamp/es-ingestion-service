var FN_DUMP_ES = require('./es_bulk');
var CONFIG = require('./config');

function copy_array(arr){
	var ar = [];
	return JSON.parse(JSON.stringify(arr));
}

GNUM = 1;

var PARALLELISM = 0;
var handler = {

	syncAr: [],
	counter: 0,
	
	processSingle: function(channel, msg){
		
		var self = this;

		//console.log('processSingle for ' + self.counter);

		var json = msg.content.toString();
		//console.log(json);
		//console.log(JSON.stringify(json));

		self.syncAr[self.counter]= msg;//.content.toString();
		self.counter++;

		//console.log('REACHED CHECK');
		if(self.counter == CONFIG.ELASTICSEARCH.BULK_SIZE){
			//console.log('Calling pushBatch for ES');
			self.pushBatch(channel, msg);
		}
	},

	pushBatch: function(channel, msg){
		var self = this;

		//console.log('REACHED pushBatch for counter ' + self.counter);
		
		//console.log('length of array object is');
		//console.log(self.syncAr.length);

		var localAr = [];
		var localMsgAr = [];
		for (var i=0; i < self.syncAr.length; i++){
			localAr.push(self.syncAr[i]);
			localMsgAr.push(self.syncAr[i].content.toString());
			//console.log(localMsgAr)
		}

		//console.log('making bulk call to ES');

		/*
		for (var i=0; i < localAr.length; i++){
					//console.log(localAr[i].content.toString());
					//channel.ack(localAr[i]);
					if(GNUM % 2 == 0){
						channel.ack(localAr[i]);
						console.log('ACKING '+ localMsgAr[i]);
						GNUM++;
					}
					else
						GNUM++;
				}
		*/
		///*	
		PARALLELISM++;
		//console.log('THREADS => ' + PARALLELISM);	
		FN_DUMP_ES(localMsgAr, function(err, res){
			
			//console.log('RECEIVED CALLBACK FROM ES dump');
			if(err){
				//console.log('NACK msg from handler ' + localMsgAr[0]);
				//channel()
				//console.log('REDUCE => ' + --PARALLELISM);
				//console.log('Messages in new queue till now ' + self.counter);
				for (var i=0; i < localMsgAr.length; i++){
					channel.reject(localAr[i],true)
				}
				//ack rabbitmq messages
			}else{
				
				//console.log('ACK message from handler ' + localMsgAr[0]);
				//channel.ack(localAr[i]);
				//console.log('REDUCE => ' + --PARALLELISM);
				//console.log('Messages in new queue till now ' + self.counter);
				for (var i=0; i < localAr.length; i++){
					//console.log(localAr[i].content.toString());
					channel.ack(localAr[i]);
				}
				//nack rabbitMq messages
			}
		});
		//*/

		self.cleanupBatch();
	},

	cleanupBatch: function(){
		//console.log('Calling for cleanupBatch');
		var self = this;

		self.syncAr.splice(0,self.syncAr.length);
		self.counter=0;
	}
};

module.exports = handler;


// var a = [1,2,3];

// var b = copy_array(a);

// b.push (4);

// console.log(a);
// console.log(b);

