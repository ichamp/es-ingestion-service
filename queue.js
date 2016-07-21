//thread_abstract
'use strict'

var debug = require('debug')('queue');

var queue = {

	ar:[],

	enqueue: function(data){
		debug('ENTER enqueue');
		var self = this;

		self.ar.push(data);

		debug('Added data to queue is => ' );
		debug(data);
	},

	dequeue: function(){
		debug('ENTER dequeue');
		var self = this;

		var val = self.ar.shift();

		debug('Dequeued data from queue is => ');
		debug(val);

		return val;
	},

	dequeueMiddle: function(id){
		debug('ENTER dequeueMiddle');
		var self = this;

		return self.ar.splice(id,1);

	},

	length: function(id){
		debug('ENTER length');
		var self = this;
		//console.log('Hi sid length is ' + self.ar.length);
		return self.ar.length;
	},

	print: function(){
		debug('ENTER print');
		var self = this;

		console.log('Queue contents are => ');

		for(var i=0; i<self.ar.length; i++){
			console.log(self.ar[i]);
		}
	}
};

module.exports = queue;
