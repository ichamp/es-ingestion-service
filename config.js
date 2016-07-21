//config.js

/*jshint multistr: true ,node: true*/
"use strict";

var config = {

	ENVIRONMENT 					: process.env.NODE_ENV || 'development',

	/* This is common config that will be loaded first
		After this the enviroment configs will be loaded and will overwrite these settings

		PUT settings here which are common for both envs
	*/
	COMMON 							: {

		WEBSERVER 					: {
			PORT 					: 8889,
		},

		// Queue details
		QUEUE 						: {
			KEY 					: 'elasticsearch',
			EXCHANGE_OPTS 			: {
				NAME                : 'ex_elasticsearch_bulk',
				TYPE                : 'topic',
		        OPTIONS             : {
		            durable         : true,
		            internal        : false,
		            autoDelete      : false,
		        },
			},
		},

	},

	/*
		Environment specific settings
		These will be loaded after common , and will overwrite common settings 

		PUT settings according to the env block
	*/

	'development' : {
		ELASTICSEARCH :{
			URL: 'http://localhost:9200',
			INDEX: 'catalog',
			TYPE: 'refiner',
			BULK_SIZE: 200,
			NUM_SHARDS: 10,
			BULK_CONCURRENCY: 20
		},

		RABBITMQ : {
        	CONNECT_STRING          : 'amqp://guest:guest@localhost:5672?heartbeat=60',
        	RETRY_INTERVAL          : 5000,     // milliseconds
    		PREFETCH_COUNT			: 5000,
    		QUEUE_NAME				: 'dump7',
    		EXCHANGE_NAME			: '',
    		EXCHANGE_KEY			: ''
    	}
	}

};

//module.exports = config;


var load = function(){
	var
		env 			= config.ENVIRONMENT,
		loadedConfig 	= config.COMMON;

	console.log('printing from config file directly');
	console.log(JSON.stringify(config));

	//copy superficially , and not deep copy
	Object.keys(config[env]).forEach(function(key) {
		loadedConfig[key] = config[env][key];
	});

	return loadedConfig;
};

module.exports = load();

