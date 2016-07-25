var FN_DUMP_ES = require('./es_bulk');
var CONFIG = require('./config');
var debug = require('debug')('handler');
var QUEUE = require('./queue');

function copy_array(arr){
	var ar = [];
	return JSON.parse(JSON.stringify(arr));
}

var PARALLELISM = 0;

var STARTED_G = false;
var STARTED_G_COUNTER = 0;

var handler = {

	syncAr: [],
	counter: 0,

	concurrency: 0,

	max_concurrency: CONFIG.ELASTICSEARCH.BULK_CONCURRENCY,

	channel : null,

	channelSet : false,

	processSingle: function(channel, data) {
		//console.log('entered processSingle');

		debug('entered processSingle');
		var self = this;

		if(self.channelSet === false){
			self.channelSet = true;
			self.channel = channel;
		}

		//console.log(data);
		//console.log('stringified below');
		//console.log(JSON.stringify(data));

		data.content = data.content.toString();



		self.syncAr[self.counter] = data; //.content.toString();
		self.counter++;

		if (self.counter == CONFIG.ELASTICSEARCH.BULK_SIZE) {
			self.addThreadRequest();
		}
	},

	addThreadRequest: function() {
		//console.log('entered addThreadRequest');

		debug('entered addThreadRequest');
		var self = this;

		//console.log(self.syncAr);

		debug('below is the data send for enqueuing');
		debug(JSON.stringify(self.syncAr));

		QUEUE.enqueue(JSON.stringify(self.syncAr));

		self.cleanupBatch();
		
		while ((self.concurrency < self.max_concurrency) && QUEUE.length()) {
				self.executeBulkThread();
			}
			
		//self.executeBulkThread();
		/*
		++STARTED_G_COUNTER;

		//console.log('Sid counter is ' + STARTED_G_COUNTER);

		if (!STARTED_G) {
			if (STARTED_G_COUNTER === CONFIG.RABBITMQ.PREFETCH_COUNT / CONFIG.ELASTICSEARCH.BULK_SIZE) {
				STARTED_G = true;
				while ((self.concurrency < self.max_concurrency) && QUEUE.length()) {
				self.executeBulkThread();
			}
			}
		} else {
			//if(STARTED_G_COUNTER === CONFIG.RABBITMQ.PREFETCH_COUNT/CONFIG.ELASTICSEARCH.BULK_SIZE)
			while ((self.concurrency < self.max_concurrency) && QUEUE.length()) {
				self.executeBulkThread();
			}
		}
		*/
	},

	executeBulkThread: function(channel) {
		//console.log('entered executeBulkThread');

		debug('entered executeBulkThread');
		var self = this;

		//if ((self.concurrency < self.max_concurrency) &&  QUEUE.length()){
		if(1){
			//console.log('SID queue length is => ' + QUEUE.length());
			var data = QUEUE.dequeue();

			// data = '[{"fields":{"consumerTag":"amq.ctag-vW9H9llGCoosHPdIwUvOTA","deliveryTag":89,"redelivered":true,"exchange":"ex_staging_catalog_refiner","routingKey":"refiner"},"properties":{"headers":{},"deliveryMode":2},"content":"{\"index\":{\"_index\":\"catalog\",\"_type\":\"refiner\",\"_id\":21149}}\n{\"id\":21149,\"vertical_id\":2,\"brand_id\":1705,\"brand\":\"9rasa\",\"is_live\":[0],\"variants\":[{\"id\":21149,\"attributes\":{\"color\":[\"Yellow\"],\"size\":[\"Free\"],\"material\":[\"Cotton\"],\"color_filter\":[\"Yellow\"]},\"price\":1999,\"mrp\":1999,\"brand_id\":1705,\"brand\":\"9rasa\",\"discount\":0,\"merchant_id\":20081,\"category_id\":[],\"is_diy\":false,\"authorised_merchant\":0,\"paytm_verified_merchant\":0,\"category_verified_merchant\":0,\"latlong\":[],\"fulfilled_by_paytm\":0,\"authorisation_level\":[],\"is_live\":0,\"salesforce_case_id\":null}],\"lowest_price\":1999,\"highest_price\":1999,\"categories\":[],\"authorisation_level\":[],\"fulfilled_by_paytm\":[0],\"merchant_ids\":[20081],\"primary_category_id\":[8378],\"filter_color\":[\"Yellow\"],\"filter_size\":[\"Free\"],\"filter_material\":[\"Cotton\"],\"filter_color_filter\":[\"Yellow\"],\"paytm_sku\":\"TESTING_DEV-WSKIRT031-yellow9RYLFR\",\"sku\":\"SK031-yellow\",\"merchant_id\":20081,\"merchant_name\":\"9rasa\",\"name\":\"9rasa Block Printed Brocade Border Cotton Skirt   \",\"mrp\":1999,\"price\":1999,\"url_key\":\"9rasa-block-printed-brocade-border-cotton-skirt-wskirt031-yellow9rylfr\",\"thumbnail\":\"3.jpg\",\"short_description\":null,\"description\":\"[{\\\"title\\\":\\\"Description\\\",\\\"description\\\":\\\"This ethnic and sensual skirt is a unique masterpiece. The appealing look of this long skirt is accentuated with brocade borders. It has a soft elastic belt for convenient fitting on the waist. This skirt is made of pure cotton & the hand block print enhances its beauty. You need not iron it before wearing. \\\\n\\\\n\\\",\\\"attributes\\\":{\\\" Style Tip:  \\\":\\\"\\\",\\\"Work Detail:\\\":\\\"   Hand block printed with brocade border  Style Tip: This stylish skirt looks great when paired with your favorite tops, shirts, kurtis and t-shirts.\\\",\\\"Sizing Info:\\\":\\\"Fits Waist Size 26 Inches to 42 Inches, Length-40 Inches Flair: 4.5 Meters Wash Care:  Dry Clean OnlyDisclaimer:The brocade border at edges may vary.\\\"}}]\",\"meta_title\":null,\"meta_description\":null,\"meta_keyword\":\"\",\"is_in_stock\":0,\"tag\":null,\"discount\":0,\"promo_text\":null,\"category_id\":8378,\"category_name\":\"Mix and Match\",\"catagory_full_path\":\" Women->Ethnic Wear->Mix and Match\",\"product_type\":1,\"need_shipping\":1,\"child_site_ids\":[],\"created_at\":\"2014-01-20T13:31:33.000Z\",\"updated_at\":\"2015-12-30T09:22:07.000Z\",\"priceSiteMap\":{},\"child_paytm_skus\":[\"TESTING_DEV-WSKIRT031-yellow9RYLFR\"],\"child_product_ids\":[21149],\"child_merchants\":[20081],\"cats\":[8378],\"out_of_stock_price\":1999,\"out_of_stock_mrp\":1999,\"index_date\":\"2016-07-20\",\"search_weight\":0}\n"},{"fields":{"consumerTag":"amq.ctag-vW9H9llGCoosHPdIwUvOTA","deliveryTag":90,"redelivered":true,"exchange":"ex_staging_catalog_refiner","routingKey":"refiner"},"properties":{"headers":{},"deliveryMode":2},"content":"{\"index\":{\"_index\":\"catalog\",\"_type\":\"refiner\",\"_id\":21155}}\n{\"id\":21155,\"vertical_id\":2,\"brand_id\":1705,\"brand\":\"9rasa\",\"is_live\":[0],\"variants\":[{\"id\":21155,\"attributes\":{\"color\":[\"Beige\"],\"size\":[\"Free\"],\"material\":[\"Georgette\"],\"color_filter\":[\"Beige\"]},\"price\":2199,\"mrp\":2199,\"brand_id\":1705,\"brand\":\"9rasa\",\"discount\":0,\"merchant_id\":20081,\"category_id\":[],\"is_diy\":false,\"authorised_merchant\":0,\"paytm_verified_merchant\":0,\"category_verified_merchant\":0,\"latlong\":[],\"fulfilled_by_paytm\":0,\"authorisation_level\":[],\"is_live\":0,\"salesforce_case_id\":null}],\"lowest_price\":2199,\"highest_price\":2199,\"categories\":[],\"authorisation_level\":[],\"fulfilled_by_paytm\":[0],\"merchant_ids\":[20081],\"primary_category_id\":[5237],\"filter_color\":[\"Beige\"],\"filter_size\":[\"Free\"],\"filter_material\":[\"Georgette\"],\"filter_color_filter\":[\"Beige\"],\"paytm_sku\":\"TESTING_DEV-WSKIRT-Sept-21029RBGFR\",\"sku\":\"KN-Sept-2102\",\"merchant_id\":20081,\"merchant_name\":\"9rasa\",\"name\":\"9rasa Faux Georgette Gota Border Skirt   \",\"mrp\":2199,\"price\":2199,\"url_key\":\"9rasa-faux-georgette-gota-border-skirt-wskirt-sept-21029rbgfr\",\"thumbnail\":\"3.jpg\",\"short_description\":null,\"description\":\"[{\\\"title\\\":\\\"Description\\\",\\\"description\\\":\\\"This ethnic and sensual skirt is a unique masterpiece. It has a soft elastic belt for convenient fitting on the waist. You need not iron it before wearing.\\\",\\\"attributes\\\":{\\\" Style Tip:  \\\":\\\"This stylish skirt looks great when paired with your favorite tops, shirts, kurtis and t-shirts.\\\",\\\"Work Detail:\\\":\\\"\\\",\\\"Sizing Info:\\\":\\\"Fits Waist Size 26 Inches to 42 InchesLength- 40 Inches Wash Care:  Hand Wash Cold\\\"}}]\",\"meta_title\":null,\"meta_description\":null,\"meta_keyword\":null,\"is_in_stock\":0,\"tag\":null,\"discount\":0,\"promo_text\":null,\"category_id\":5237,\"category_name\":\"Skirts\",\"catagory_full_path\":\" Women->Western Wear->Skirts\",\"product_type\":1,\"need_shipping\":1,\"child_site_ids\":[],\"created_at\":\"2014-01-20T13:31:33.000Z\",\"updated_at\":\"2015-12-30T09:22:07.000Z\",\"priceSiteMap\":{},\"child_paytm_skus\":[\"TESTING_DEV-WSKIRT-Sept-21029RBGFR\"],\"child_product_ids\":[21155],\"child_merchants\":[20081],\"cats\":[5237],\"out_of_stock_price\":2199,\"out_of_stock_mrp\":2199,\"index_date\":\"2016-07-20\",\"search_weight\":0}\n"}]';

			//console.log('Going to parse data dequeued from queue');
			try {
				data = JSON.parse(data);
			} catch (err) {
				console.log('Error in parsing json from string');
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

				//console.log('Sid bulk ar is below');
				//console.log(bulkAr);

				self.concurrency++;

				console.log('PARALLELISM = ' + self.concurrency);
				console.log('Queue length = ' + QUEUE.length());

				FN_DUMP_ES(data, bulkAr, function(err, res) {
					if (err) {
						//console.log('NACKING data of length' = data.length);
						for (var i = 0; i < data.length; i++) {
							self.channel.reject(data[i], true);
						}

						self.concurrency--;
						console.log('PARALLELISM DECREASED = ' + self.concurrency);
						//self.executeBulkThread();

					} else {
						//console.log('ACKING data of length = ' + data.length);
						for (var i = 0; i < data.length; i++) {
							//console.log(localAr[i].content.toString());
							//console.log(JSON.stringify(data[i]));
							//console.log(data[i]);
							self.channel.ack(data[i]);
						}

						self.concurrency--;
						console.log('PARALLELISM DECREASED = ' + self.concurrency);
						//self.executeBulkThread();
					}
				});
			}
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
		//console.log('entered cleanupBatch');

		debug('entered cleanupBatch');
		var self = this;

		self.syncAr.splice(0,self.syncAr.length);
		self.counter=0;
	}
};

module.exports = handler;
