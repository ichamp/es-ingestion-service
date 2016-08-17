'use strict';

var debug = require('debug')('es');
var elasticsearch = require('elasticsearch');
var async = require('async');

var es_ops = {

  /*****
  Use below connect function to create connection with ES cluster.
  *****/
  connect: function (host, log, requestTimeout) {
    var config = {
      host: host
    };

    if (log) {
      config.log = 'trace';
    }

    if(requestTimeout) {
      config.requestTimeout = requestTimeout;
    }

    return new elasticsearch.Client(config);
  },

/*
  Below is one of the config from config array
  var config = {
    host: 'localhost:9200',
    log: false,
    requestTimeout: 120000,//120ms
    index: 'catalog',
    type: 'refiner',
    client: ''
  }
*/
  connectMultiIndex: function(configAr){
    var clientAr = [];
    var runner = 0;
    configAr.forEach(function(conf){
      var config = {};
      if(conf.host)
        config.host = conf.host;

      if(conf.log)
        config.log = true;
      else
        config.log = false;

      if(conf.requestTimeout)
        config.requestTimeout = conf.requestTimeout;

      conf.client = new elasticsearch.Client(config);

      //console.log('RUNNER => ' + ++runner);
      //clientAr.push(new elasticsearch.Client(config));
    });
  },

  /*****
  Use below to create a new index. It is assumed that you don't know the id of this document,
  and hence finally several copies of similar data can be there with different 'id'
  *****/
  insert: function (client, esIndex, dumpObj, cb) {
    client.create({
      index: esIndex.index,
      type: esIndex.type,
      body: dumpObj
    }).then(function (resp) {
      return cb(null, resp._id);
    }, function (err) {
      return cb(err);
    });
  },

  /*****
  Use below to create or update a document, it will inform the version of the document bumped up.
  Important is to be able to define a unique primary 'id' which is used to create or update the
  document contents
  *****/
  insertUpdateExisting: function (client, esIndex, id, dumpObj, cb) {
    client.index({
      index: esIndex.index,
      type: esIndex.type,
      id: id,
      body: dumpObj
    }).then(function (resp) {
      if (resp._id && resp._version) {
        cb(null, resp._id, resp._version);
      } else {
        cb(null, null, null);
      }
    }, function (err) {
      cb(err);
    });
  },

  count: function (client, esIndex, searchObj, cb) {
    client.count({
      index: esIndex.index,
      type: esIndex.type,
      //body: searchObj
    }).then(function (resp) {
      if (resp) {
        cb(null, resp.count);
      } else {
        cb(new Error('Count not exists. Error with count query'));
      }
    }, function (err) {
      cb(err);
    });
  },

  //dumpESBulk: function(client, bulkAr, cb) {
  dumpESBulk: function(client, obj, bulkAr, cb) {
    client.bulk({
      body: bulkAr
    }).then(function(resp) {
        cb(null);
      },
      function(err) {
        cb(err);
      });
  },

  
  dumpESBulkMultiIndex: function(configAr, obj, bulkAr, cb) {
    
    var logger = 0;
    //console.log('Hi SID reached the point of printing dumpESBulkMultiIndex');
    //console.log(configAr);
    //console.log(JSON.stringify(configAr));

    // Object.keys(configAr).forEach(function(prop) {
    //   if (prop !== 'client') {
    //     console.log(prop);
    //     console.log(configAr[prop]);
    //   }
    // });

    var NEW_BULK = [];

    // console.log('SHURU ORIGINAL');
    // console.log(bulkAr);
    // console.log('KHATAM ORIGINAL');


    async.eachSeries(configAr, runQuery, respond);

    
    //console.log(NEW_BULK);

    function runQuery(config, lcb){
      //config.client.bulk

      //console.log('SID CALLING async eachSeries');
      //console.log('Logger => ' + ++logger);
      //console.log(config);
      
      bulkArParse(config);

      // console.log('SHURU TRANSFORMED');
      // console.log(NEW_BULK);
      // console.log('KHATAM TRANSFORMED');

      es_ops.dumpESBulk(config.client, obj, NEW_BULK, function(err, res){
        if(err && !res){
          cb(err);
        } else {
        }
        lcb();
      });
    }

    function respond(err) {
      if (err) {
        return cb(err);
      } else {
        return cb(null);
      }
    }


    function bulkArParse(conf){
      //Parse the entire bulkAr object as per needed configs of client, index and type

      //Rows are delimited by \n
      //console.log(logger);

      //console.log('Upar');
      //console.log(NEW_BULK);
      //console.log('Neeche');

      NEW_BULK.splice(0, NEW_BULK.length);

      var replacer = {
        "index": {
          "_index": "catalog",
          "_type": "refiner",
          "_id": 111
        }
      };

      bulkAr.forEach(function(bulk) {
        var newstr = bulk.split('\n');
        //console.log('Length after split is => ' + newstr.length);
        //console.log(newstr);

        //newstr[0] = {"index":{"_index":"catalog","_type":"refiner","_id":262490};
        var json = JSON.parse(newstr[0]);

        Object.keys(json).forEach(function(prop){
          json[prop]._index = conf.index;
          json[prop]._type = conf.type;
        });

//console.log('Sid printing json below');
//console.log(json);

//console.log('Index => ' + conf.index +  '   Type => ' + conf.type);
        newstr[0] = JSON.stringify(json);

        bulk = newstr.join('\n');
        //console.log(bulk);

        NEW_BULK.push(bulk);
      });
      
      //bulkAr = newbulk;
      //console.log('Printing the contents of bulkAr here down =>');
      //console.log(newbulk);
    }

  },

  searchSingle: function (client, esIndex, searchObj, cb) {
    client.search({
      index: esIndex.index,
      type: esIndex.type,
      body: searchObj
    }).then(function (resp) {
      var result = [];
      if (resp && resp.hits && resp.hits.hits && resp.hits.hits.length) {
        result = resp.hits.hits.map(function (e) {
          return e._source;
        });
      }
      cb(null, result);
    }, function (err) {
      cb(err);
    });
  },

  getQuery: function (options) {
    var body = {
      query: {
        bool: {
          must: []
        }
      }
    };

    if (options) {
      if (options.size) {
        body.size = options.size;
      }

      if (options.match) {
        var matchFields = options.match;
        matchFields.forEach(function (prop) {
          body.query.bool.must.push(getMatchObj(prop));
        });
      }

      if (options.range) {
        body.query.bool.must.push(getRangeObj(options.range));
      }

      if (options.sort) {
        body.sort = options.sort;
      }

      return body;
    }
  }
};

function getMatchObj(matcher) {
  var obj = {
    match: {}
  };
  obj.match = matcher;
  return obj;
}

function getRangeObj(ranger) {
  var obj = {
    range: {}
  };
  obj.range = ranger;
  return obj;
}

module.exports = es_ops;

// -- Test Code ---------------------------------------------------------
if (require.main === module) {
  (function () {

    var userOpt = process.argv[2];

    var options = {
      size: '10',
      match: [{
        field1: 'value1'
      }, {
        field2: 'value2'
      }],
      range: {
        field1: {
          'gte': '2015-12-11T13:26:45.211Z',
          'lte': '2015-12-16T13:26:45.211Z'
        }
      },
      sort: [{
        'updatedAt': {
          'order': 'desc'
        }
      }],
      aggs: [{
        aggName: 'aggName',
        field: 'fieldName',
        size: 15
      }]
    };

    if (userOpt)
      options = userOpt;

    console.log(JSON.stringify(es_ops.getQuery(options)));
  })();
}
