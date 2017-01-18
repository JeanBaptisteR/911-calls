var elasticsearch = require('elasticsearch');
var csv = require('csv-parser');
var fs = require('fs');

var esClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
});

//Create index and mapping
esClient.indices.create({
	index : 'calls'
},function(err,resp){
	if(!!err){
		console.log(err);
		return;
	}
	console.log('Index created');
	var body = {
		call:{
			properties:{
				location : { "type" : "geo_point" },
				cat : { "type" : "string", fielddata : true },
				desc : {"type" : "string", fielddata : true},
				date : {"type" : "date"},
				city : {"type" : "keyword"}
			}
		}
	};
	esClient.indices.putMapping({index:"calls", type:"call", body:body},function(err,resp){
		console.log('Mapping created');
		if(!!err){
			console.log(err);
			return;
		}
		//Import
		var calls = [];
		fs.createReadStream('../911.csv')
			.pipe(csv())
			.on('data', data => {
				var call = {
					location : {
						lat : parseFloat(data.lat),
						lon : parseFloat(data.lng)
					},	
					cat : data.title.split(':')[0],
					desc : data.title.split(':')[1].trim(),
					date : new Date(Date.parse(data.timeStamp+' GMT')),
					city : data.twp
				};
				calls.push({ index: { _index: 'calls', _type: 'call'}});
				calls.push(call);
			})
			.on('end', () => {
				esClient.bulk({
					body: calls
				},function(err,resp){
					console.log('Data indexed');
					if(!!err){
						console.log('Error : '+err);				
					}
				});
			});
	});
});



