var elasticsearch = require('elasticsearch');
var csv = require('csv-parser');
var fs = require('fs');

var esClient = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'error'
});

var calls = [];
fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {
		var call = {
			location : {
				lat : parseFloat(data.lat),
				lng : parseFloat(data.lng)
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
			console.log('Error '+err);
		});
    });
