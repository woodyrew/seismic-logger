var xray = require('x-ray');
var geolib = require('geolib');
var moment = require('moment');
var crypto = require("crypto");
var request = require('request');
var config = require('./config.json');


/**
 * Creates a new database if required.
 * 
 * @param  {string} db_name
 * @param  {Function} callback   Deal with the response
 * 
 * @return {void}
 */
var create_new_database = function create_name_database (db_name, callback) {
	request.put(config.couchDB_location + db_name, function (error, response, body) {
		callback(error, body);
	});
};

/**
 * Checks the database is available and present
 * 
 * @param  {Function} callback    What to do upon success - fail stops
 * 
 * @return {void}
 */
var ensure_db_present = function ensure_db_present (callback) {
	request.get(config.couchDB_location + '_all_dbs', function (error, response, body) {
		var content = [];
		if (!error && response.statusCode == 200) {
			all_dbs = JSON.parse(body);
			// console.log(all_dbs);

			if (-1 === all_dbs.indexOf(config.db_name)) {
				create_new_database(config.db_name, function (error, body) {
					if (error) {
						console.log({error: error, body: body});
						return false;
					}
				});
			}
			callback();
		}
		else {
			console.log({"error": error, "status": response.statusCode, "message": body});
		}
	});
};

/**
 * Checks a record exists
 * 
 * @param  {string} id           md5 hash of data stored
 * @param  {Function} callback
 * @return {void}
 */
var check_record_exists = function check_record_exists (id, callback) {
	var url = config.couchDB_location + config.db_name + '/' + id;
	request.get(url, function (error, response, body) {
		if (!error && response.statusCode == 200) {
			callback(error, true);
		}
		else if(!error && response.statusCode == 404){
			callback(error, false);
		}
		else {
			callback(error, null);
		}
	});
};

/**
 * Only adds a record if the it doesn't already exist
 * 
 * @param {string}    md5 hash
 * @param {document}  document to store
 */
var add_if_not_exist = function add_if_not_exist (id, doc) {
	check_record_exists(id, function (error, exists) {
		var url = config.couchDB_location + config.db_name + '/' + id;
		var options = {
			"url": url,
			"json": true,
			"body": doc
		};
		if (!error && !exists) {
			request.put(options, function (error, response, body) {
				if (error) {
					console.log({"error": error, "message": body});
				}
				else {
					console.log({"status": response.statusCode, "message": body});
				}
			});
		}
		else {
			console.log('already exists');
		}
	});
};

/**
 * Gets and processes the seismic data from the source website
 * 
 * @return {void}
 */
var process_seismic_data = function get_seismic_data () {
	xray('http://www.sismologia.cl/links/ultimos_sismos.html')
	.select([{
		$root: 'table tr',
		columns: ['td']
	}])
	.run(function (err, seismic) {
		seismic.forEach(function(element, index){
			if (element.columns && element.columns.length) {
				var local_moment;
				var converted = {
					raw: {
						utc_time: element.columns[1],
						local_time: element.columns[0],
						magnitude: element.columns[5]
					},
					latitude: element.columns[2],
					longitude: element.columns[3],
					depth: element.columns[4],
					magnitude: element.columns[5].split(' ')[0]
				};

				converted.distance = geolib.getDistance(config.my_location, {latitude: converted.latitude, longitude: converted.longitude}, config.distance_accuracy);
				converted.distance_km = converted.distance / 1000;

				if (converted.distance_km < config.store_within_km) {
					converted.utc_time = moment(converted.raw.utc_time, "YYYY/MM/DD HH:mm:ss").format("YYYY-MM-DD HH:mm:ss");
					local_moment = moment(converted.raw.local_time, "YYYY/MM/DD HH:mm:ss");
					converted.local_time = local_moment.format("YYYY-MM-DD HH:mm:ss");
					converted.local_epoch = local_moment.unix();
				
					md5 = crypto.createHash("md5")
					.update(element.columns.join(' ') + config.hash_salt)
					.digest("hex");

					add_if_not_exist(md5, converted);
				}
			}
		});
	});

};

ensure_db_present(process_seismic_data);
