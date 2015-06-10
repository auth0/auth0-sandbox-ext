var mongo;


var webtask = function (context, req, res) {
    //if (!Bluebird) Bluebird = require('bluebird');
    var _ = require('lodash');
    var Bluebird = require('bluebird');
    
    var action = validate_method({
        put_job: 'PUT',
        get_job: 'GET',
        delete_job: 'DELETE',
        list_jobs: 'GET',
        reserve_jobs: 'POST',
        renew_reservation: 'PUT',
        release_reservation: 'DELETE',
    });
    
    if (action === 'put_job') {
        // Create or update a scheduled webtask
        
        if (!validate_params(['MONGO_COLLECTION', 'container', 'name', 'schedule', 'token']))
            return;
            
        var now = new Date();
        
        var item = {
            state: 'active',
            schedule: context.data.schedule,
            token: context.data.token,
            container: context.data.container,
            name: context.data.name,
            last_run_at: null,
            next_available_at: now,
            last_result: null,
            run_count: 0,
            error_count: 0,
        };
        
        return withMongoCollection(context.data.MONGO_COLLECTION)
            .then(function (coll) {
                return coll.findOneAndUpdateAsync({
                    container: item.container,
                    name: item.name,
                }, item, {
                    returnOriginal: false,
                    upsert: true,
                });
            })
            .get('value')
            .then(function (data) {
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.end(JSON.stringify(data));
            })
            .catch(function (err) {
                error(err.statusCode || 500, err);
            });
    } else if (action === 'reserve_jobs') {
        // Reserve a set of jobs for execution
        
        if (!validate_params(['count', 'ttl']))
            return;
        
        var now = new Date();
        var next_available_at = new Date(now.valueOf() + (parseInt(context.data.ttl, 10) * 1000))
        
        return withMongoCollection(context.data.MONGO_COLLECTION)
            .then(function (coll) {
                return Bluebird.resolve(_.range(context.data.count))
                    // TODO (ggoodman): Refactor this to use reduce to avoid calling N queries if reservation fails before N
                    .map(function (n) {
                        console.log("Attempting reservation", n);
                        var filter = {
                            next_available_at: {
                                $lte: now
                            }
                        };
                        var update = {
                            $set: {
                                next_available_at: next_available_at
                            }
                        };
                        
                        return coll.findOneAndUpdateAsync(filter, update, {
                            returnOriginal: false, // Return modified
                        })
                            .get('value'); // Only pull out the value
                        
                    }, {concurrency: 1}) // Serial mapping
                    .filter(Boolean);
                
            })
            // .get('value')
            .then(function (data) {
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.end(JSON.stringify(data));
            })
            .catch(function (err) {
                error(err.statusCode || 500, err);
            });
    }
    
    // Helper methods
    
    function withMongoDb () {
        if (mongo) return Bluebird.resolve(mongo);
        
        var MongoClient = require('mongodb').MongoClient;
        var connect = Bluebird.promisify(MongoClient.connect, MongoClient);
        
        return connect(context.data.MONGO_URL)
            .then(function (db) {
                mongo = db;
                
                return mongo;
            });
    }
    
    function withMongoCollection (collName) {
        return withMongoDb()
            .call('collection', collName) // Get a Collection handle
            .then(Bluebird.promisifyAll); // Promisify all methods of Connection
    }

    function validate_method (valid_actions) {
        var action = context.data.action;
        var expected_method = valid_actions[action];
        
        if (!action) return error(400, 'Missing action.');
        if (!expected_method) return error(400, 'Invalid action.');
        if (expected_method !== req.method) return error(405, 'Invalid method.');
        
        return action;
    }

    function validate_params(required_params) {
        for (var i in required_params) {
            if (typeof context.data[required_params[i]] !== 'string') {
                return error(400, 'Missing ' + required_params[i] + '.');
            }
        }
        return true;
    }

    function error(code, err) {
        console.log(code + (err ? (': ' + err) : ''));
        res.writeHead(code);
        res.end(err);
        return false;
    }
};

return webtask;