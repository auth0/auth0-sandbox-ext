var _ = require('lodash');
var Bluebird = require('bluebird');

var mongo;

var valid_actions = {
    put_job: 'PUT',
    get_job: 'GET',
    destroy_job: 'DELETE',
    list_jobs: 'GET',
    job_history: 'GET',
    reserve_jobs: 'POST',
    renew_reservation: 'PUT',
    release_reservation: 'DELETE',
};


var webtask = function (context, req, res) {
    var action = validate_method(valid_actions);
    
    if (!validate_params(['MONGO_COLLECTION']))
        return;
    
    if (action === 'put_job') {
        // Create or update a scheduled webtask
        
        if (!validate_params(['container', 'name', 'schedule', 'token']))
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
                            },
                            state: 'active',
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
            .then(function (data) {
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.end(JSON.stringify(data));
            })
            .catch(function (err) {
                error(err.statusCode || 500, err);
            });
    } else if (action === 'list_jobs') {
        // List all scheduled webtasks or only those in a container passed via query
        
        return withMongoCollection(context.data.MONGO_COLLECTION)
            .then(function (coll) {
                var query = {};
                
                // Both admin and user will hit this endpoint. When admin, container can be unset
                if (context.data.container) query.container = context.data.container;
                
                // TODO (ggoodman): Pagination logic
                return coll.findAsync(query)
                    .then(function (cursor) {
                        return Bluebird.promisify(cursor.toArray, cursor)();
                    });
            })
            .then(function (data) {
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.end(JSON.stringify(data));
            })
            .catch(function (err) {
                error(err.statusCode || 500, err);
            });
    
    } else if (action === 'get_job') {
        // Get a single scheduled webtask
        
        if (!validate_params(['container', 'name']))
            return;
            
        return withMongoCollection(context.data.MONGO_COLLECTION)
            .then(function (coll) {
                var query = {
                    container: context.data.container,
                    name: context.data.name,
                };
                
                return coll.findOneAsync(query, {});
            })
            .then(function (data) {
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.end(JSON.stringify(data));
            })
            .catch(function (err) {
                error(err.statusCode || 500, err);
            });
    
    } else if (action === 'destroy_job') {
        // Get a single scheduled webtask
        
        if (!validate_params(['container', 'name']))
            return;
            
        return withMongoCollection(context.data.MONGO_COLLECTION)
            .then(function (coll) {
                var filter = {
                    container: context.data.container,
                    name: context.data.name,
                };
                
                return coll.deleteOne(filter, {w: 1});
            })
            .then(function (data) {
                res.writeHead(204);
                res.end();
            })
            .catch(function (err) {
                error(err.statusCode || 500, err);
            });
    
    } else {
        res.writeHead(404);
        res.end('Method not found');
    }
    
    // Helper methods
    
    function withMongoDb () {
        if (mongo) return mongo;
        
        var MongoClient = require('mongodb').MongoClient;
        var connect = Bluebird.promisify(MongoClient.connect, MongoClient);
        
        return connect(context.data.MONGO_URL)
            .then(function (db) {
                // Store the settled promise resolving to the db object
                mongo = Bluebird.resolve(db);
                
                return db;
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