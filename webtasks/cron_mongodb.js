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
    update_job: 'PUT',
};


return function (context, req, res) {
    var action = validate_method(valid_actions);
    var now = new Date();
    
    if (!validate_params(['JOB_COLLECTION', 'LOG_COLLECTION', 'cluster_url']))
        return;
    
    if (action === 'put_job') {
        // Create or update a scheduled webtask
        
        if (!validate_params(['container', 'name']))
            return;
        
        if (!validate_body(['schedule', 'token']))
            return;
            
        var update = {
            $set: {
                state: 'active',
                schedule: context.body.schedule,
                token: context.body.token,
                cluster_url: context.data.cluster_url,
                container: context.data.container,
                name: context.data.name,
                last_scheduled_at: now,
                next_available_at: now,
            },
            $setOnInsert: {
                last_result: null,
                run_count: 0,
                error_count: 0,
            }
        };
        
        return withMongoCollection(context.data.JOB_COLLECTION)
            .then(function (coll) {
                return coll.findOneAndUpdateAsync({
                    container: context.data.container,
                    name: context.data.name,
                }, update, {
                    returnOriginal: false,
                    upsert: true,
                });
            })
            .get('value')
            .then(stripMongoId)
            .then(function (data) {
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.end(JSON.stringify(data));
            })
            .catch(function (err) {
                respondWithError(err.statusCode || 500, err);
            });
    } else if (action === 'reserve_jobs') {
        // Reserve a set of jobs for execution
        
        if (!validate_params(['count', 'ttl']))
            return;
        
        var next_available_at = new Date(now.valueOf() + (parseInt(context.data.ttl, 10) * 1000));
        
        return withMongoCollection(context.data.JOB_COLLECTION)
            .then(function (coll) {
                return Bluebird.resolve(_.range(context.data.count))
                    // TODO (ggoodman): Refactor this to use reduce to avoid calling N queries if reservation fails before N
                    .map(function (n) {
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
                        
                    })
                    // Don't prevent one failure from blocking the entire
                    // reservation (that would mean extra waiting on those that
                    // didn't fail)
                    .settle()
                    // Log errors and continue with safe fallback
                    .map(function (result) {
                        if (result.isRejected()) {
                            return null;
                        } else {
                            return result.value();
                        }
                    })
                    // Elminate error queries and queries that did not match
                    // (this is the case when there are fewer than N jobs
                    // available for running now)
                    .filter(Boolean);
                
            })
            .map(stripMongoId)
            .then(function (data) {
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.end(JSON.stringify(data));
            })
            .catch(function (err) {
                respondWithError(err.statusCode || 500, err);
            });
    } else if (action === 'list_jobs') {
        // List all scheduled webtasks or only those in a container passed via query
        
        return withMongoCollection(context.data.JOB_COLLECTION)
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
            .map(stripMongoId)
            .then(function (data) {
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.end(JSON.stringify(data));
            })
            .catch(function (err) {
                respondWithError(err.statusCode || 500, err);
            });
    
    } else if (action === 'get_job') {
        // Get a single scheduled webtask
        
        if (!validate_params(['container', 'name']))
            return;
            
        return withMongoCollection(context.data.JOB_COLLECTION)
            .then(function (coll) {
                var query = {
                    container: context.data.container,
                    name: context.data.name,
                };
                
                return coll.findOneAsync(query, {});
            })
            .then(stripMongoId)
            .then(function (data) {
                if (!data) {
                    res.writeHead(404);
                    return res.end("Not found");
                }
                
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.end(JSON.stringify(data));
            })
            .catch(function (err) {
                respondWithError(err.statusCode || 500, err);
            });
    
    } else if (action === 'destroy_job') {
        // Get a single scheduled webtask
        
        if (!validate_params(['container', 'name']))
            return;
            
        var deleteJob = withMongoCollection(context.data.JOB_COLLECTION)
            .then(function (coll) {
                var filter = {
                    container: context.data.container,
                    name: context.data.name,
                };
                
                return coll.deleteOne(filter, {w: 1});
            });
            
        var deleteLogs = withMongoCollection(context.data.LOG_COLLECTION)
            .then(function (coll) {
                var filter = {
                    container: context.data.container,
                    name: context.data.name,
                };
                
                return coll.deleteMany(filter, {w: 1});
            });
        
        Bluebird.all([deleteJob, deleteLogs])
            .then(function (data) {
                res.writeHead(204);
                res.end();
            })
            .catch(function (err) {
                respondWithError(err.statusCode || 500, err);
            });
    
    } else if (action === 'update_job') {
        // Update a single scheduled webtask based on criteria
        
        if (!validate_params(['container', 'name']))
            return;
        
        if (!validate_body(['criteria', 'updates']))
            return;
            
        if (!_.isObject(context.body.criteria)) return respondWithError(400, 'Expecting criteria to be an object');
        if (!_.isObject(context.body.updates)) return respondWithError(400, 'Expecting updates to be an object');
        
        var criteria = context.body.criteria;
        var updates = context.body.updates;
        var result = updates.last_result;
        
        var makeMongoDates = function (obj, fields) {
            _.forEach(fields, function (field) {
                if (obj[field]) obj[field] = new Date(obj[field]);
            });
        };
        
        makeMongoDates(criteria, ['last_scheduled_at', 'next_available_at']);
        makeMongoDates(updates, ['last_scheduled_at', 'next_available_at']);
            
        // This update includes a last_result, so we will add it to our log
        if (result) {
            // Fire and forget this as considered non-critical
            withMongoCollection(context.data.LOG_COLLECTION)
                .then(function (coll) {
                    var logEntry = _.extend(result, {
                        container: context.data.container,
                        name: context.data.name,
                        created_at: new Date(),
                    });
                    
                    return coll.insertOneAsync(logEntry, {w: 1});
                });
        }
            
        return withMongoCollection(context.data.JOB_COLLECTION)
            .then(function (coll) {
                var filter = _.defaults({
                    container: context.data.container,
                    name: context.data.name,
                }, context.body.critera);
                
                var update = {
                    $set: context.body.updates,
                };
                
                // Track stats based on result
                if (result) {
                    if (result.type == 'success') update.$inc = { run_count: 1};
                    if (result.type == 'error') update.$inc = { error_count: 1};
                }
                
                return coll.findOneAndUpdateAsync(filter, update)
                    .then(stripMongoId);
            })
            .then(function (data) {
                // There may be no jobs that match criteria, resulting in 
                // data being null
                
                if (!data) throw {
                    statusCode: 404,
                    message: 'No job matched the supplied criteria',
                };
                
                res.writeHead(200);
                res.end(JSON.stringify(data));
            })
            .catch(function (err) {
                respondWithError(err.statusCode || 500, err);
            });
    } else if (action === 'job_history') {
        // List all scheduled webtask's result history
        
        if (!validate_params(['container', 'name']))
            return;
            
        return withMongoCollection(context.data.LOG_COLLECTION)
            .then(function (coll) {
                var query = {
                    container: context.data.container,
                    name: context.data.name,
                };
                
                var cursor = coll.find(query)
                    .sort({created_at: -1});
                    
                var fetchResults = Bluebird.promisify(cursor.toArray, cursor);
                
                // TODO (ggoodman): Pagination logic
                return fetchResults();
            })
            .map(stripMongoId)
            .then(function (data) {
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.end(JSON.stringify(data));
            })
            .catch(function (err) {
                respondWithError(err.statusCode || 500, err);
            });
    
    } else {
        // TODO (ggoodman): Consider: https://github.com/auth0/auth0-sandbox-ext/pull/1#discussion_r32170649
        
        res.writeHead(404);
        res.end('Method not found');
    }
    
    // Helper methods
    
    function stripMongoId (doc) {
        if (doc) {
            if (doc._id) delete doc._id;
            if (doc.last_result && doc.last_result._id) delete doc.last_result._id;
        }
        
        return doc;
    }
    
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
        
        if (!action) return respondWithError(400, 'Missing action.');
        if (!expected_method) return respondWithError(400, 'Invalid action.');
        if (expected_method !== req.method) return respondWithError(405, 'Invalid method.');
        
        return action;
    }

    function validate_params(required_params) {
        for (var i in required_params) {
            if (typeof context.data[required_params[i]] !== 'string') {
                return respondWithError(400, 'Missing query parameter ' + required_params[i] + '.');
            }
        }
        return true;
    }

    function validate_body(required_fields) {
        for (var i in required_fields) {
            if (!context.body[required_fields[i]]) {
                return respondWithError(400, 'Missing payload parameter ' + required_fields[i] + '.');
            }
        }
        return true;
    }

    function respondWithError(code, err) {
        
        if (!_.isString(err)) err = JSON.stringify(err);
        
        res.writeHead(code);
        res.end(err);
        
        return false;
    }
};