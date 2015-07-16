var Boom = require('boom');
var Bluebird = require('bluebird');
var Express = require('express');
var Jwt = require('jsonwebtoken');
var Webtask = require('webtask-tools');
var _ = require('lodash');

var app = Express();
var router = Express.Router();
var mongo;

Bluebird.longStackTraces();

// Check for required configuration parameters
app.use(function (req, res, next) {
    var data = req.webtaskContext.data;
    var required = ['JOB_COLLECTION', 'LOG_COLLECTION', 'MONGO_URL', 'cluster_url'];
    
    for (var i in required) {
        var key = required[i];
        
        if (!data[key]) {
            var err = Boom.badGateway('Cron webtask needs to be configured '
                + 'with the parameter: `' + key + '`.', data);
            return next(err);
        }
    }
    
    next();
});

// Set up connection to mongodb
app.use(function (req, res, next) {
    // Short-cut if mongo is already setup
    if (mongo) {
        req.mongo = mongo;
        return next();
    }
    
    var MongoClient = require('mongodb').MongoClient;
    var connect = Bluebird.promisify(MongoClient.connect, MongoClient);
    var data = req.webtaskContext.data;
    
    return connect(data.MONGO_URL)
        .then(function (db) {
            // Store the settled promise resolving to the db object
            mongo = req.mongo = db;
            
            console.log('connected to mongodb');
        })
        .catch(function (err) {
            throw Boom.wrap(err, 503, 'Database unreachable.');
        })
        .nodeify(next);
});

// Parse incoming json bodies
app.use(require('body-parser').json());

app.use(router);

app.use(function(err, req, res, next) {
    console.log(err.message);
    console.log(err.stack);
    
    if (!err.isBoom) err = Boom.wrap(err);
    
    res
        .set(err.output.headers)
        .status(err.output.statusCode)
        .json(err.output.payload);
});



router.post('/reserve',
    ensure('query', ['count', 'ttl']),
    function (req, res, next) {
        var data = req.webtaskContext.data;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var count = Math.max(0, Math.min(100, parseInt(req.query.count, 10)));
        var now = new Date();
        var nextAvailableAt = new Date(now.valueOf() + (parseInt(data.ttl, 10) * 1000));
        
        console.log('attempting to reserve ' + count + ' jobs.');
        
        Bluebird.map(_.range(count), function (n) {
            var filter = {
                cluster_url: data.cluster_url,
                next_available_at: {
                    $lte: now
                },
                state: 'active',
            };
            var update = {
                $set: {
                    next_available_at: nextAvailableAt
                }
            };
            return Bluebird.promisify(jobs.findOneAndUpdate, jobs)(filter, update, {
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
                console.log('error reserving job: ' + result.value().message);
                return null;
            } else {
                return result.value();
            }
        })
        // Elminate error queries and queries that did not match
        // (this is the case when there are fewer than N jobs
        // available for running now)
        .filter(Boolean)
        .map(stripMongoId)
        .then(res.json.bind(res), next);
});

router.get('/:container?', function (req, res, next) {
    var data = req.webtaskContext.data;
    var jobs = req.mongo.collection(data.JOB_COLLECTION);
    var query = {
        cluster_url: data.cluster_url,
    };
    
    // Both admin and user will hit this endpoint. When admin, container can be unset
    if (req.params.container) query.container = req.params.container;
    
    var limit = req.query.limit
        ? Math.max(0, Math.min(20, parseInt(req.query.limit, 10)))
        : 10;
    var skip = req.query.offset
        ? Math.max(0, parseInt(req.query.offset, 10))
        : 0;
    var cursor = jobs.find(query)
        .skip(skip)
        .limit(limit);
    
    Bluebird.promisify(cursor.toArray, cursor)()
        .catch(function (err) {
            throw Boom.wrap(err, 503, 'Error querying database.');
        })
        .map(stripMongoId)
        .then(res.json.bind(res), next);
});

// Internal handler for updating a job's state
router.post('/:container/:name',
    ensure('params', ['container', 'name']),
    ensure('body', ['criteria', 'updates']),
    function (req, res, next) {
        var data = req.webtaskContext.data;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var query = canonicalizeDates(_.defaults(req.body.criteria, {
           cluster_url: data.cluster_url,
           container: req.params.container,
           name: req.params.name,
        }));
        var updates = canonicalizeDates(req.body.updates);
        
        Bluebird.promisify(jobs.findOneAndUpdate, jobs)(query, updates, {
            returnOriginal: false,
            upsert: true,
        })
            .catch(function (err) {
                throw Boom.wrap(err, 503, 'Error updating database.');
            })
            .get('value')
            .then(stripMongoId)
            .then(res.json.bind(res), next);
});

// Create or update an existing cron job (idempotent)
router.put('/:container/:name',
    ensure('params', ['container', 'name']),
    ensure('body', ['token', 'schedule']),
    function (req, res, next) {
        var data = req.webtaskContext.data;
        var maxJobsPerContainer = parseInt(data.max_jobs_per_container, 10) || 100;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var now = new Date();
        var tokenData = Jwt.decode(req.body.token, {complete: true});
        
        var update = {
            $set: {
                state: 'active',
                schedule: req.body.schedule,
                token: req.body.token,
                cluster_url: data.cluster_url,
                container: req.params.container,
                name: req.params.name,
                expires_at: null,
                last_scheduled_at: now,
                next_available_at: now,
                token_data: tokenData.payload,
            },
            $setOnInsert: {
                results: [],
                run_count: 0,
                error_count: 0,
            }
        };
        
        if (tokenData.payload.exp) {
            update.$set.expires_at = new Date(tokenData.payload.exp);
        }
        
        if (tokenData.payload.nbf) {
            update.$set.next_available_at = new Date(tokenData.payload.nbf);
        }
        
        var countExistingCursor = jobs.find({
            cluster_url: data.cluster_url,
            container: req.params.container,
        });
        var countExisting = Bluebird.promisify(countExistingCursor.count, countExistingCursor)();
        
        var alreadyExistsCursor = jobs.find({
            cluster_url: data.cluster_url,
            container: req.params.container,
            name: req.params.name,
        });
        var alreadyExists = Bluebird.promisify(alreadyExistsCursor.count, alreadyExistsCursor)();
                
        Bluebird.all([countExisting, alreadyExists])
            .catch(function (err) {
                throw Boom.wrap(err, 503, 'Error querying database.');
            })
            .then(function (counts) {
                var sameContainerCount = counts[0];
                var exists = !!counts[1];
                
                if (!exists && sameContainerCount >= maxJobsPerContainer) {
                    throw Boom.badRequest('Unable to schedule more than '
                        + maxJobsPerContainer
                        + ' jobs per container.');
                }
                
                return Bluebird.promisify(jobs.findOneAndUpdate, jobs)({
                    cluster_url: data.cluster_url,
                    container: req.params.container,
                    name: req.params.name,
                }, update, {
                    returnOriginal: false,
                    upsert: true,
                })
                    .catch(function (err) {
                        throw Boom.wrap(err, 503, 'Error updating database.');
                    })
                    .get('value');
            })
            .then(stripMongoId)
            .then(res.json.bind(res), next);
});

router.get('/:container/:name',
    ensure('params', ['container', 'name']),
    function (req, res, next) {
        var data = req.webtaskContext.data;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var query = {
            cluster_url: data.cluster_url,
            container: req.params.container,
            name: req.params.name,
        };
        var projection = {
            results: { $slice: 1 },
        };
        
        Bluebird.promisify(jobs.findOne, jobs)(query, projection)
            .catch(function (err) {
                throw Boom.wrap(err, 503, 'Error querying database.');
            })
            .then(function (job) {
                if (!job) {
                    throw Boom.notFound('No such job `' + req.params.name + '`.');
                }
                
                return job;
            })
            .then(stripMongoId)
            .then(res.json.bind(res), next);
});

router.delete('/:container/:name',
    ensure('params', ['container', 'name']),
    function (req, res, next) {
        var data = req.webtaskContext.data;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var query = {
            cluster_url: data.cluster_url,
            container: req.params.container,
            name: req.params.name,
        };
        var sort = {
            cluster_url: 1,
            container: 1,
            name: 1,
        };
        
        Bluebird.promisify(jobs.findAndRemove, jobs)(query, sort, {})
            .catch(function (err) {
                console.log(err);
                throw Boom.wrap(err, 503, 'Error querying database.');
            })
            .then(function (job) {
                if (!job) {
                    throw Boom.notFound('No such job `' + req.params.name + '`.');
                }
            })
            .then(respondWith204, next);
        
        function respondWith204 () {
            res.status(204).send();
        }
});

router.get('/:container/:name/history',
    ensure('params', ['container', 'name']),
    function (req, res, next) {
        var data = req.webtaskContext.data;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var query = {
            cluster_url: data.cluster_url,
            container: req.params.container,
            name: req.params.name,
        };
        
        var limit = req.query.limit
            ? Math.max(0, Math.min(20, parseInt(req.query.limit, 10)))
            : 10;
        var skip = req.query.offset
            ? Math.max(0, parseInt(req.query.offset, 10))
            : 0;
        var projection = {
            results: { $slice: [skip, limit] },
        };
        
        Bluebird.promisify(jobs.findOne, jobs)(query, projection)
            .catch(function (err) {
                throw Boom.wrap(err, 503, 'Error querying database.');
            })
            .then(function (job) {
                if (!job) {
                    throw Boom.notFound('No such job `' + req.params.name + '`.');
                }
                
                return job;
            })
            .get('results')
            .map(stripMongoId)
            .then(res.json.bind(res), next);
});

module.exports = Webtask.fromConnect(app);


// Helper methods

function ensure (source, fields) {
    return function (req, res, next) {
        var data = req[source];
        
        for (var i in fields) {
            if (!data[fields[i]]) {
                return next(Boom.badRequest('Missing ' + source + 'parameter '
                    + '`' + fields[i] + '`.'));
            }
        }
        
        next();
    };
}

function canonicalizeDates (obj) {
    if (_.isArray(obj)) return _.map(obj, canonicalizeDates);
    if (_.isObject(obj)) {
        if (obj['$date']) return new Date(obj['$date']);
        else return _.mapValues(obj, canonicalizeDates);
    }
    return obj;
}

function stripMongoId (doc) {
    if (doc) {
        if (doc._id) delete doc._id;
        if (doc.results) doc.results = _.map(doc.results, stripMongoId);
    }
    
    return doc;
}