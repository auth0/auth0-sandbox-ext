var Boom = require('boom');
var Bluebird = require('bluebird');
var Cron = require('cron-parser');
var Express = require('express');
var Jwt = require('jsonwebtoken');
var Webtask = require('webtask-tools');
var _ = require('lodash');

var app = Express();
var router = Express.Router();
var mongo;

// Check for required configuration parameters
app.use(function (req, res, next) {
    var data = req.webtaskContext.data;
    var required = ['JOB_COLLECTION', 'MONGO_URL'];

    for (var i in required) {
        var key = required[i];

        if (!data[key]) {
            var err = Boom.badGateway('Cron webtask needs to be configured '
                + 'with the parameter: `' + key + '`.');
            return next(err);
        }
    }

    if (!data.CLUSTER_HOST) {
        data.CLUSTER_HOST = req.headers.host;
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
    var secrets = req.webtaskContext.secrets;
    
    return MongoClient.connect(secrets.MONGO_URL, {
        promiseLibrary: Bluebird,
    })
        .then(function(db) {
            mongo = req.mongo = db;
        })
        .catch(function (err) {
            throw Boom.wrap(err, 502, 'Database unreachable.');
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
    ensure('body', ['count', 'expiry', 'now']),
    function (req, res, next) {
        var data = req.webtaskContext.data;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var count = Math.max(0, Math.min(100, parseInt(req.body.count, 10)));
        var now = canonicalizeDates(req.body.now);
        var reservationExpiry = canonicalizeDates(req.body.expiry);
        var cluster_host = data.CLUSTER_HOST;

        console.log('Attempting to reserve `%d` jobs for cluster `%s` that are available at `%s`.',
            count, cluster_host, now.toISOString());
        
        var reservations = _.map(_.range(count), function (n) {
            var filter = {
                cluster_url: cluster_host,
                next_available_at: {
                    $lte: now
                },
                state: 'active',
            };
            var update = {
                $set: {
                    next_available_at: reservationExpiry,
                }
            };
            var options = {
                returnOriginal: false, // Return modified
            };
            
            return jobs.findOneAndUpdate(filter, update, options)
                .get('value'); // Only pull out the value
        });

    // Don't prevent one failure from blocking the entire
    // reservation (that would mean extra waiting on those that
    // didn't fail)
    Bluebird.settle(reservations)
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
        .tap(function (jobs) {
            console.log('Successfully reserved ' + jobs.length + ' job(s).');
        })
        .map(stripMongoId)
        .then(res.json.bind(res), next);
});

router.get('/:container?',
    function (req, res, next) {
        var data = req.webtaskContext.data;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var cluster_host = data.CLUSTER_HOST;
        var query = {
            cluster_url: cluster_host,
        };
    
        // Both admin and user will hit this endpoint. When admin, container can be unset
        if (req.params.container) query.container = req.params.container;
    
        var limit = req.query.limit
            ? Math.max(0, Math.min(20, parseInt(req.query.limit, 10)))
            : 10;
        var skip = req.query.offset
            ? Math.max(0, parseInt(req.query.offset, 10))
            : 0;
        var projection = {
            results: { $slice: 1 },
        };
        var cursor = jobs.find(query)
            .project(projection)
            .skip(skip)
            .limit(limit);
        
        cursor.toArray()
            .catch(function (err) {
                throw Boom.wrap(err, 502, 'Error querying database.');
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
        var cluster_host = data.CLUSTER_HOST;
        var query = canonicalizeDates(_.defaults(req.body.criteria, {
           cluster_url: cluster_host,
           container: req.params.container,
           name: req.params.name,
        }));
        var updates = { $set: canonicalizeDates(req.body.updates) };

        jobs.findOneAndUpdate(query, updates, {
            projection: {
                results: 0, // Exclude results
            },
            returnOriginal: false,
        })
            .catch(function (err) {
                throw Boom.wrap(err, 502, 'Error updating database');
            })
            .get('value')
            .then(function (job) {
                if (!job) {
                    throw Boom.notFound('No such job `'
                        + cluster_host + '/api/cron/'
                        + req.params.container + '/'
                        + req.params.name + '`.');
                }

                return job;
            })
            .then(stripMongoId)
            .tap(function(job) {
                console.log('Job metadata updated: `' + cluster_host + '/api/cron/'
                    + req.params.container + '/'
                    + req.params.name + '`.');
            })
            .then(res.json.bind(res), next);
});

// Create or update an existing cron job (idempotent)
router.put('/:container/:name',
    ensure('params', ['container', 'name']),
    ensure('body', ['token', 'schedule']),
    validateStates,
    function (req, res, next) {
        var data = req.webtaskContext.data;
        var maxJobsPerContainer = parseInt(data.max_jobs_per_container, 10) || 100;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var cluster_host = data.CLUSTER_HOST;
        var tokenData = Jwt.decode(req.body.token, {complete: true});
        var intervalOptions = {};
        var now = new Date();
        var nextAvailableAt;
        var state = req.fromState;

        if (tokenData.payload.exp) {
            intervalOptions.endDate = new Date(tokenData.payload.exp * 1000);
        }

        if (tokenData.payload.nbf) {
            intervalOptions.currentDate = new Date(tokenData.payload.nbf * 1000);
        }

        try {
            var interval = Cron.parseExpression(req.body.schedule, intervalOptions);
        } catch (e) {
            return next(Boom.badRequest('Invalid cron expression `'
                + req.body.schedule + '`.', req.body));
        }

        var update = {
            $set: {
                state: state,
                schedule: req.body.schedule,
                token: req.body.token,
                cluster_url: cluster_host,
                container: req.params.container,
                name: req.params.name,
                expires_at: intervalOptions.endDate || null,
                token_data: tokenData.payload,
            },
            $setOnInsert: {
                created_at: now,
                results: [],
                run_count: 0,
                error_count: 0,
            }
        };

        var countExistingCursor = jobs.find({
            cluster_url: cluster_host,
            container: req.params.container,
        });
        var countExisting = countExistingCursor.count();

        var alreadyExistsCursor = jobs.find({
            cluster_url: cluster_host,
            container: req.params.container,
            name: req.params.name,
        })
            .limit(1)
            // Limit the data returned
            .project({
                schedule: 1,
            });
        var alreadyExists = alreadyExistsCursor.next();

        Bluebird.all([countExisting, alreadyExists])
            .catch(function (err) {
                throw Boom.wrap(err, 502, 'Error querying database');
            })
            .spread(function (sameContainerCount, job) {
                if (!job && sameContainerCount >= maxJobsPerContainer) {
                    throw Boom.badRequest('Unable to schedule more than '
                        + maxJobsPerContainer
                        + ' jobs per container.');
                }
                
                // If the schedule changed, let's re-calculate `next_available_at`.
                if (!job || job.schedule !== req.body.schedule) {
                    try {
                        nextAvailableAt = interval.next();
                    } catch (e) {
                        return next(Boom.badRequest('The provided token\'s `nbf` and `exp` '
                            + 'claims are such that the job would never run with the '
                            + 'schedule `' + req.body.schedule + '`.'));
                        
                    }
                    
                    update.$set.next_available_at = nextAvailableAt;
                    update.$set.last_scheduled_at = nextAvailableAt;
                }

                return jobs.findOneAndUpdate({
                    cluster_url: cluster_host,
                    container: req.params.container,
                    name: req.params.name,
                }, update, {
                    returnOriginal: false,
                    upsert: true,
                    projection: {
                        results: {
                            $slice: 1,
                        },
                    },
                })
                    .catch(function (err) {
                        throw Boom.wrap(err, 502, 'Error updating database');
                    })
                    .get('value');
            })
            .then(stripMongoId)
            .tap(function(job) {
                console.log('Created or updated job: `' + cluster_host + '/api/cron/'
                    + req.params.container + '/'
                    + req.params.name + '`.');
            })
            .then(res.json.bind(res), next);
});

router.put('/:container/:name/state',
    ensure('params', ['container', 'name']),
    validateStates,
    function (req, res, next) {
        var validStates = ['active', 'inactive'];
        var data = req.webtaskContext.data;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var cluster_host = data.CLUSTER_HOST;
        var state = req.fromState;

        var query = {
            cluster_url: cluster_host,
            container: req.params.container,
            name: req.params.name,
        };

        var projection = {
            results: { $slice: 1 },
        };
    
        jobs.find(query)
            .project({
                state: 1,
            })
            .limit(1)
            .next()
            .catch(function (err) {
                throw Boom.wrap(err, 502, 'Error querying database.');
            })
            .then(function (job) {
                if (!job) {
                    throw Boom.notFound('No such job `'
                        + cluster_host + '/api/cron/'
                        + req.params.container + '/'
                        + req.params.name + '`.');
                }
                
                if (validStates.indexOf(job.state) < 0) {
                    return next(Boom.preconditionFailed('The job is in an invalid state.'));
                }
                
                // Make sure that no intervening action changed the job's state.
                query.state = job.state;
                
                var update = {
                    $set: {
                        state: state,
                    }
                };
                
                var options = {
                    projection: projection,
                    returnOriginal: false, // Return modified
                };
                
                return jobs.findOneAndUpdate(query, update, options)
                    .get('value'); // Only pull out the value
            })
            .then(stripMongoId)
            .then(res.json.bind(res), next);
});

router.get('/:container/:name',
    ensure('params', ['container', 'name']),
    function (req, res, next) {
        var data = req.webtaskContext.data;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var cluster_host = data.CLUSTER_HOST;
        var query = {
            cluster_url: cluster_host,
            container: req.params.container,
            name: req.params.name,
        };
        var projection = {
            results: { $slice: 1 },
        };

        jobs.findOne(query, projection)
            .catch(function (err) {
                throw Boom.wrap(err, 502, 'Error querying database.');
            })
            .then(function (job) {
                if (!job) {
                    throw Boom.notFound('No such job `'
                        + cluster_host + '/api/cron/'
                        + req.params.container + '/'
                        + req.params.name + '`.');
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
        var cluster_host = data.CLUSTER_HOST;
        var query = {
            cluster_url: cluster_host,
            container: req.params.container,
            name: req.params.name,
        };
        var options = {
            projection: {
                cluster_url: 1,
                container: 1,
                name: 1,
            },
        };

        jobs.findOneAndDelete(query, options)
            .catch(function (err) {
                console.log(err);
                throw Boom.wrap(err, 502, 'Error querying database');
            })
            .get('value')
            .then(function (job) {
                if (!job) {
                    throw Boom.notFound('No such job `'
                        + cluster_host + '/api/cron/'
                        + req.params.container + '/'
                        + req.params.name + '`.');
                }

                return job;
            })
            .tap(function() {
                console.log('Job destroyed: `' + cluster_host + '/api/cron/'
                    + req.params.container + '/'
                    + req.params.name + '`.');
            })
            .then(respondWith204(res), next);
});

router.get('/:container/:name/history',
    ensure('params', ['container', 'name']),
    function (req, res, next) {
        var data = req.webtaskContext.data;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var cluster_host = data.CLUSTER_HOST;
        var query = {
            cluster_url: cluster_host,
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

        jobs.find(query)
            .project(projection)
            .limit(1)
            .next()
            .catch(function (err) {
                throw Boom.wrap(err, 502, 'Error querying database');
            })
            .then(function (job) {
                if (!job) {
                    throw Boom.notFound('No such job `'
                        + cluster_host + '/api/cron/'
                        + req.params.container + '/'
                        + req.params.name + '`.');
                }

                return job;
            })
            .get('results')
            .map(stripMongoId)
            .then(res.json.bind(res), next);
});

router.post('/:container/:name/history',
    ensure('params', ['container', 'name']),
    ensure('body', ['scheduled_at', 'started_at', 'completed_at', 'type', 'body']),
    function (req, res, next) {
        var data = req.webtaskContext.data;
        var jobs = req.mongo.collection(data.JOB_COLLECTION);
        var result = canonicalizeDates(req.body);
        var cluster_host = data.CLUSTER_HOST;
        var query = {
            cluster_url: cluster_host,
            container: req.params.container,
            name: req.params.name,
        };
        var update = {
            $push: {
                results: {
                    $each: [result],
                    $position: 0, // Push to head of array
                    $slice: 100, // Maximum 100 history entries
                }
            },
            $inc: {
                run_count: +(result.type === 'success'),
                error_count: +(result.type === 'error'),
            }
        };

        jobs.findOneAndUpdate(query, update, {
            projection: {
                results: 0,
            },
            returnOriginal: false,
        })
            .catch(function (err) {
                throw Boom.wrap(err, 502, 'Error updating database');
            })
            .get('value')
            .then(function (job) {
                if (!job) {
                    throw Boom.notFound('No such job `'
                        + cluster_host + '/api/cron/'
                        + req.params.container + '/'
                        + req.params.name + '`.');
                }

                return job;
            })
            .then(stripMongoId)
            .tap(function() {
                console.log('Job result recorded: `' + cluster_host + '/api/cron/'
                    + req.params.container + '/'
                    + req.params.name + '`.');
            })
            .then(respondWith204(res), next);
});

module.exports = Webtask.fromConnect(app);


// Helper methods

function ensure (source, fields) {
    return function (req, res, next) {
        var data = req[source];

        for (var i in fields) {
            if (!data[fields[i]]) {
                return next(Boom.badRequest('Missing ' + source + ' parameter '
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


function respondWith204 (res) {
    return function () {
        res.status(204).send();
    };
}

function validateStates(req, res, next) {
    var validStates = ['active', 'inactive'];
    var state = req.body.state || req.query.state;
    
    if (state) {
        if (validStates.indexOf(state) < 0) {
            return next(Boom.badRequest('Job `state` must be one of: '
                + validStates.join(', ') + '`.'));
        }
    }
    
    req.fromState = state || 'active';
    
    next();
}