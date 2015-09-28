var aws = require('aws-sdk');
var sandboxjs = require('sandboxjs');
var initialized;

// Set max sockets limit on http to enable large number of concurrent etcd watchers
require('http').globalAgent.maxSockets = 5000;
require('https').globalAgent.maxSockets = 5000;

module.exports = function (context, req, res) {

    console.log('Request: ', {
        bucket: context.data.bucket,
        path: context.data.path,
        method: req.method,
        no_location: !!context.data.no_location
    });

    // Validate and normalize parameters

    var required_params = [
        'access_key_id', 'secret_access_key', 'region', 'path', 'bucket'
    ];
    for (var i in required_params) {
        if (typeof context.data[required_params[i]] !== 'string') {
            return error(400, 'Missing ' + required_params[i] + '.');
        }
    }

    // Authorize request

    if (typeof context.data.method === 'string') {
        if (req.method !== context.data.method) {
            return error(405, 'The verb must be ' + context.data.method + '.');
        }
    }
    else if (req.method !== 'GET' && req.method !== 'PUT') {
        return error(405, 'The verb must be PUT or GET.');
    }

    // Configure AWS proxy

    if (!initialized) {
        initialized = true;
        console.log('Initializing AWS proxy', {});
        aws.config.accessKeyId = context.data.access_key_id;
        aws.config.secretAccessKey = context.data.secret_access_key;
        aws.config.region = context.data.region;
        aws.config.sslEnabled = true;
        aws.config.logger = process.stdout;
    }
    
    if (context.data.method === 'GET') {
        read_code(context.data, function (err, data) {
            if (err) {
                console.log('S3 download error:', {
                    bucket: context.data.bucket,
                    path: context.data.path,
                    method: req.method,
                    no_location: !!context.data.no_location,
                    error: err.message || err.toString(),
                    details: JSON.stringify(err)
                });
                return error(err.statusCode || 502, err.message || err);
            }
            
            res.writeHead(200, {
                'Content-Type': 'application/octet-stream',
                'Cache-Control': 'no-cache'
            });
            return res.end(data);

        });
    }
    else {
        var profile = sandboxjs.init({
            url: context.cluster_url,
            container: context.container,
            token: context.token,
        });
        var options = {
            bucket: context.data.bucket,
            path: context.data.path,
            no_location: !!context.data.no_location,
            data: req,
            profile: profile,
        };
        
        // Stream data to S3
        store_code(options, function (err, url) {
            if (err) {
                return error(err.statusCode || 502, err.stack || err.message || err);
            }
            
            var headers = {};
            
            if (url) headers['Location'] = url;
            
            res.send(200, headers);
            res.end();
        });
    }

    return;

    function error(code, err) {
        try {
            console.log(code + ': ' + err);
            res.writeHead(code);
            res.end(err);
        }
        catch (e) {
            // ignore
        }
    }
};

module.exports.read_code = read_code;
module.exports.store_code = store_code;

// The AWS api must be appropriately configured prior to calling this.
function read_code(options, cb) {
    if (!options) options = {};

    var error;

    ['bucket', 'path'].forEach(function (key) {
        if (!hop(options, key)) error = new Error('Missing required option `'
            + key + '`.');
    });

    if (error) return cb(error);

    var s3 = new aws.S3({ params: { Bucket: options.bucket, Key: options.path }});

    s3.getObject(function(err, data) {
        if (err) {
            err.statusCode = 502;

            return cb(err);
        }
        
        return cb(null, data.Body);
    });
}

// The AWS api must be appropriately configured prior to calling this.
function store_code(options, cb) {
    if (!options) options = {};

    var error;

    ['bucket', 'path', 'data', 'profile'].forEach(function (key) {
        if (!hop(options, key)) error = new Error('Missing required option `'
            + key + '`.');
    });

    if (error) return cb(error);

    var s3 = new aws.S3({ params: { Bucket: options.bucket, Key: options.path }});

    s3.upload({ Body: options.data }).send(function(err, data) {
        if (err) {
            err.statusCode = 502;

            return cb(err);
        }

        console.log('Upload to S3 completed: ', data.Location);

        if (!!options.no_location) {
            return cb();
        }

        var read_token_options = {
            param: {
                method: 'GET',
                path: options.path,
            },
        };

        options.profile.create(read_token_options, function (err, webtask) {
            if (err) {
                err.statusCode = 502;

                return cb(err);
            }

            return webtask.url;
        });
    });
}

function hop (obj, prop) {
    return Object.prototype.hasOwnProperty.call(obj, prop);
}