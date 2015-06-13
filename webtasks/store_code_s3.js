var aws = require('aws-sdk');
var initialized;

return function (context, req, res) {

    console.log('Request: ', { 
        bucket: context.data.bucket, 
        path: context.data.path, 
        method: req.method 
    });

    // Validate and normalize parameters    

    var required_params = [
        'access_key_id', 'secret_access_key', 'region', 'path', 'bucket'
    ];
    for (var i in required_params) {
        if (typeof context.data[required_params[i]] !== 'string') {
            return error(400, 'Missing ' + required_params[i] + '.');
        }
    };

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
        console.log('Initializing AWS proxy', {})
        aws.config.accessKeyId = context.data.access_key_id;
        aws.config.secretAccessKey = context.data.secret_access_key;
        aws.config.region = context.data.region;
        aws.config.sslEnabled = true;
        aws.config.logger = process.stdout;
    }

    var s3 = new aws.S3({ params: { Bucket: context.data.bucket, Key: context.data.path }});

    if (context.data.method === 'GET') {
        // Stream data from S3
        s3.getObject().createReadStream().pipe(res);
    }
    else {
        // Stream data to S3
        s3.upload({ Body: req }).send(function(err, data) {
            if (err) {
                return error(502, err.stack || err.message || err);
            } 
            else {
                console.log('Upload to S3 completed: ', data.Location);
                context.create_token_url({
                    // Fix the method and path parametrs to only allow GET of the S3 data
                    params: {
                        method: 'GET',
                        path: context.data.path
                    },
                    disable_parse_body: true,
                    disable_merge_body: true,
                    disable_self_revocation: true,
                    delegation_depth: 0
                }, function (err, url) {
                    if (err) {
                        return error(502, err.stack || err.message || err);
                    } 
                    res.writeHead(200, { Location: url });
                    res.end();
                })
            }
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