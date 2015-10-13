var dynamo;

// Set max sockets limit on http to enable large number of concurrent etcd watchers
require('http').globalAgent.maxSockets = 5000;
require('https').globalAgent.maxSockets = 5000;

return function (context, req, res) {

    if (req.method !== 'PUT' && req.method !== 'GET') {
        return error(405);
    }

    if (!dynamo) {
        // Create DynamoDB client 

        if (!validate_params(['ACCESS_KEY_ID', 'SECRET_ACCESS_KEY', 'REGION', 'TABLE']))
            return;

        console.log('Creating DynamoDB client', { req_id: context.id, region: context.data.REGION });

        var aws = require('aws-sdk');
        aws.config.accessKeyId = context.data.ACCESS_KEY_ID;
        aws.config.secretAccessKey = context.data.SECRET_ACCESS_KEY;
        aws.config.region = context.data.REGION;
        aws.config.sslEnabled = true;
        aws.config.logger = process.stdout;
        dynamo = new aws.DynamoDB({ params: { TableName: context.data.TABLE }});
    }

    if (req.method === 'PUT') {
        
        // Revoke the token

        if (!validate_params(['jti','jwt']))
            return;

        console.log('Revoking a token', {
            req_id: context.id,
            jwt: context.data.jwt
        });

        dynamo.putItem({
            Item: {
                jti: { S: context.data.jti },
                jwt: { S: context.data.jwt },
                t: { S: (new Date()).toString() }
            }
        }, function (err) {
            if (err) {
                console.log('Error revoking a token', {
                    req_id: context.id,
                    error: err.stack || error,
                    jwt: context.data.jwt
                });
                return error(502);
            }

            console.log('Revoked a token', {
                req_id: context.id
            });

            res.writeHead(200);
            return res.end();
        });
    }
    else { // req.method === 'GET'

        // Check token revocation

        if (!validate_params(['jti']))
            return;

        console.log('Checking token revocation', {
            req_id: context.id,
            jti: context.data.jti
        });

        dynamo.query({
            KeyConditions: {
                jti: { 
                    ComparisonOperator: 'EQ',
                    AttributeValueList: [ { S: context.data.jti } ]
                }
            }
        }, function (err, data) {
            if (err) {
                console.log('Error checking token revocation', {
                    req_id: context.id,
                    error: err.stack || err,
                    jti: context.data.jti
                });
                return error(502);
            }
            var status = data.Count > 0 ? 200 : 404;
            console.log('Checked token revocation', {
                req_id: context.id,
                jti: context.data.jti,
                status_code: status
            });
            res.writeHead(status);
            return res.end();
        });
    }

    function validate_params(required_params) {
        for (var i in required_params) {
            if (typeof context.data[required_params[i]] !== 'string') {
                return error(400, 'Missing ' + required_params[i] + '.');
            }
        };
        return true;
    }

    function error(code, err) {
        console.log(code + (err ? (': ' + err) : ''), { req_id: context.id });
        res.writeHead(code);
        res.end(err);
        return false;
    }
};