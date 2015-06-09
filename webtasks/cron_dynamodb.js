var dynamo;
var lodash;

return function (context, req, res) {
    var action = validate_method({
        put_job: 'PUT',
        get_job: 'GET',
        delete_job: 'DELETE',
        list_jobs: 'GET',
        reserve_jobs: 'POST',
        renew_reservation: 'PUT',
        release_reservation: 'DELETE',
    });
    
    if (!dynamo) {
        // Create DynamoDB client 

        if (!validate_params(['ACCESS_KEY_ID', 'SECRET_ACCESS_KEY', 'REGION', 'TABLE']))
            return;

        console.log('Creating DynamoDB client', { region: context.data.REGION });

        var aws = require('aws-sdk');
        aws.config.accessKeyId = context.data.ACCESS_KEY_ID;
        aws.config.secretAccessKey = context.data.SECRET_ACCESS_KEY;
        aws.config.region = context.data.REGION;
        aws.config.sslEnabled = true;
        aws.config.logger = process.stdout;
        dynamo = new aws.DynamoDB({ params: { TableName: context.data.TABLE }});
    }
    
    if (!lodash) lodash = require('lodash');
    
    if (action === 'put_job') {
        if (!validate_params(['container', 'name', 'schedule', 'token']))
            return;
        
        var now = (new Date()).toISOString();
        
        var item = {
            job_id: { S: context.data.container + "." + context.data.name },
            state: { S: 'active' },
            schedule: { S: context.data.schedule },
            token: { S: context.data.token },
            container: { S: context.data.container },
            name: { S: context.data.name },
            last_run_at: { S: now },
            next_run_at: { S: now },
            //last_result: { S: "" },
            run_count: { N: '0' },
            error_count: { N: '0' },
        };
        
        dynamo.putItem({
            Item: item,
            ReturnValues: 'ALL_OLD'
        }, function (err, data) {
            if (err) {
                console.log('Error storing a token schedule', {
                    error: err.stack || error,
                    schedule: context.data.schedule,
                    token: context.data.token,
                    container: context.data.container,
                    name: context.data.name,
                });
                
                return error(502, 'Error storing webtask schedule: ' + err.message );
            }
            
            item = _.mapValues(item, _.flow(_.values, _.first));
            
            res.writeHead(_.isEmpty(data) ? 201 : 200, {'Content-Type': 'application/json'});
            return res.end(JSON.stringify(item));
        });
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
        };
        return true;
    }

    function error(code, err) {
        console.log(code + (err ? (': ' + err) : ''));
        res.writeHead(code);
        res.end(err);
        return false;
    }
}