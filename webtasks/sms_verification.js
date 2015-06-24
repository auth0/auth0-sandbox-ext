var request = require('request');

return function (ctx, cb) {
    if (!ctx.data.API_KEY)
        return cb(new Error('Missing `API_KEY` parameter.'));
    if (!ctx.data.phone && !ctx.data.email)
        return cb(new Error('Missing `phone` and `email` parameters.'));

    if (ctx.data.verification_code) {

        // verify user, return profile

        request({
            url: 'https://webtask-cli.auth0.com/oauth/ro',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            timeout: 20000,
            body: JSON.stringify({
                client_id: 'pyp3Ee2t0vhpC3cB1TUUCtRQv3qdEoTe',
                username: ctx.data.phone || ctx.data.email,
                password: ctx.data.verification_code,
                connection: ctx.data.phone ? 'sms' : 'email',
                grant_type: 'password',
                scope: 'openid webtask'
            })
        }, function (error, res, body) {
            if (error) {
                console.log('Failed to verify code sent to', ctx.data.phone || ctx.data.email, 'with error', error);
                return cb(error);
            }
            if (res.statusCode < 200 || res.statusCode > 299) {
                var msg = 'Unable to verify code sent to ' 
                    + (ctx.data.phone || ctx.data.email) + ' with HTTP status ' + res.statusCode 
                    + ' and body ' + body;
                console.log(msg);
                return cb(new Error(msg));
            }
            try {
                body = JSON.parse(body);
            }
            catch (e) {
                var msg = 'Failed to verify code sent to ' + (ctx.data.phone || ctx.data.email)
                    + ' with response body ' + body;
                console.log(msg);
                return cb(new Error(msg));
            }
            console.log('Successful verification ', ctx.data.phone || ctx.data.email);
            cb(null, body);
        });
    }
    else {

        // initiate verification

        var payload;
        if (ctx.data.phone) {
            payload = {
                phone_number: ctx.data.phone,
                connection: 'sms',
                email_verified: false
            }
        }
        else {
            payload = {
                email: ctx.data.email,
                connection: 'email'
            }
        }

        request({
            url: 'https://webtask-cli.auth0.com/api/v2/users',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + ctx.data.API_KEY
            },
            timeout: 20000,
            body: JSON.stringify(payload)
        }, function (error, res, body) {
            if (error) {
                console.log('Failed to initialize verification to ', ctx.data.phone || ctx.data.email, 'with error', error);
                return cb(error);
            }
            if (res.statusCode < 200 || res.statusCode > 299) {
                var msg = 'Unable to initiate verification to ' 
                    + (ctx.data.phone || ctx.data.email) + ' with HTTP status ' + res.statusCode 
                    + ' and body ' + body;
                console.log(msg);
                return cb(new Error(msg));
            }
            try {
                body = JSON.parse(body);
            }
            catch (e) {
                var msg = 'Failed to initialize verification to ' + (ctx.data.phone || ctx.data.email)
                    + ' with response body ' + body;
                console.log(msg);
                return cb(new Error(msg));
            }
            console.log('Initiated verification ', ctx.data.phone || ctx.data.email);
            cb(null, body);
        });

    }
}