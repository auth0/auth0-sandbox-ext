var request = require('request');

return function (ctx, cb) {
    if (!ctx.data.API_KEY)
        return cb(new Error('Missing `API_KEY` parameter.'));
    if (!ctx.data.phone)
        return cb(new Error('Missing `phone` parameter.'));

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
                username: ctx.data.phone,
                password: ctx.data.verification_code,
                connection: 'sms',
                grant_type: 'password',
                scope: 'openid profile'
            })
        }, function (error, res, body) {
            if (error) {
                console.log('Failed to verify SMS code', ctx.data.phone, 'with error', error);
                return cb(error);
            }
            if (res.statusCode < 200 || res.statusCode > 299) {
                var msg = 'Unable to verify SMS code sent to ' 
                    + ctx.data.phone + ' with HTTP status ' + res.statusCode 
                    + ' and body ' + body;
                console.log(msg);
                return cb(new Error(msg));
            }
            try {
                body = JSON.parse(body);
            }
            catch (e) {
                var msg = 'Failed to verify SMS code sent to ' + ctx.data.phone 
                    + ' with response body ' + body;
                console.log(msg);
                return cb(new Error(msg));
            }
            console.log('Successful SMS verification ', ctx.data.phone);
            cb(null, body);
        }
    }
    else {

        // initiate SMS verification

        request({
            url: 'https://webtask-cli.auth0.com/api/v2/users',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + ctx.data.API_KEY
            },
            timeout: 20000,
            body: JSON.stringify({
                phone_number: ctx.data.phone,
                connection: 'sms',
                email_verified: false
            })
        }, function (error, res, body) {
            if (error) {
                console.log('Failed to initialize SMS verification to ', ctx.data.phone, 'with error', error);
                return cb(error);
            }
            if (res.statusCode < 200 || res.statusCode > 299) {
                var msg = 'Unable to initiate SMS verification to ' 
                    + ctx.data.phone + ' with HTTP status ' + res.statusCode 
                    + ' and body ' + body;
                console.log(msg);
                return cb(new Error(msg));
            }
            try {
                body = JSON.parse(body);
            }
            catch (e) {
                var msg = 'Failed to initialize SMS verification to ' + ctx.data.phone 
                    + ' with response body ' + body;
                console.log(msg);
                return cb(new Error(msg));
            }
            console.log('Initiated SMS verification ', ctx.data.phone);
            cb(null, body);
        });

    }
}