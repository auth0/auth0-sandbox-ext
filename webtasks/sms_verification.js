var request = require('request');

return function (ctx, cb) {
    if (!ctx.data.API_KEY)
        return cb(new Error('Missing `API_KEY` parameter.'));
    if (!ctx.data.phone)
        return cb(new Error('Missing `phone` parameter.'));

    if (ctx.data.verification_code) {
        // verify user, return profile

        return cb(new Error('Not supported yet'));
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