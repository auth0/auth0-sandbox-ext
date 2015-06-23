var Request = require('request');
 
return function (context, cb) {
    Request({
        url: 'https://google.com',
        time: true,
    }, function (err, res, body) {
        if (err) return cb(err);
        
        cb(null, res.elapsedTime);
    })
}