'use latest';

const hbs = require('handlebars');

/** Webtask proxy to authenticate with auth0
 * @param TASK_URL Task to authenticate
 */

const VIEW = hbs.compile(`
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf8" />
        <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=0" />

        <title>Authenticate</title>

        <script src="//cdn.auth0.com/js/lock-7.6.2.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/then-request/2.1.1/request.min.js"></script>
      </head>

      <body>
        <script type="application/javascript">
            window.TASK_URL     = '{{{TASK_URL}}}';
            window.CLIENT_ID    = '{{{CLIENT_ID}}}';
            window.AUTH0_DOMAIN = '{{{AUTH0_DOMAIN}}}';

            window.onload = {{{FUNC_TO_RUN}}};
        </script>
      </body>
    </html>
`);

const FUNC_TO_RUN = function() {
    var lock = new Auth0Lock(CLIENT_ID || 'ODGT86Z8Sx0e92shNVn9N5H8JNGAh8R9', AUTH0_DOMAIN || 'webtaskme.auth0.com');
    var lock_opts = {
        authParams: {
            scope: 'openid email'
        }
    };

    lock.show(lock_opts, function (err, profile, id_token) {
        if(err) return console.log(err);

        var opts = {
            headers: {
                'Authorization': 'Bearer ' + id_token
            }
        };

        request('GET', TASK_URL, opts)
            .then(function (res) {
                switch(res.headers['content-type']) {
                    case 'application/json':
                        var parsed = JSON.parse(res.body);

                        document.body.innerHTML      = '<pre>' + JSON.stringify(parsed, null, 1) + '</pre>';
                        break;
                    default:
                        document.body.innerHTML = res.body;
                }
            })
            .catch(function (err) {
                console.log(err);
            });
    });
}.toString();

function toQs(obj) {
    var str = '';

    Object.keys(obj)
        .forEach(function (key, index) {
            str += (index === 0 ? '?' : '&') + key + '=' + obj[key];
        });

    return str;
}

module.exports = (ctx, req, res) => {
    if(!ctx.data.container || !ctx.data.taskname) {
       res.statusCode = 400;
       res.end('Must provide container & taskname params');
    }

    var TASK_URL = (ctx.data.baseUrl   || 'https://webtask.it.auth0.com') +
                   '/api/run/'        +
                   ctx.data.container +
                   '/'                +
                   ctx.data.taskname  +
                   toQs(ctx.data.query);

    res.end(
        VIEW({
           FUNC_TO_RUN,
           CLIENT_ID:    ctx.data.clientId,
           AUTH0_DOMAIN: ctx.data.auth0Domain,
           TASK_URL
        })
    );
};
