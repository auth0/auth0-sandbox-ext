;(function () {

    window.webtaskify = function (options) {
        if (typeof options === 'string')
            options = { func: options };
        if (typeof options !== 'object')
            throw new Error('options must be an object');
        if (typeof options.func !== 'string')
            throw new Error('options.func missing or not a string');

        normalize_param('token', options);
        normalize_param('url', options, 'https://webtask.it.auth0.com');
        normalize_param('container', options);

        var script = document.scripts[options.func];
        if (!script)
            throw new Error('The script tag with `' + options.func + '` id not found.');
        if (script.type !== 'text/nodejs' && script.type !== 'text/csharp')
            throw new Error('The script tag `' + options.func + '` must specify `text/nodes` or `text/csharp` type.');

        return function (data, callback) {
            if (callback && typeof callback !== 'function')
                throw new Error('The callback must be a function.');

            var code = 'return function (cb) { var context = { data: ' + JSON.stringify(data) + ' }; '
                + '(function () {'
                + (script.type === 'text/nodejs'
                    ? script.text 
                    : 'return require("edge").func(function () {/*\n' + script.text + '\n*/}); '
                  )
                + '})()(context, cb); }';

            var req = new XMLHttpRequest();
            if (callback) {
                req.onload = function () {
                    var result = this.responseText;
                    try {
                        result = JSON.parse(result);
                    }
                    catch (e) {
                    }
                    if (this.status === 200) {
                        return callback(null, result);
                    }
                    else {
                        var error = new Error('Execution failure (' + result.status + '): ' 
                            + (this.responseText || 'no response.'));
                        error.code = result.status;
                        error.result = result;
                        return callback(error);
                    }
                };
            }
            req.open('post', options.url + '/api/run/' + options.container, true);
            req.setRequestHeader('Authorization', 'Bearer ' + options.token);
            req.send(code);
        };
    };

    var query = getParams(window.location.search.substring(1));
    var hash = getParams(window.location.hash.substring(1));

    function normalize_param(param, options, fallback) {
        options[param] =
            options[param]
            || hash['webtask_' + param]
            || query['webtask_' + param]
            || window.localStorage.getItem('webtask_' + param)
            || fallback;

        if (typeof options[param] !== 'string')
            throw new Error('The `' + param + '` parameter must be specified. You can specify it through '
                + '`options.' + param + '` property, `webtask_' + param + '` URL hash key or query parameter, '
                + 'or `webtask_' + param + '` local storage key.'
                + (param !== 'token' ? '' :
                    ' You can get your webtask token at https://webtask.io.'));
    }

    function getParams(str) {
        var params = {};
        var e,
            a = /\+/g,  // Regex for replacing addition symbol with a space
            r = /([^&;=]+)=?([^&;]*)/g,
            d = function (s) { return decodeURIComponent(s.replace(a, " ")); },
            q = str;

        while (e = r.exec(q))
           params[d(e[1])] = d(e[2]);

        return params;
    }

})();