require('dotenv').load();

var assert = require('assert')
    , async = require('async')

describe('revoke_dynamodb', function () {

    var context_params = ['ACCESS_KEY_ID', 'SECRET_ACCESS_KEY', 'REGION', 'TABLE'];
    var revoke_dynamodb = compile_webtask('revoke_dynamodb.js');

    it('prerequisities are met', function () {
        context_params.forEach(function (p) {
            assert.ok(typeof process.env[p] === 'string', p + ' env variable must be configured');
        });
    });

    it('token can be revoked', function (done) {
        var context = create_context(context_params, {
            jti: 'auth0-test-jti',
            jwt: 'auth0-test-jwt'
        });
        var req = { 
            method: 'PUT'
        };
        var res = create_res(function () {
            assert.equal(res.status, 200);
            done();

        });
        revoke_dynamodb(context, req, res);
    });

    it('revocation check for revoked token works', function (done) {
        async.series([
            function (callback) {
                var context = create_context(context_params, {
                    jti: 'auth0-test-jti',
                    jwt: 'auth0-test-jwt'
                });
                var req = { 
                    method: 'PUT'
                };
                var res = create_res(function () {
                    assert.equal(res.status, 200);
                    callback();
                });
                revoke_dynamodb(context, req, res);
            },
            function (callback) {
                var context = create_context(context_params, {
                    jti: 'auth0-test-jti'
                });
                var req = { 
                    method: 'GET'
                };
                var res = create_res(function () {
                    assert.equal(res.status, 200);
                    callback();
                });
                revoke_dynamodb(context, req, res);                
            }
        ], done);
    });

    it('revocation check for non-revoked token works', function (done) {
        var context = create_context(context_params, {
            jti: 'auth0-test-jti-nonexistent'
        });
        var req = { 
            method: 'GET'
        };
        var res = create_res(function () {
            assert.equal(res.status, 404);
            done();
        });
        revoke_dynamodb(context, req, res);
    });

    it('revocation without jti fails', function (done) {
        var context = create_context(context_params, {
            jwt: 'auth0-test-jwt'
        });
        var req = { 
            method: 'PUT'
        };
        var res = create_res(function () {
            assert.equal(res.status, 400);
            done();
        });
        revoke_dynamodb(context, req, res);
    });

    it('revocation without jwt fails', function (done) {
        var context = create_context(context_params, {
            jti: 'auth0-test-jti'
        });
        var req = { 
            method: 'PUT'
        };
        var res = create_res(function () {
            assert.equal(res.status, 400);
            done();
        });
        revoke_dynamodb(context, req, res);
    });

    it('revocation check without jti fails', function (done) {
        var context = create_context(context_params, {
            jwt: 'auth0-test-jwt'
        });
        var req = { 
            method: 'GET'
        };
        var res = create_res(function () {
            assert.equal(res.status, 400);
            done();
        });
        revoke_dynamodb(context, req, res);
    });

});

function compile_webtask(file) {
    return eval('(function () { '
        + require('fs').readFileSync(__dirname + '/../webtask/' + file, 'utf8')
        + '})')();
}

function create_context(global_params, params) {
    var context = { data: params };
    global_params.forEach(function (p) {
        context.data[p] = process.env[p];
    });
    return context;
}

function create_res(callback) {
    var res = {
        writeHead: function (status, headers) {
            res.status = status;
            res.headers = headers;
        },
        write: function (chunk) {
            if (chunk)
                res.body = (res.body || '') + chunk;
        },
        end: function (chunk) {
            res.write(chunk);
            callback();
        }
    };
    return res;
}
