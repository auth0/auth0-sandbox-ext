# Webtask extensions

This project implements extensibility points of the Auth0 Webtask runtime using Auth0 Webtasks themselves. It also provides a pattern of local development of webtasks which combines GitHub code management, local testability of webtasks, and webtask deployment.

## JWT token revocation

One extensibility point of Auth0 Webtask runtime is support for JWT token revocation. JWT token revocation requires durable storage so that information about revoked tokens can be queried and existing tokens can be marked as revoked. This functionality of Auth0 Webtasks is externalized behind an HTTP API and implemented as a webtask itself, so that various implementations can take dependency on variety of storage technologies. Current implementation is using DynamoDB.

The HTTP API is as follows:

```
HTTP PUT /?jti={jti}&jwt={jwt}
```

The PUT endpoint revokes a JWT token identified with the `jti` identifier. The `jwt` represents the entire JWT token being revoked which is stored for ease of future auditing. There is no validation of whether `jti` matches the identifier in the token itself. The API returns HTTP 200 on success, or an error code otherwise.

```
HTTP GET /?jti={jti}
```

The GET endpoint checks the revocation status of a JWT token with specific `jti` value. HTTP 200 status code indicates the token is revoked (i.e. the information about that token is present in the revocation database). HTTP 404 status code indicates the token is not revoked (i.e. no information about that token was found in the revocation database). Other codes indicate error conditions in the revocation check itself.

## Webtask code

Webtask code for DynamoDB revocation check is [here](https://github.com/auth0/auth0-sandbox-ext/blob/master/webtask/revoke_dynamodb.js). The URL pointing to the raw representiation of this code, which can be used when creating the webtask token, is https://raw.githubusercontent.com/auth0/auth0-sandbox-ext/master/webtask/revoke_dynamodb.js.

## Local testing

This repository establishes a pattern of local testing and development of webtasks.

The *mocha* test framework is used to execute tests.

The [package.json](https://github.com/auth0/auth0-sandbox-ext/blob/master/package.json) file lists all Node.js modules the webtask code has a dependency on within the `devDependencies` section. These modules inclue a *subset* of modules available in the Auth0 Webtask cluster, as well as additional modules that may be used by the test code itself.

**NOTE** The *.env* file placed at the root of the repository on the development machine (and excluded from source versioning) must include all secret parameters required by the webtask code. These are the same parameters that at runtime will be included in an encrypted form in the webtask token.

The [test code](https://github.com/auth0/auth0-sandbox-ext/blob/master/test/tests.js) follows this pattern:

1. The *.env* file is loaded and secret paramaters added to `process.env`. In this case they include AWS credentials and parameters to connect to a DynamoDB table.  
2. The [webtask code](https://github.com/auth0/auth0-sandbox-ext/blob/master/webtask/revoke_dynamodb.js) to be tested is loaded and compiled into a callable JavaScript function using `eval` - just like it will be in the Auth0 Webtask runtime.
3. The `context`, `req`, and `res` arguments of the webtask function are mocked within the test code itself. The secret parameters from the *.env* file are added to `context.data`. The `req` object is constructed on a per-test basis. The `res.{writeHead|write|end}` is mocked to capture the response data.
4. When the mock of `res.end` is called, a callback function is invoked that returns control to test code and allows it to perform validation checks.

With this setup, webtask code can be developed and tested locally with `mocha`.

## Building webtask tokens

tl;dr

Basic idea is to have `npm build` (or we can enhance webtaskify to support this mode) which will create a webtask token for all webtasks in the repository. By default the command will set the webtask code URL to the GitHub raw URL of the webtask code (this can be automatically determined from the environment). It will also add parameters from the `.env` file as encrypted parameters to the webtask token. With this mechanism in place building webtask tokens will be a one-command process.

## Issue Reporting

If you have found a bug or if you have a feature request, please report them at this repository issues section. Please do not report security vulnerabilities on the public GitHub issue tracker. The [Responsible Disclosure Program](https://auth0.com/whitehat) details the procedure for disclosing security issues.

## Author

[Auth0](auth0.com)

## License

This project is licensed under the MIT license. See the [LICENSE](LICENSE) file for more info.
