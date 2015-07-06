var request = require('request')
    , async = require('async')
    , ejs = require('ejs')
    , jsonwebtoken = require('jsonwebtoken');

return function (context, req, res) {
    async.series([
        function (callback) {
            // Normalize and validate params
            if (req.method !== 'GET' && req.method !== 'POST') 
                return callback(error(405, 'Only GET and POST requests are supported.'));
            if (typeof context.data.tenant === 'string' && !isNaN(context.data.month) && !isNaN(context.data.year)) {
                context.data.has_inputs = 1;
                context.data.year = Math.floor(+context.data.year);
                context.data.month = Math.floor(+context.data.month);
            }
            var required_params = ['STRIPE_KEY', 'ID_TOKEN', 'A0_CLIENT_ID', 'A0_DOMAIN'];
            for (var p in required_params)
                if (!context.data[required_params[p]])
                    return callback(
                        error(400, 'The `' + required_params[p] 
                            + '` parameter must be provided.'));
            if (context.data.has_inputs) {
                if (context.data.month < 1 || context.data.month > 12)
                    return callback(error(400, 'The month must be between 1 and 12.'));
                if (context.data.year < 2014)
                    return callback(error(400, 'The year must be at least 2014.'));
            }
            else if (req.method !== 'GET') 
                return callback(error(400, 'Requests without tenant, month, and year specified must be GET requests.'));

            callback();
        },
        function (callback) {
            // Generate HTML report or form to ask for parameters
            if (context.data.has_inputs) {
                return generate_report(context, req, res, callback);
            }
            else {
                return generate_form(context, req, res, callback);
            }
        }
    ], function (error) {
        if (error) {
            try {
                console.log('ERROR', error);
                res.writeHead(error.code || 500);
                res.end(error.stack || error.message || JSON.stringify(error, null, 2));
            }
            catch (e) {
                // ignore
            }
        }
    });
}

function generate_report(context, req, res, callback) {
    async.series([
        function (callback) {
            // Get stripe_id by calling delegation endpoint of auth0-finops
            // with a specially rigged authorization rule.
            request({
                url: 'https://' + context.data.A0_DOMAIN + '/delegation?tenant=' 
                    + encodeURIComponent(context.data.tenant),
                method: 'POST',
                form: {
                    client_id: context.data.A0_CLIENT_ID,
                    grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                    scope: 'openid subscription',
                    api_type: 'app',
                    target: '',
                    id_token: context.data.ID_TOKEN
                }
            }, function (err, sres, body) {
                console.log('DELEGATION RESPONSE', err, sres ? sres.statusCode : 0, body);
                if (err) return callback(error(502, error));
                if (sres.statusCode === 200) {
                    try {
                        body = JSON.parse(body);
                        var jwt = jsonwebtoken.decode(body.id_token);
                        context.data.stripe_customer_id = jwt.subscription.stripeCustomerId;
                        if (jwt.subscription.billing_info)
                            context.data.tax_id = jwt.subscription.billing_info.tax_id;
                    }
                    catch (e) {
                        return callback(error(502, 'Unable to parse Auth0 /delegation response.'));
                    }
                    if (typeof context.data.stripe_customer_id !== 'string')
                        return callback(error(400, 'The tenant is not associated with a Stripe.'));
                }
                else if (sres.statusCode === 400) {
                    try {
                        body = JSON.parse(body);
                    }
                    catch (e) {
                        return callback(error(502, 'Unable to parse Auth0 /delegation response.'));
                    }
                    if (body.error_description && body.error_description.indexOf('does not have a stripe') > 0) {
                        context.data.error = 'Tenant ' + context.data.tenant + ' does not have a Stripe ID.'
                        return generate_form(context, req, res, callback);
                    }
                    else
                        return callback(error(502, body));
                }
                else 
                    return callback(error(502, 'Invalid response status code from Auth0 /oauth/token: ' + sres.statusCode + '. Details: ' + body));
                return callback();
            })
        },
        function (callback) {
            // Get charges history
            // Process charge through Stripe
            var from_date = Math.floor((new Date(context.data.year, context.data.month - 1)).valueOf() / 1000);
            var to_date = Math.floor((new Date(context.data.year, context.data.month)).valueOf() / 1000);
            request({ 
                url: 'https://api.stripe.com/v1/charges'
                    + '?customer=' + context.data.stripe_customer_id
                    + '&created[gte]=' + from_date
                    + '&created[lt]=' + to_date
                    + '&expand[]=data.invoice'
                    + '&expand[]=data.customer'
                    + '&limit=100',
                method: 'GET',
                auth: {
                    user: context.data.STRIPE_KEY,
                    pass: ''
                }
            }, function (err, sres, body) {
                if (err)
                    return callback(error(502, err));
                if (sres.statusCode === 200) {
                    try {
                        context.data.charges = JSON.parse(body);
                    }
                    catch (e) {
                        return callback(error(502, 'Unable to parse Stripe response as JSON.'));
                    }
                }
                else 
                    return callback(error(502, 'Invalid response status code from Stripe: ' + sres.statusCode + '. Details: ' + body));
                return callback();
            });
        },
        function (callback) {
            // prepare model
            context.data.total_amount = 0;
            context.data.charges.data.forEach(function (charge) {
                charge.amount_view = charge_view(charge.amount);
                if (charge.status === 'paid') {
                    context.data.total_amount += charge.amount;
                    charge.plan_name = deep_get(charge, 'invoice.lines.data.0.plan.name');
                    charge.plan_interval = deep_get(charge, 'invoice.lines.data.0.plan.interval');
                    if (!context.data.due_date) 
                        context.data.due_date = new Date(charge.created * 1000);
                    if (!context.data.customer_name)
                        context.data.customer_name = deep_get(charge, 'customer.description');
                    if (!context.data.customer_name)
                        context.data.customer_name = deep_get(charge, 'customer.active_card.name');
                    //console.log('CHARGE', charge);
                    if (!context.data.address && deep_get(charge, 'card.address_line1')) {
                        context.data.address = [];
                        ['line1', 'line2', 'city', 'state', 'zip', 'country'].forEach(function (line) {
                            if (charge.card['address_' + line])
                                context.data.address.push(charge.card['address_' + line]);
                        });
                    }
                    if (!context.data.card && deep_get(charge, 'card.type'))
                        context.data.card = charge.card.type + ' ending ' + (charge.card.last4 || charge.card.dynamic_last4 || '****');
                }
            });
            context.data.total_amount_view = charge_view(context.data.total_amount);
            if (!context.data.due_date)
                context.data.due_date = new Date(context.data.year, context.data.month);
            if (!context.data.customer_name)
                context.data.customer_name = context.data.tenant;

            // generate report
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(ejs.render(invoice_view.stringify(), { context: context }));
            return callback(); 
        }
    ], callback);

    function charge_view(amount) {
        var cents = amount % 100;
        cents = cents < 10 ? '0' + cents : '' + cents;
        return 'USD&nbsp;' + Math.floor(amount / 100) + '.' + cents;
    }

    function deep_get(o, p) {
        p = p.split('.');
        for (var i in p) {
            if (!o || typeof o !== 'object') return undefined;
            o = o[p[i]];
        }
        return o;
    }
}

function generate_form(context, req, res, callback) {
    res.writeHead(context.data.error ? 400 : 200, { 'Content-Type': 'text/html' });
    res.end(ejs.render(form_view.stringify(), { context: context }));
    return callback();
}

function error(code, message) {
    var e = typeof message === 'string' ? new Error(message) : message;
    e.code = code;
    return e;
}

function form_view() {/*
<html>
<head>
    <title>Auth0 Stripe Invoice</title>
    <link rel="stylesheet" href="//maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css" />
    <style>
        body { 
            padding-top: 30px
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="jumbotron col-md-6 col-md-offset-3">
            <h1>Auth0 Invoice</h1>
            <% if (context.data.error) { %>
            <div class="alert alert-danger"><%= context.data.error %></div>
            <% }; %>
            <form method="POST">
              <div class="form-group">
                <label for="tenant">Tenant name</label>
                <input type="text" class="form-control" id="tenant" name="tenant" value="<%= context.data.tenant || '' %>">
              </div>
              <div class="form-group">
                <label for="month">Month</label>
                <input type="text" class="form-control" id="month" name="month" value="<%= context.data.month || ((new Date()).getMonth() + 1) %>">
              </div>
              <div class="form-group">
                <label for="year">Year</label>
                <input type="text" id="year" class="form-control" name="year" value="<%= context.data.year || ((new Date()).getYear() + 1900) %>">
              </div>
              <button type="submit" class="btn btn-default">Generate invoice</button>
            </form>
        </div>
    </div>
</body>
</html>
*/}

function invoice_view() {/*
<html>
<head>
    <title>auth0-invoice-<%= context.data.tenant %>-<%= context.data.month %>-<%= context.data.year %></title>
    <link rel="stylesheet" href="//maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css" />
    <style>
        body { 
            padding-top: 30px;
        }
        .wt-row {
            padding-top: 50px;
        }
        .wt-cell {
            padding-right: 10px;
            vertical-align: top;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="row">
            <div class="col-md-8">
                <h1>Auth0, Inc.</h1>
            </div>
        </div>
        <div class="row">
            <div class="col-md-4">
                <p>10777 Main Street, Suite 204
                    <br>Bellevue, WA 98004, USA
                    <br>+1 425 312 6521
                    <br>finops@auth0.com
                    <br>https://auth0.com
            </div>
        </div>
        <div class="row wt-row">
            <div class="col-md-4">
                <table>
                    <tr><td class="text-right wt-cell"><strong>Bill to</strong></td>
                    <td><%= context.data.customer_name %>
                    <% if (context.data.address) context.data.address.forEach(function (line) { %>
                        <br><%= line %>
                    <% }); %></td>
                    <% if (context.data.tax_id) { %>
                    <tr><td class="text-right wt-cell"><strong>Tax ID</strong></td>
                        <td><%= context.data.tax_id %></td></tr>
                    <% }; %>
                    </tr>
                    <tr><td class="text-right wt-cell"><strong>Invoice #</strong></td><td><%= context.data.tenant %>-<%= context.data.month %>-<%= context.data.year %></td></tr>
                    <tr><td class="text-right wt-cell"><strong>Terms</strong></td><td>Due on receipt</td></tr>
                    <tr><td class="text-right wt-cell"><strong>Date</strong></td><td><%= context.data.due_date.toDateString() %></td></tr>
                    <tr><td class="text-right wt-cell"><strong>Due date</strong></td><td><%= context.data.due_date.toDateString() %></td></tr>
                </table>
            </div>
        </div>
        <div class="row wt-row">
            <div class="col-md-12">
            <% if (context.data.charges.count === 0) { %>
            <p>There were no charges in the period.</p>
            <% } else { %>
            <table class="table">
                <tr><th>Date</th><th>Description</th><th>Amount</th></tr>
                <% context.data.charges.data.forEach(function (charge) { if (charge.status === 'paid') { %>
                    <tr>
                        <td><%= (new Date(charge.created * 1000)).toDateString() %></td>
                        <td>
                            <strong><%= 'Auth0 Identity Management Services' %></strong>
                            <% if (charge.plan_name) { %>
                            <br>Subscription plan: <%= charge.plan_name %>
                            <% }; %>
                            <% if (charge.plan_interval) { %>
                            <br>Interval: <%= charge.plan_interval %>
                            <% }; %>
                        </td>
                        <td><%-  charge.amount_view  %></td>
                    </tr>
                <% }}); %>
                <tr>
                    <td colspan="2" class="text-right"><strong>Payment<% if (context.data.card) { %>
                     with <%= context.data.card %><% }; %></strong></td>
                    <td><strong><%- context.data.total_amount_view %></strong></td>
                </tr>
                <tr>
                    <td colspan="2" class="text-right"><strong>Balance due</strong></td>
                    <td><strong>USD&nbsp;0.00</strong></td>
                </tr>
            </table>
            <% }; %>
            </div>
        </div>
    </div>
</body>
</html>
*/}
