exports.test = function (ctx, cb) {
    cb(null, ctx);
};

Object.defineProperty(exports, 'cron_backend_mongodb', {
  configurable: false,
  enumerable: true,
  get: function () {
    return require('../webtasks/cron_backend_mongodb');
  }
});

Object.defineProperty(exports, 'store_code_s3', {
  configurable: false,
  enumerable: true,
  get: function () {
    return require('../webtasks/store_code_s3');
  }
});
