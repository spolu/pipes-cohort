var fwk = require('pipes');

var config = fwk.baseConfig();

config['TINT_NAME'] = 'cohort';

config['COHORT_DBNAME'] = 'cohort';

/** export merged configuration */
exports.config = config;
