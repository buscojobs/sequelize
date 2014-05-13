var _ = require('lodash')
  , Abstract = require('../abstract')

var MssqlDialect = function(sequelize) {
  this.sequelize = sequelize
}

MssqlDialect.prototype.supports = _.defaults({
  'VALUES ()': true,
  'LIMIT ON UPDATE':true
}, Abstract.prototype.supports)

module.exports = MssqlDialect
