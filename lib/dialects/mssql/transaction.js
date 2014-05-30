var ISOLATION_LEVEL = require('tedious').ISOLATION_LEVEL
  , Transaction = require('../../transaction');

var MSSQLTransaction = module.exports = function(sequelize, options) {
  Transaction.call(this, sequelize, options);
}

MSSQLTransaction.prototype = new Transaction();
MSSQLTransaction.prototype.constructor = MSSQLTransaction;

MSSQLTransaction.prototype.prepareEnvironment = function(callback) {
  var self           = this
  , connectorManager = self.sequelize.transactionManager.getConnectorManager(self.id)

  self.begin(function() {
    connectorManager.afterTransactionSetup(callback)
  })
}

MSSQLTransaction.prototype.begin = function(callback) {
  console.log("BEGIN");
  var connectorManager = this.sequelize.transactionManager.getConnectorManager(this.id);

  connectorManager.getConnection().then(function(connection){
    connection.beginTransaction(function(err){
      connectorManager.pool.release(connection);
      callback(err);
    });
  });
}

MSSQLTransaction.prototype.commit = function() {
  console.log("COMMIT");
  var self           = this
  , connectorManager = this.sequelize.transactionManager.getConnectorManager(this.id)

  connectorManager.getConnection().then(function(connection){
    connection.commitTransaction(function(err){
      self.cleanup();
    });
  });
}

MSSQLTransaction.prototype.rollback = function() {
  console.log("ROLLBACK");
  var self           = this
  , connectorManager = this.sequelize.transactionManager.getConnectorManager(this.id)

  connectorManager.getConnection().then(function(connection){
    connection.rollbackTransaction(function(err){
      self.cleanup();
    });
  });
}
