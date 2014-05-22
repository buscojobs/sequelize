var Utils         = require("../../utils")
  , AbstractQuery = require('../abstract/query')
  , uuid          = require('node-uuid')
  , Request       = require('tedious').Request;

module.exports = (function() {
  var Query = function(client, sequelize, callee, options) {
    this.client    = client;
    this.callee    = callee;
    this.sequelize = sequelize;
    this.uuid      = uuid.v4();
    this.options   = Utils._.extend({
      logging: console.log,
      plain: false,
      raw: false
    }, options || {});

    var self = this;
    this.checkLoggingOption();
    this.promise = new Utils.Promise(function (resolve, reject) {
      self.resolve = resolve;
      self.reject = reject;
    });
  }

  Utils.inherit(Query, AbstractQuery);
  Query.prototype.run = function(sql) {
    var self = this;
    this.sql = sql;

    if (this.options.logging !== false) {
      this.sequelize.log('Executing (' + this.options.uuid + '): ' + this.sql);
    }
    
    var request = new Request(self.sql, function(err, rowCount, rows) {
      //console.log(self.sql);
      if (err) {
        console.log(self.sql)
        console.log(JSON.stringify(err));
        self.reject(err);
      } else {
        // Since the rows returned are in the tedious raw format, we need to reformat them
        // before returning them.
        var newRows = [];
        var newRow;
        rows.forEach(function(row) {
          newRow = {};
          row.forEach(function(column) {
            newRow[column.metadata.colName] = column.value;
          });
          newRows.push(newRow);
          console.log(newRow);
        });
        //console.log(JSON.stringify(newRows));
        self.resolve(self.formatResults(newRows));
      }
      self.promise.emit('sql', self.sql, self.options.uuid);
/*
      if (err) {
        err.sql = sql;

        self.reject(err);
      } else {
        self.resolve(self.formatResults(results))
      }
      */
    });

    self.client.execSql(request);

    return this.promise;
  }

  return Query;
})();
