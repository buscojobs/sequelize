var mssql
  , Pooling = require('generic-pool')
  , Query   = require("./query")
  , Utils   = require("../../utils")
  , Connection = null
  , without = function(arr, elem) { return arr.filter(function(e) { return e.query.uuid != elem.query.uuid }) }
module.exports = (function() {
  var ConnectorManager = function(sequelize, config) {
    try {
      if (config.dialectModulePath) {
        mssql = require(config.dialectModulePath)
      } else {
        Connection = require('tedious').Connection;
        mssql = new Connection({});
      }
    } catch (err) {
      console.log('You need to install the mssql connector manually, using the following command:');
      console.log('npm install -g tedious');
    }

    this.sequelize = sequelize
    this.client = null
    this.config = config || {}
    this.config.port = this.config.port || 3306
    this.disconnectTimeoutId = null
    this.queue = []
    this.activeQueue = []
    this.maxConcurrentQueries = (this.config.maxConcurrentQueries || 50)
    this.poolCfg = Utils._.defaults(this.config.pool, {
      maxConnections: 10,
      minConnections: 0,
      maxIdleTime: 1000,
      handleDisconnects: false,
      validate: validateConnection
    });
    this.pendingQueries = 0;
    this.useReplicaton = !!config.replication;
    this.useQueue = config.queue !== undefined ? config.queue : true;

    var self = this

    if (this.useReplicaton) {
      var reads = 0
        , writes = 0;

      // Init configs with options from config if not present
      for (var i in config.replication.read) {
        config.replication.read[i] = Utils._.defaults(config.replication.read[i], {
          host: this.config.host,
          port: this.config.port,
          username: this.config.username,
          password: this.config.password,
          instanceName: this.instanceName,
          database: this.config.database
        });
      }
      config.replication.write = Utils._.defaults(config.replication.write, {
        host: this.config.host,
        port: this.config.port,
        username: this.config.username,
        password: this.config.password,
        instanceName: this.instanceName,
        database: this.config.database
      });

      // I'll make my own pool, with blackjack and hookers!
      this.pool = {
        release: function (client) {
          if (client.queryType == 'read') {
            return this.read.release(client);
          } else {
            return this.write.release(client);
          }
        },
        acquire: function (callback, priority, queryType) {
          if (queryType == 'SELECT') {
            this.read.acquire(callback, priority);
          } else {
            this.write.acquire(callback, priority);
          }
        },
        drain: function () {
          this.read.drain();
          this.write.drain();
        },
        read: Pooling.Pool({
          name: 'sequelize-read',
          create: function (done) {
            if (reads >= self.config.replication.read.length) {
              reads = 0
            }
            var config = self.config.replication.read[reads++];

            connect.call(self, function (err, connection) {
              if (connection) {
                connection.queryType = 'read'
              }

              done(err, connection)
            }, config);
          },
          destroy: function(client) {
            disconnect.call(self, client)
          },
          validate: self.poolCfg.validate,
          max: self.poolCfg.maxConnections,
          min: self.poolCfg.minConnections,
          idleTimeoutMillis: self.poolCfg.maxIdleTime
        }),
        write: Pooling.Pool({
          name: 'sequelize-write',
          create: function (done) {
            connect.call(self, function (err, connection) {
              if (connection) {
                connection.queryType = 'read'
              }

              done(err, connection)
            }, self.config.replication.write);
          },
          destroy: function(client) {
            disconnect.call(self, client)
          },
          validate: self.poolCfg.validate,
          max: self.poolCfg.maxConnections,
          min: self.poolCfg.minConnections,
          idleTimeoutMillis: self.poolCfg.maxIdleTime
        })
      };
    } else if (this.poolCfg) {
      //the user has requested pooling, so create our connection pool
      this.pool = Pooling.Pool({
        name: 'sequelize-mssql',
        create: function (done) {
          connect.call(self, function (err, connection) {
            // This has to be nested for some reason, else the error won't propagate correctly
            done(err, connection);
          })
        },
        destroy: function(client) {
          disconnect.call(self, client)
        },
        max: self.poolCfg.maxConnections,
        min: self.poolCfg.minConnections,
        validate: self.poolCfg.validate,
        idleTimeoutMillis: self.poolCfg.maxIdleTime
      })
    }

    this.onProcessExit = function () {
      //be nice & close our connections on exit
      if (self.pool) {
        self.pool.drain()
      } else if (self.client) {
        disconnect(self.client)
      }

      return
    }.bind(this);

    process.on('exit', this.onProcessExit)
  }

  Utils._.extend(ConnectorManager.prototype, require("../abstract/connector-manager").prototype);

  ConnectorManager.prototype.query = function(sql, callee, options) {
    var self = this

    options = options || {}

    if (this.useQueue) {
      // If queueing we'll let the execQueueItem method handle connecting
      var queueItem = {
        query: new Query(null, this.sequelize, callee, options),
        sql: sql
      };

      queueItem.query.options.uuid = this.config.uuid
      enqueue.call(this, queueItem, options)
      return queueItem.query.promise.finally(function () {
        afterQuery.call(self, queueItem)
      })
    }

    var query = new Query(null, this.sequelize, callee, options);
    this.pendingQueries++;

    query.options.uuid = this.config.uuid

    return this.getConnection(options).then(function (client) {
      query.client = client
      return query.run(sql).finally(function () {
        self.pendingQueries--;
        if (self.pool) {
          self.pool.release(query.client);
        } else {
          if (self.pendingQueries === 0) {
            setTimeout(function() {
              if (self.pendingQueries === 0){
                self.disconnect.call(self);
              }
            }, 100);
          }
        }
      });
    })
  };

  ConnectorManager.prototype.getConnection = function(options) {
    var self = this;

    options = options || {}

    return new Utils.Promise(function (resolve, reject) {
      if (!self.pool) {
        // Regular client caching
        if (self.client) {
          return resolve(self.client);
        } else {
          // Cache for concurrent queries
          if (self._getConnection) {
            return resolve(self._getConnection)
          }

          // Set cache and acquire connection
          self._getConnection = this;
          connect.call(self, function(err, client) {
            if (err) {
              return reject(err);
            }

            // Unset caching, should now be caught by the self.client check
            self._getConnection = null;
            self.client = client;
            resolve(client);
          });
        }
      }
      if (self.pool) {
        // Acquire from pool
        self.pool.acquire(function(err, client) {
          if (err) {
            return reject(err);
          }
          resolve(client);
        }, options.priority, options.type);
      }
    })
  };

  ConnectorManager.prototype.disconnect = function() {
    if (this.client) {
      disconnect.call(this, this.client)
    }
    return
  };

  // private
  var disconnect = function(client) {
    var self = this;
    this.client = null;

    if (!client) {
      return // TODO possible orphaning of clients?
    }

    client.close();

    client.on('end', function() {
      if (!self.useQueue) {
        return client.destroy();
      }

      var intervalObj = null
      var cleanup = function () {
        // make sure to let client finish before calling destroy
        if (client._queue && (client._queue.length > 0)) {
          return
        }
        // needed to prevent mssql connection leak
        client.destroy()
        clearInterval(intervalObj)
      }
      intervalObj = setInterval(cleanup, 10)
      cleanup()
      return
    })
  }

  var connect = function(done, config) {
    config = config || this.config
    var connectionConfig = {
      server: config.host,
      userName: config.username,
      password: config.password,
      options: {
        database: config.database,
        instanceName: config.instanceName,
        rowCollectionOnRequestCompletion: true
      }
    };
    if (config.host.indexOf("\\")!=-1) {
      var params = config.host.split("\\");
      connectionConfig.server = params[0];
      connectionConfig.options.instanceName = params[1];
    }

    if (config.dialectOptions) {
      Object.keys(config.dialectOptions).forEach(function (key) {
        connectionConfig[key] = config.dialectOptions[key];
      });
    }

    var connection = new Connection(connectionConfig);
    connection.on('connect', function(err) {
      if (err) {
        
        // @TODO: MANEJAR CODIGOS DE ERROR
        var errorCode = /.*connect\s+(.*)/g.exec(err.message);
        if (errorCode != null)
          errorCode = errorCode[1];
        
        switch(errorCode) {
          case 'ECONNREFUSED':
          case 'ER_ACCESS_D2ENIED_ERROR':
            done('Failed to authenticate for SQL Server. Please double check your settings.')
            break
          case 'ENOTFOUND':
          case 'EHOSTUNREACH':
          case 'EINVAL':
            done('Failed to find SQL Server server. Please double check your settings.')
            break
          default:
            done(err);
            break;
        }
        return;
      }

      console.log('Connected!');
      done(null, connection);
    });

    //connection.execSql("SET time_zone = '+0:00'");
    // client.setMaxListeners(self.maxConcurrentQueries)
    this.isConnecting = false
    if (config.pool !== null && config.pool.handleDisconnects) {
      handleDisconnect(this.pool, connection)
    }
  }

  var handleDisconnect = function(pool, client) {
    client.on('error', function(err) {
      
      // @TODO: MANEJAR CODIGOS DE ERROR

      if (err.code !== 'PROTOCOL_CONNECTION_LOST') {
        throw err
      }

      client.close();
      
      pool.destroy(client)
    })
  }

  var validateConnection = function(client) {
    return client && client.state !== 'disconnected'
  }

  var enqueue = function(queueItem, options) {
    options = options || {}
    if (this.activeQueue.length < this.maxConcurrentQueries) {
      this.activeQueue.push(queueItem)
      execQueueItem.call(this, queueItem)
    } else {
      this.queue.push(queueItem)
    }
  }

  var dequeue = function(queueItem) {
    //return the item's connection to the pool
    if (this.pool) {
      this.pool.release(queueItem.client)
    }
    this.activeQueue = without(this.activeQueue, queueItem)
  }

  var transferQueuedItems = function(count) {
    for(var i = 0; i < count; i++) {
      var queueItem = this.queue.shift();
      if (queueItem) {
        enqueue.call(this, queueItem)
      }
    }
  }

  var afterQuery = function(queueItem) {
    dequeue.call(this, queueItem)
    transferQueuedItems.call(this, this.maxConcurrentQueries - this.activeQueue.length)
    disconnectIfNoConnections.call(this)
  }

  var execQueueItem = function(queueItem) {
    this.getConnection({
      priority: queueItem.query.options.priority,
      type: queueItem.query.options.type
    }).then(function (connection) {
      queueItem.query.client = connection
      queueItem.client = connection

      queueItem.query.run(queueItem.sql)
    }, function (err) {
      queueItem.query.reject(err)
    })
  }

  ConnectorManager.prototype.__defineGetter__('hasQueuedItems', function() {
    return (this.queue.length > 0) || (this.activeQueue.length > 0) || (this.client && this.client._queue && (this.client._queue.length > 0))
  })

  // legacy
  ConnectorManager.prototype.__defineGetter__('hasNoConnections', function() {
    return !this.hasQueuedItems
  })

  ConnectorManager.prototype.__defineGetter__('isConnected', function() {
    return this.client !== null
  })

  var disconnectIfNoConnections = function() {
    var self = this

    this.disconnectTimeoutId && clearTimeout(this.disconnectTimeoutId)
    this.disconnectTimeoutId = setTimeout(function() {
      self.isConnected && !self.hasQueuedItems && self.disconnect()
    }, 100)
  }

  return ConnectorManager
})()