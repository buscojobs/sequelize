var Utils     = require("../../utils")
  , DataTypes = require("../../data-types")
  , util      = require("util")

module.exports = (function() {
  var QueryGenerator = {
    dialect: 'mssql',

    createSchema: function() {
      var query = "SHOW TABLES"
      return Utils._.template(query)({})
    },

    dropSchema: function(tableName, options) {
      return QueryGenerator.dropTableQuery(tableName, options)
    },

    showSchemasQuery: function() {
      return "SHOW TABLES"
    },

    createTableQuery: function(tableName, attributes, options) {
      options = Utils._.extend({
        engine: 'InnoDB',
        charset: null
      }, options || {})

      var self = this;

      var query   = "CREATE TABLE IF NOT EXISTS <%= table %> (<%= attributes%>)<%= comment %> ENGINE=<%= engine %> <%= charset %> <%= collation %>"
        , primaryKeys = []
        , foreignKeys = {}
        , attrStr = []

      for (var attr in attributes) {
        if (attributes.hasOwnProperty(attr)) {
          var dataType = this.mysqlDataTypeMapping(tableName, attr, attributes[attr])

          if (Utils._.includes(dataType, 'PRIMARY KEY')) {
            primaryKeys.push(attr)

            if (Utils._.includes(dataType, 'REFERENCES')) { 
               // MySQL doesn't support inline REFERENCES declarations: move to the end
              var m = dataType.match(/^(.+) (REFERENCES.*)$/)
              attrStr.push(this.quoteIdentifier(attr) + " " + m[1].replace(/PRIMARY KEY/, ''))
              foreignKeys[attr] = m[2]
            } else {
              attrStr.push(this.quoteIdentifier(attr) + " " + dataType.replace(/PRIMARY KEY/, ''))
            }            
          } else if (Utils._.includes(dataType, 'REFERENCES')) {
            // MySQL doesn't support inline REFERENCES declarations: move to the end
            var m = dataType.match(/^(.+) (REFERENCES.*)$/)
            attrStr.push(this.quoteIdentifier(attr) + " " + m[1])
            foreignKeys[attr] = m[2]
          } else {
            attrStr.push(this.quoteIdentifier(attr) + " " + dataType)
          }
        }
      }

      var values = {
        table: this.quoteTable(tableName),
        attributes: attrStr.join(", "),
        comment: options.comment && Utils._.isString(options.comment) ? " COMMENT " + this.escape(options.comment) : "",
        engine: options.engine,
        charset: (options.charset ? "DEFAULT CHARSET=" + options.charset : ""),
        collation: (options.collate ? "COLLATE " + options.collate : "")
      }
      , pkString = primaryKeys.map(function(pk) { return this.quoteIdentifier(pk) }.bind(this)).join(", ")

      if (!!options.uniqueKeys) {
        Utils._.each(options.uniqueKeys, function(columns) {
          values.attributes += ", UNIQUE uniq_" + tableName + '_' + columns.fields.join('_') + " (" + Utils._.map(columns.fields, self.quoteIdentifier).join(", ") + ")";
        })
      }

      if (pkString.length > 0) {
        values.attributes += ", PRIMARY KEY (" + pkString + ")"
      }

      for (var fkey in foreignKeys) {
        if(foreignKeys.hasOwnProperty(fkey)) {
          values.attributes += ", FOREIGN KEY (" + this.quoteIdentifier(fkey) + ") " + foreignKeys[fkey]
        }
      }

      return Utils._.template(query)(values).trim() + ";"
    },

    showTablesQuery: function() {
      return 'SHOW TABLES;'
    },

    uniqueConstraintMapping: {
      code: 'ER_DUP_ENTRY',
      map: function(str) {
        // we're manually remvoving uniq_ here for a future capability of defining column names explicitly
        var match = str.replace('uniq_', '').match(/Duplicate entry .* for key '(.*?)'$/)
        if (match === null || match.length < 2) {
          return false
        }

        return match[1].split('_')
      }
    },

    addColumnQuery: function(tableName, attributes) {
      var query      = "ALTER TABLE `<%= tableName %>` ADD <%= attributes %>;"
        , attrString = []

      for (var attrName in attributes) {
        var definition = attributes[attrName]

        attrString.push(Utils._.template('`<%= attrName %>` <%= definition %>')({
          attrName: attrName,
          definition: this.mysqlDataTypeMapping(tableName, attrName, definition)
        }))
      }

      return Utils._.template(query)({ tableName: tableName, attributes: attrString.join(', ') })
    },

    removeColumnQuery: function(tableName, attributeName) {
      var query = "ALTER TABLE `<%= tableName %>` DROP `<%= attributeName %>`;"
      return Utils._.template(query)({ tableName: tableName, attributeName: attributeName })
    },

    changeColumnQuery: function(tableName, attributes) {
      var query      = "ALTER TABLE `<%= tableName %>` CHANGE <%= attributes %>;"
      var attrString = []

      for (var attrName in attributes) {
        var definition = attributes[attrName]

        attrString.push(Utils._.template('`<%= attrName %>` `<%= attrName %>` <%= definition %>')({
          attrName: attrName,
          definition: definition
        }))
      }

      return Utils._.template(query)({ tableName: tableName, attributes: attrString.join(', ') })
    },

    renameColumnQuery: function(tableName, attrBefore, attributes) {
      var query      = "ALTER TABLE `<%= tableName %>` CHANGE <%= attributes %>;"
      var attrString = []

      for (var attrName in attributes) {
        var definition = attributes[attrName]

        attrString.push(Utils._.template('`<%= before %>` `<%= after %>` <%= definition %>')({
          before: attrBefore,
          after: attrName,
          definition: definition
        }))
      }

      return Utils._.template(query)({ tableName: tableName, attributes: attrString.join(', ') })
    },

    /*
      Returns an insert into command. Parameters: table name + hash of attribute-value-pairs.
    */
    insertQuery: function(table, valueHash, modelAttributes) {
      var query
        , valueQuery          = "INSERT INTO <%= table %> (<%= attributes %>) VALUES (<%= values %>)"
        , emptyQuery          = "INSERT INTO <%= table %>"
        , fields              = []
        , values              = []
        , key
        , value
        , modelAttributeMap   = {}

      if (modelAttributes) {
        Utils._.each(modelAttributes, function (attribute, key) {
          modelAttributeMap[key] = attribute;
          if (attribute.field) {
            modelAttributeMap[attribute.field] = attribute;
          }
        });
      }

      if (this._dialect.supports['DEFAULT VALUES']) {
        emptyQuery += " DEFAULT VALUES"
      } else if (this._dialect.supports['VALUES ()']) {
        emptyQuery += " VALUES ()"
      }

      if (this._dialect.supports['RETURNING']) {
        valueQuery += " RETURNING *"
        emptyQuery += " RETURNING *"
      }

      valueHash = Utils.removeNullValuesFromHash(valueHash, this.options.omitNull)

      for (key in valueHash) {
        if (valueHash.hasOwnProperty(key)) {
          value = valueHash[key]

          // SERIALS' can't be NULL in SQL Server
          if (!modelAttributeMap || !modelAttributeMap[key]){ 
            fields.push(this.quoteIdentifier(key));  
            values.push(this.escape(value, (modelAttributeMap && modelAttributeMap[key]) || undefined));
          }else if(!modelAttributeMap[key].autoIncrement){
            fields.push(this.quoteIdentifier(key));  
            values.push(this.escape(value, (modelAttributeMap && modelAttributeMap[key]) || undefined));
          }else if((modelAttributeMap[key].autoIncrement === true && value) || (modelAttributeMap[key].autoIncrement === false)){
            fields.push(this.quoteIdentifier(key));  
            values.push(this.escape(value, (modelAttributeMap && modelAttributeMap[key]) || undefined));
          }
        }
      }

      var replacements  = {
        table:      this.quoteTable(table),
        attributes: fields.join(","),
        values:     values.join(",")
      }

      query = (replacements.attributes.length ? valueQuery : emptyQuery) + ";"

      return Utils._.template(query)(replacements)
    },

    /*bulkInsertQuery: function(tableName, attrValueHashes, options) {
      var query = "INSERT<%= ignoreDuplicates %> INTO <%= table %> (<%= attributes %>) VALUES <%= tuples %>;"
        , tuples = []
        , allAttributes = []

      Utils._.forEach(attrValueHashes, function(attrValueHash, i) {
        Utils._.forOwn(attrValueHash, function(value, key, hash) {
          if (allAttributes.indexOf(key) === -1) allAttributes.push(key)
        })
      })

      // Delete auto-incremental attributes
      console.log(options)

      Utils._.forEach(attrValueHashes, function(attrValueHash, i) {
        tuples.push("(" +
          allAttributes.map(function (key) {
            return this.escape(attrValueHash[key])
          }.bind(this)).join(",") +
        ")")
      }.bind(this))

      var replacements  = {
        ignoreDuplicates: options && options.ignoreDuplicates ? ' IGNORE' : '',
        table: this.quoteTable(tableName),
        attributes: allAttributes.map(function(attr){
                      return this.quoteIdentifier(attr)
                    }.bind(this)).join(","),
        tuples: tuples
      }

      return Utils._.template(query)(replacements)
    },*/

    deleteQuery: function(tableName, where, options) {
      options = options || {}

      var table = this.quoteTable(tableName)
      if (options.truncate === true) {
        // Truncate does not allow LIMIT and WHERE
        return "TRUNCATE " + table
      }

      where = this.getWhereConditions(where)
      var limit = ""

      if(Utils._.isUndefined(options.limit)) {
        options.limit = 1;
      }

      if(!!options.limit) {
        limit = " LIMIT " + this.escape(options.limit)
      }

      return "DELETE FROM " + table + " WHERE " + where + limit
    },

    addIndexQuery: function(tableName, attributes, options) {
      var transformedAttributes = attributes.map(function(attribute) {
        if(typeof attribute === 'string') {
          return this.quoteIdentifier(attribute)
        } else {
          var result = ""

          if (!attribute.attribute) {
            throw new Error('The following index attribute has no attribute: ' + util.inspect(attribute))
          }

          result += this.quoteIdentifier(attribute.attribute)

          if (attribute.length) {
            result += '(' + attribute.length + ')'
          }

          if (attribute.order) {
            result += ' ' + attribute.order
          }

          return result
        }
      }.bind(this))

      var onlyAttributeNames = attributes.map(function(attribute) {
        return (typeof attribute === 'string') ? attribute : attribute.attribute
      }.bind(this))

      options = Utils._.extend({
        indicesType: null,
        indexName: Utils._.underscored(tableName + '_' + onlyAttributeNames.join('_')),
        parser: null
      }, options || {})

      return Utils._.compact([
        "CREATE", options.indicesType, "INDEX", options.indexName,
        (options.indexType ? ('USING ' + options.indexType) : undefined),
        "ON", tableName, '(' + transformedAttributes.join(', ') + ')',
        (options.parser ? "WITH PARSER " + options.parser : undefined)
      ]).join(' ')
    },

    showIndexQuery: function(tableName, options) {
      var sql = "SHOW INDEX FROM `<%= tableName %>`<%= options %>"
      return Utils._.template(sql)({
        tableName: tableName,
        options: (options || {}).database ? ' FROM `' + options.database + '`' : ''
      })
    },

    removeIndexQuery: function(tableName, indexNameOrAttributes) {
      var sql       = "DROP INDEX <%= indexName %> ON <%= tableName %>"
        , indexName = indexNameOrAttributes

      if (typeof indexName !== 'string') {
        indexName = Utils._.underscored(tableName + '_' + indexNameOrAttributes.join('_'))
      }

      return Utils._.template(sql)({ tableName: tableName, indexName: indexName })
    },

    attributesToSQL: function(attributes) {
      var result = {}

      for (var name in attributes) {
        var dataType = attributes[name]

        if (Utils.isHash(dataType)) {
          var template

          if (dataType.type.toString() === DataTypes.ENUM.toString()) {
            if (Array.isArray(dataType.values) && (dataType.values.length > 0)) {
              template = "ENUM(" + Utils._.map(dataType.values, function(value) {
                return this.escape(value)
              }.bind(this)).join(", ") + ")"
            } else {
              throw new Error('Values for ENUM haven\'t been defined.')
            }
          } else {
            template = dataType.type.toString();
          }

          if (dataType.hasOwnProperty('allowNull') && (!dataType.allowNull)) {
            template += " NOT NULL"
          }

          if (dataType.autoIncrement) {
            template += " auto_increment"
          }

          // Blobs/texts cannot have a defaultValue
          if (dataType.type !== "TEXT" && dataType.type._binary !== true && Utils.defaultValueSchemable(dataType.defaultValue)) {
            template += " DEFAULT " + this.escape(dataType.defaultValue)
          }

          if (dataType.unique === true) {
            template += " UNIQUE"
          }

          if (dataType.primaryKey) {
            template += " PRIMARY KEY"
          }

          if (dataType.comment && Utils._.isString(dataType.comment) && dataType.comment.length) {
            template += " COMMENT " + this.escape(dataType.comment)
          }

          if(dataType.references) {
            template += " REFERENCES " + this.quoteTable(dataType.references)

            if(dataType.referencesKey) {
              template += " (" + this.quoteIdentifier(dataType.referencesKey) + ")"
            } else {
              template += " (" + this.quoteIdentifier('id') + ")"
            }

            if(dataType.onDelete) {
              template += " ON DELETE " + dataType.onDelete.toUpperCase()
            }

            if(dataType.onUpdate) {
              template += " ON UPDATE " + dataType.onUpdate.toUpperCase()
            }

          }

          result[name] = template
        } else {
          result[name] = dataType
        }
      }

      return result
    },

    findAutoIncrementField: function(factory) {
      var fields = []

      for (var name in factory.attributes) {
        if (factory.attributes.hasOwnProperty(name)) {
          var definition = factory.attributes[name]

          if (definition && definition.autoIncrement) {
            fields.push(name)
          }
        }
      }

      return fields
    },

    addLimitAndOffset: function(options, query){
      query = query || ""
      if (options.offset && !options.limit) {
        query += " TOP " + options.offset + ", " + 18440000000000000000;
      } else if (options.limit) {
        if (options.offset) {
          query += " TOP " + options.offset + ", " + options.limit
        } else {
          query += " TOP " + options.limit
        }
      }
      return query;
    },

    quoteIdentifier: function(identifier, force) {
      if (identifier === '*') return identifier
      return "[" + identifier + "]";
    },
    /**
     * Generates an SQL query that returns all foreign keys of a table.
     *
     * @param  {String} tableName  The name of the table.
     * @param  {String} schemaName The name of the schema.
     * @return {String}            The generated sql query.
     */
    getForeignKeysQuery: function(tableName, schemaName) {
      return "SELECT CONSTRAINT_NAME as constraint_name FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE where TABLE_NAME = '" + tableName + "' AND CONSTRAINT_NAME!='PRIMARY' AND CONSTRAINT_SCHEMA='" + schemaName + "' AND REFERENCED_TABLE_NAME IS NOT NULL;"
    },

    /**
     * Generates an SQL query that removes a foreign key from a table.
     *
     * @param  {String} tableName  The name of the table.
     * @param  {String} foreignKey The name of the foreign key constraint.
     * @return {String}            The generated sql query.
     */
    dropForeignKeyQuery: function(tableName, foreignKey) {
      return 'ALTER TABLE ' + this.quoteTable(tableName) + ' DROP FOREIGN KEY ' + this.quoteIdentifier(foreignKey) + ';'
    },

    /*
      Returns a query for selecting elements in the table <tableName>.
      Options:
        - attributes -> An array of attributes (e.g. ['name', 'birthday']). Default: *
        - where -> A hash with conditions (e.g. {name: 'foo'})
                   OR an ID as integer
                   OR a string with conditions (e.g. 'name="foo"').
                   If you use a string, you have to escape it on your own.
        - order -> e.g. 'id DESC'
        - group
        - limit -> The maximum count you want to get.
        - offset -> An offset value to start from. Only useable with limit!
    */

    selectQuery: function(tableName, options, model) {
      // Enter and change at your own peril -- Mick Hansen
      options = options || {}

      var table               = null
        , self                = this
        , query
        , limit               = options.limit
        , mainQueryItems      = []
        , mainAttributes      = options.attributes && options.attributes.slice(0)
        , mainJoinQueries     = []
        // We'll use a subquery if we have hasMany associations and a limit and a filtered/required association
        , subQuery            = limit && (options.hasIncludeWhere || options.hasIncludeRequired || options.hasMultiAssociation)
        , subQueryItems       = []
        , subQueryAttributes  = null
        , subJoinQueries      = []
        , mainTableAs             = null
        
      if (!Array.isArray(tableName) && model) {
        options.tableAs = mainTableAs = model.name
      }
      options.table = table = !Array.isArray(tableName) ? this.quoteTable(tableName) : tableName.map(function(t) {
        if (Array.isArray(t)) {
          return this.quoteTable(t[0], t[1])
        }
        return this.quoteTable(t, true)
      }.bind(this)).join(", ")

      if (subQuery && mainAttributes) {
        if (model.hasPrimaryKeys) {
          model.primaryKeyAttributes.forEach(function(keyAtt){
            if(mainAttributes.indexOf(keyAtt) == -1){
              mainAttributes.push(keyAtt)
            }
          })
        } else {
          mainAttributes.push("id")
        }          
      }

      // Escape attributes
      mainAttributes = mainAttributes && mainAttributes.map(function(attr){
        var addTable = true

        if (attr instanceof Utils.literal) {
          return attr.toString(this)
        }

        if (attr instanceof Utils.fn || attr instanceof Utils.col) {
          return attr.toString(self)
        }

        if(Array.isArray(attr) && attr.length == 2) {
          if (attr[0] instanceof Utils.fn || attr[0] instanceof Utils.col) {
            attr[0] = attr[0].toString(self)
            addTable = false
          } else {
            if (attr[0].indexOf('(') === -1 && attr[0].indexOf(')') === -1) {
              attr[0] = this.quoteIdentifier(attr[0])
            }
          }
          attr = [attr[0], this.quoteIdentifier(attr[1])].join(' as ')
        } else {
          attr = attr.indexOf(Utils.TICK_CHAR) < 0 && attr.indexOf('"') < 0 ? this.quoteIdentifiers(attr) : attr
        }

        if (options.include && attr.indexOf('.') === -1 && addTable) {
          attr = mainTableAs + '.' + attr
        }
        return attr
      }.bind(this))

      // If no attributes specified, use *
      mainAttributes = mainAttributes || (options.include ? [mainTableAs+'.*'] : ['*'])

      // If subquery, we ad the mainAttributes to the subQuery and set the mainAttributes to select * from subquery
      if (subQuery) {
        // We need primary keys
        subQueryAttributes = mainAttributes
        mainAttributes = [mainTableAs+'.*']
      }

      if (options.include) {
        var generateJoinQueries = function(include, parentTable) {
          var table         = include.model.getTableName()
            , as            = include.as
            , joinQueryItem = ""
            , joinQueries = {
              mainQuery: [],
              subQuery: []
            }
            , attributes
            , association   = include.association
            , through       = include.through
            , joinType      = include.required ? ' INNER JOIN ' : ' LEFT OUTER JOIN '
            , includeWhere  = {}
            , whereOptions  = Utils._.clone(options)

          whereOptions.keysEscaped = true

          if (tableName !== parentTable && mainTableAs !== parentTable) {
            as = parentTable+'.'+include.as
          }

          // includeIgnoreAttributes is used by aggregate functions
          if (options.includeIgnoreAttributes !== false) {
            attributes  = include.attributes.map(function(attr) {
              var attrAs = attr;

              if (Array.isArray(attr) && attr.length == 2) {
                attr = attr.map(function ($attr) {
                  return $attr._isSequelizeMethod ? $attr.toString(self) : $attr;
                })
                
                attrAs = attr[1];
                attr = attr[0];
              }
              return self.quoteIdentifier(as) + "." + self.quoteIdentifier(attr) + " AS " + self.quoteIdentifier(as + "." + attrAs);
            })

            if (include.subQuery && subQuery) {
              subQueryAttributes = subQueryAttributes.concat(attributes)
            } else {
              mainAttributes = mainAttributes.concat(attributes)
            }
          }

          if (through) {
            var throughTable      = through.model.getTableName()
              , throughAs         = as + "." + through.as
              , throughAttributes = through.attributes.map(function(attr) {
                return self.quoteIdentifier(throughAs) + "." + self.quoteIdentifier(attr) + " AS " + self.quoteIdentifier(throughAs + "." + attr)
              })
              , primaryKeysSource = association.source.primaryKeyAttributes
              , tableSource       = parentTable
              , identSource       = association.identifier
              , attrSource        = primaryKeysSource[0]
              , where

              , primaryKeysTarget = association.target.primaryKeyAttributes
              , tableTarget       = as
              , identTarget       = association.foreignIdentifier
              , attrTarget        = primaryKeysTarget[0]

              , sourceJoinOn
              , targetJoinOn
              , targetWhere

            if (options.includeIgnoreAttributes !== false) {
              // Through includes are always hasMany, so we need to add the attributes to the mainAttributes no matter what (Real join will never be executed in subquery)
              mainAttributes = mainAttributes.concat(throughAttributes)
            }

            // Filter statement for left side of through
            // Used by both join and subquery where
            sourceJoinOn = self.quoteTable(tableSource) + "." + self.quoteIdentifier(attrSource) + " = "
              sourceJoinOn += self.quoteIdentifier(throughAs) + "." + self.quoteIdentifier(identSource)

            // Filter statement for right side of through
            // Used by both join and subquery where
            targetJoinOn = self.quoteIdentifier(tableTarget) + "." + self.quoteIdentifier(attrTarget) + " = "
              targetJoinOn += self.quoteIdentifier(throughAs) + "." + self.quoteIdentifier(identTarget)

            // Generate join SQL for left side of through
            joinQueryItem += joinType + self.quoteTable(throughTable, throughAs) + " ON "
              joinQueryItem += sourceJoinOn

            // Generate join SQL for right side of through
            joinQueryItem += joinType + self.quoteTable(table, as) + " ON "
              joinQueryItem += targetJoinOn


            if (include.where) {
              targetWhere = self.getWhereConditions(include.where, self.sequelize.literal(self.quoteIdentifier(as)), include.model, whereOptions)
              joinQueryItem += " AND "+ targetWhere
              if (subQuery) {
                if (!options.where) options.where = {}

                // Creating the as-is where for the subQuery, checks that the required association exists
                options.where["__"+throughAs] = self.sequelize.asIs([ '(',

                  "SELECT " + self.quoteIdentifier(throughAs) + "." + self.quoteIdentifier(identSource) + " FROM " + self.quoteTable(throughTable, throughAs),
                  ! include.required && joinType + self.quoteTable(association.source.tableName, tableSource) + " ON " + sourceJoinOn || '',
                  joinType + self.quoteTable(table, as) + " ON " + targetJoinOn,
                  "WHERE " + ( ! include.required && targetWhere || sourceJoinOn + " AND " + targetWhere ),
                  "LIMIT 1",

                ')', 'IS NOT NULL'].join(' '))
              }
            }
          } else {
            var primaryKeysLeft = association.associationType === 'BelongsTo' ? association.target.primaryKeyAttributes : include.association.source.primaryKeyAttributes
              , tableLeft       = association.associationType === 'BelongsTo' ? as : parentTable
              , attrLeft        = primaryKeysLeft[0]
              , tableRight      = association.associationType === 'BelongsTo' ? parentTable : as
              , attrRight       = association.identifier
              , joinOn

            // Filter statement
            // Used by both join and subquery where
            joinOn =
              // Left side
              (
                ( subQuery && !include.subQuery && include.parent.subQuery && !( include.hasParentRequired && include.hasParentWhere ) ) && self.quoteIdentifier(tableLeft + "." + attrLeft) ||
                self.quoteTable(tableLeft) + "." + self.quoteIdentifier(attrLeft)
              )

              + " = " +

              // Right side
              (
                ( subQuery && !include.subQuery && include.parent.subQuery && ( include.hasParentRequired && include.hasParentWhere ) ) && self.quoteIdentifier(tableRight + "." + attrRight) ||
                self.quoteTable(tableRight) + "." + self.quoteIdentifier(attrRight)
              )

            if (include.where) {
              joinOn += " AND " + self.getWhereConditions(include.where, self.sequelize.literal(self.quoteIdentifier(as)), include.model, whereOptions)

              // If its a multi association we need to add a where query to the main where (executed in the subquery)
              if (subQuery && association.isMultiAssociation && include.required) {
                if (!options.where) options.where = {}

                // Creating the as-is where for the subQuery, checks that the required association exists
                options.where["__"+as] = self.sequelize.asIs([ '(',

                  "SELECT " + self.quoteIdentifier(attrRight),
                  "FROM " + self.quoteTable(table, as),
                  "WHERE " + joinOn,
                  "LIMIT 1",

                ')', 'IS NOT NULL'].join(' '))
              }
            }

            // Generate join SQL
            joinQueryItem += joinType + self.quoteTable(table, as) + " ON " + joinOn

          }

          if (include.subQuery && subQuery) {
            joinQueries.subQuery.push(joinQueryItem);
          } else {
            joinQueries.mainQuery.push(joinQueryItem);
          }

          if (include.include) {
            include.include.forEach(function(childInclude) {
              if (childInclude._pseudo) return
              var childJoinQueries = generateJoinQueries(childInclude, as)

              if (childInclude.subQuery && subQuery) {
                joinQueries.subQuery = joinQueries.subQuery.concat(childJoinQueries.subQuery)
              } else {
                joinQueries.mainQuery = joinQueries.mainQuery.concat(childJoinQueries.mainQuery)
              }
            }.bind(this))
          }
          return joinQueries
        }

        // Loop through includes and generate subqueries
        options.include.forEach(function(include) {
          var joinQueries = generateJoinQueries(include, options.tableAs)

          subJoinQueries = subJoinQueries.concat(joinQueries.subQuery)
          mainJoinQueries = mainJoinQueries.concat(joinQueries.mainQuery)
        }.bind(this))
      }

      // If using subQuery select defined subQuery attributes and join subJoinQueries
      if (subQuery) {
        subQueryItems.push("SELECT ")

      // Else do it the reguar way
      } else {
        mainQueryItems.push("SELECT ")
      }

      var limitOrder = this.addLimitAndOffset(options, query)

      // Add LIMIT, OFFSET to sub or main query
      if (limitOrder) {
        if (subQuery) {
          subQueryItems.push(limitOrder)
        } else {
          mainQueryItems.push(limitOrder)
        }
      }

      // If using subQuery select defined subQuery attributes and join subJoinQueries
      if (subQuery) {
        subQueryItems.push(" " + subQueryAttributes.join(', ') + " FROM " + options.table)
        if (mainTableAs) {
          subQueryItems.push(" AS "+mainTableAs)
        }
        subQueryItems.push(" " + subJoinQueries.join(''))

      // Else do it the reguar way
      } else {
        mainQueryItems.push(" " + mainAttributes.join(', ') + " FROM " + options.table)
        if (mainTableAs) {
          mainQueryItems.push(" AS "+mainTableAs)
        }
        mainQueryItems.push(" " + mainJoinQueries.join(''))
      }

      // Add WHERE to sub or main query
      if (options.hasOwnProperty('where')) {

        options.where = this.getWhereConditions(options.where, mainTableAs || tableName, model, options)
        if (subQuery) {
          subQueryItems.push(" WHERE " + options.where)
        } else {
          mainQueryItems.push(" WHERE " + options.where)
        }
      }

      // Add GROUP BY to sub or main query
      if (options.group) {
        options.group = Array.isArray(options.group) ? options.group.map(function (t) { return this.quote(t, model) }.bind(this)).join(', ') : options.group
        if (subQuery) {
          subQueryItems.push(" GROUP BY " + options.group)
        } else {
          mainQueryItems.push(" GROUP BY " + options.group)
        }
      }
      
      // Add HAVING to sub or main query
      if (options.hasOwnProperty('having')) {
        options.having = this.getWhereConditions(options.having, tableName, model, options, false)
        if (subQuery) {
          subQueryItems.push(" HAVING " + options.having)
        } else {
          mainQueryItems.push(" HAVING " + options.having)
        }
      }

      // Add ORDER to sub or main query
      if (options.order) {
        var mainQueryOrder = [];
        var subQueryOrder = [];

        if (Array.isArray(options.order)) {
          options.order.forEach(function (t) {
            if (subQuery && !(t[0] instanceof Model) && !(t[0].model instanceof Model)) {
              subQueryOrder.push(this.quote(t, model))
            }
            mainQueryOrder.push(this.quote(t, model))
          }.bind(this))
        } else {
          mainQueryOrder.push(options.order)
        }
        
        if (mainQueryOrder.length) {
          mainQueryItems.push(" ORDER BY " + mainQueryOrder.join(', '))
        }
        if (subQueryOrder.length) {
          subQueryItems.push(" ORDER BY " + subQueryOrder.join(', '))
        }
      }

      // If using subQuery, select attributes from wrapped subQuery and join out join tables
      if (subQuery) {
        query = "SELECT " + mainAttributes.join(', ') + " FROM ("
          query += subQueryItems.join('')
        query += ") AS "+options.tableAs
        query += mainJoinQueries.join('')
        query += mainQueryItems.join('')
      } else {
        query = mainQueryItems.join('')
      }

      query += ";";

      return query
    },

    mysqlDataTypeMapping: function(tableName, attr, dataType) {
      if (Utils._.includes(dataType, 'UUID')) {
        dataType = dataType.replace(/UUID/, 'CHAR(36) BINARY')
      }

      return dataType
    }
  }

  return Utils._.extend(Utils._.clone(require("../abstract/query-generator")), QueryGenerator)
})()
