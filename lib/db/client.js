
/**
 * MySql: https://github.com/felixge/node-mysql
 */

function MySql () {
    this.client = require('mysql');
    this.connection = null;
    this.config = null;

    this.mysql = true;
    this.name = 'mysql';
}

MySql.prototype.connect = function (options, done) {
    this.connection = this.client.createConnection(options);
    this.connection.connect(function (err) {
        if (err) return done(err);
        this.config = this.connection.config;
        this.config.schema = this.config.database;
        done();
    }.bind(this));
    
    this.connection.on('error', function (err) {
        if (err.code == 'PROTOCOL_CONNECTION_LOST') {
            this.handleDisconnect(options);
        }
        else throw err;
    }.bind(this));
}

MySql.prototype.handleDisconnect = function (options) {
    setTimeout(function () {
        this.connect(options, function (err) {
            err && this.handleDisconnect(options);
        }.bind(this));
    }.bind(this), 2000);
}

MySql.prototype.query = function (sql, done) {
    this.connection.query(sql, function (err, rows) {
        if (err) return done(err);
        done(null, rows);
    });
}

MySql.prototype.getColumnsInfo = function (data) {
    var columns = {};
    for (var key in data) {
        var column = data[key];
        columns[column.Field] = {
            type: column.Type,
            allowNull: column.Null === 'YES' ? true : false,
            key: column.Key.toLowerCase(),
            defaultValue: column.Default
            // extra: column.Extra
        };
    }
    return columns;
}


/**
 * PostgreSql: https://github.com/brianc/node-postgres
 * or: https://github.com/brianc/node-postgres-pure
 */

function PostgreSQL () {
    try {this.client = require('pg')}
    catch (err) {
        try {this.client = require('pg.js')}
        catch (err) {
            throw Error('Could not find `pg` or `pg.js` module');
        }
    }
    this.connection = null;
    this.config = null;

    this.pg = true;
    this.name = 'pg';
}

PostgreSQL.prototype.connect = function (options, done) {
    this.connection = new this.client.Client(options);
    this.connection.connect(function (err) {
        if (err) return done(err);
        this.config = this.connection.connectionParameters;
        this.config.schema = options.schema || 'public';
        done();
    }.bind(this));
}

PostgreSQL.prototype.query = function (sql, done) {
    this.connection.query(sql, function (err, result) {
        if (err) return done(err);
        if (result.command == 'INSERT' && result.rows.length) {
            var obj = result.rows[0],
                key = Object.keys(obj)[0];
            result.insertId = obj[key];
            return done(null, result);
        }
        // select
        done(null, result.rows);
    });
}

function getType (column) {
    switch (true) {
        case /^double precision$/.test(column.Type):
            return 'double';

        case /^numeric$/.test(column.Type):
            return column.numeric_precision
                ? 'decimal('+column.numeric_precision+','+column.numeric_scale+')'
                : 'decimal'
        
        case /^time\s.*/.test(column.Type):
            return 'time';	
        case /^timestamp\s.*/.test(column.Type):
            return 'timestamp';

        case /^bit$/.test(column.Type):
            return 'bit('+column.character_maximum_length+')';
        case /^character$/.test(column.Type):
            return 'char('+column.character_maximum_length+')';
        case /^character varying$/.test(column.Type):
            return 'varchar('+column.character_maximum_length+')';

        case /^boolean$/.test(column.Type):
            return 'char';

        default: return column.Type;
    }
}
PostgreSQL.prototype.getColumnsInfo = function (data) {
    var columns = {};
    for (var key in data) {
        var column = data[key];
        columns[column.Field] = {
            type: getType(column),
            allowNull: column.Null === 'YES' ? true : false,
            key: (column.Key && column.Key.slice(0,3).toLowerCase()) || '',
            defaultValue: column.Default && column.Default.indexOf('nextval')==0 ? null : column.Default
            // extra: column.Extra
        };
    }
    return columns;
}


/**
 * Sqlite: https://github.com/mapbox/node-sqlite3
 */

function SQLite () {
    try {
        this.client = require('sqlite3');
    } catch (err) {
        throw new Error('Could not find `sqlite3` module');
    }
    this.connection = null;
    this.config = null;

    this.sqlite = true;
    this.name = 'sqlite';
}

SQLite.prototype.connect = function (options, done) {
    this.connection = new this.client.Database(options.database);
    this.config = {schema:''};
    done();
}

SQLite.prototype.query = function (sql, done) {
    if (/^(insert|update|delete)/i.test(sql)) {
        this.connection.run(sql, function (err) {
            if (err) return done(err);
            done(null, {insertId: this.lastID});
        });
    } else {
        this.connection.all(sql, function (err, rows) {
            if (err) return done(err);
            done(null, rows);
        });
    }
}

SQLite.prototype.getColumnsInfo = function (data) {
    var columns = {};
    for (var i=0; i < data.length; i++) {
        var column = data[i];
        columns[column.name] = {
            type: column.system_type_id,
            allowNull: column.is_nullable === 1 ? true : false,
            key: column.isprimary === 1 ? 'pri' : '',
            defaultValue: null
        };
    }
    return columns;
}



/**
 * MsSql: https://github.com/patriksimek/node-mssql
 */

function MsSql () {
    try {
        this.client = require('mssql');
    } catch (err) {
        throw new Error('Could not find `mssql` module');
    }
    this.connection = null;
    this.config = null;

    this.mssql = true;
    this.name = 'mssql';
}

MsSql.prototype.connect = function (options, done) {
    this.config = {schema:''};
    var connStr = "mssql://" + options.user + ":" + options.password + "@" + options.database;
    this.connection = this.client.connect(connStr)
        .then(function(){
            done();
        })
        .catch(function(){
        });
}

MsSql.prototype.query = function (sql, done) {
    new this.client.Request().query(sql).then(function(recordset) {
        done(null, recordset);
    })
    // .catch(function(err) {
    //     console.log("Error in query", sql, err);
    //     // ... query error checks
    // })
    ;    
}
var getTypeForMsSqlTypeName = function(column){
    switch (true) {
        case /^numeric$/.test(column.TypeName):
            return column.precision
                ? 'decimal('+column.precision+','+column.max_length+')'
                : 'decimal'
        case /^bit$/.test(column.TypeName):
            return 'bit('+column.max_length+')';
        case /^n?char$/.test(column.TypeName):
            return 'char('+column.max_length+')';
        case /^n?varchar$/.test(column.TypeName):
            return 'varchar('+column.max_length+')';

        default: return column.TypeName;
    }
}
MsSql.prototype.getColumnsInfo = function (data) {
    var columns = {};
    for (var i=0; i < data.length; i++) {
        var column = data[i];
        columns[column.name] = {
            type: getTypeForMsSqlTypeName(column),
            allowNull: (column.is_nullable === 1),
            key: column.isprimary ? 'pri' : '',
            defaultValue: null
        };
    }
    return columns;
}


/**
 * Factory
 */

function Client (config) {
    if (config.mysql)
        return new MySql();

    if (config.pg)
        return new PostgreSQL();

    if (config.sqlite)
        return new SQLite();

    if (config.mssql)
        return new MsSql();

    else throw new Error('Not supported database type!');
}


exports = module.exports = Client;
