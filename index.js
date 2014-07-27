/**
 * Created by HyveMynd on 7/22/14.
 */
var async = require('async');
var _ = require('underscore')._;
var r = require('rethinkdb');
var assert = require('assert');
var Table = require("./lib/table");


var Revision = function (args) {
    var config = args;
    var self = this;

    /**
     * Open a new connection. Must have already called connect(args, next).
     * @param next
     */
    self.openConnection = function (next) {
        r.connect(config, next);
    };

    /**
     * Create a new database with the given name.
     * @param name name of the new database
     * @param next (err, result) result is {created: 1} if successful
     */
    self.createDb = function (name, next) {
        r.connect({host: config.host, port: config.port}, function (err, conn) {
            assert.ok(err === null, err);
            r.dbCreate(name).run(conn, function (err, result) {
                conn.close();
                next(err, result);
            });
        });
    };

    /**
     * Drop the database with the given name.
     * @param name the database to drop
     * @param next (err, result) result is {dropped:1} is successful
     */
    self.dropDb = function (name, next) {
        r.connect({host: config.host, port: config.port}, function (err, conn) {
            assert.ok(err === null, err);
            r.dbDrop(name).run(conn, function (err, result) {
                conn.close();
                next(err, result);
            });
        });
    };

    /**
     * Create a table with the given name.
     * @param tableName the table to create
     * @param callback (err, result) result contains {created:1} if successful
     */
    self.createTable = function(tableName, callback){
        r.connect(config, function(err,conn){
            assert.ok(err === null, err);
            r.tableCreate(tableName).run(conn, function(err,result){
                conn.close();
                callback(err, result);
            });
        });
    };

    /**
     * Find whether the given table name exists in the database
     * @param tableName the table to find
     * @param next (err, result) result is true if the table exists
     */
    self.tableExists = function(tableName, next){
        r.connect(config, function(err,conn){
            assert.ok(err === null, err);
            r.tableList().run(conn,function(err, tables){
                assert.ok(err === null, err);
                conn.close();
                next(null, _.contains(tables,tableName));
            });
        });
    };

    /**
     * Find whether the given database exists
     * @param dbName the database to find
     * @param next (err, result) result is true if the table exists
     */
    self.dbExists = function(dbName, next){
        r.connect(config, function(err,conn){
            assert.ok(err === null, err);
            r.dbList().run(conn,function(err, dbs){
                assert.ok(err === null, err);
                conn.close();
                next(null, _.contains(dbs,dbName));
            });
        });
    };

    /**
     * Create a database and list of tables
     * @param tables the names of the tables to create
     * @param next
     */
    self.install = function(tables, next){
        assert.ok(tables && tables.length > 0, "Be sure to set the tables array on the config");
        self.createDb(config.db, function(err,result){
            async.each(tables, self.createTable, function(err) {
                next(err, err === null);
            });
        });
    };

    return this;
};

var Connector = function () {
    var config = {};
    var self = this;

    var setConfig = function (args) {
        assert.ok(args.db, "Db must be specified");
        config.db = args.db;
        config.host = args.host || 'localhost';
        config.port = args.port || 28015;
    };

    /**
     * Connects to the rethinkdb. Called first
     * @param args host, port, db (only db is required. defaults to localhost:28015
     * @param next the rethinkdb object. Allows for fluent calls
     */
    self.connect = function (args, next) {
        setConfig(args);
        r.connect(config, function (err, conn) {
            assert.ok(err === null, err);
            r.db(config.db).tableList().run(conn, function (err, tables) {
                if (!err){
                    _.each(tables, function (table) {
                        this[table] = new Table(config, table);
                    });
                    next(null, new Revision(config));
                } else {
                    next(err);
                }
            });
        });
    };
};

module.exports = new Connector();