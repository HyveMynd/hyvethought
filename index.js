/**
 * Created by HyveMynd on 7/22/14.
 */
var async = require('async');
var _ = require('underscore')._;
var r = require('rethinkdb');
var assert = require('assert');
var Table = require("./lib/table");


var Revision = function () {
    var config = {};

    var setConfig = function (args) {
        assert.ok(args.db, "Db must be specified");
        config.db = args.db;
        config.host = args.host || 'localhost';
        config.port = args.port || 28015;
    };

    /**
     * Connects to the rethinkdb.
     * @param args host, port, db (only db is required. defaults to localhost:28015
     * @param next the rethinkdb object. Allows for fluent calls
     */
    this.connect = function (args, next) {
        setConfig(args);
        r.connect(config, function (err, conn) {
            assert.ok(err === null, err);
            r.db(config.db).tableList().run(conn, function (err, tables) {
                if (!err){
                    _.each(tables, function (table) {
                        this[table] = new Table(config, table);
                    });
                }
                next(err, this);
            });
        });
    };

    /**
     * Open a new connection. Must have already called connect(args, next).
     * @param next
     */
    this.openConnection = function (next) {
        r.connect(config, next);
    };

    /**
     * Create a new database with the given name.
     * @param name name of the new database
     * @param next (err, result) result is {created: 1} if successful
     */
    this.createDb = function (name, next) {
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
    this.dropDb = function (name, next) {
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
    this.createTable = function(tableName, callback){
        this.openConnection(function(err,conn){
            assert.ok(err === null,err);
            r.tableCreate(tableName).run(conn,function(err,result){
                conn.close();
                callback(err, result);
            });
        });
    };

    /**
     * Find whether the given table name exists in the database
     * @param tableName the table to find
     * @param callback (err, result) result is true if the table exists
     */
    this.tableExists = function(tableName, callback){
        this.openConnection(function(err,conn){
            assert.ok(err === null,err);
            r.tableList().run(conn,function(err,tables){
                assert.ok(err === null,err);
                conn.close();
                callback(null, _.contains(tables,tableName));
            });
        });
    };

    /**
     * Find whether the given database exists
     * @param dbName the database to find
     * @param next (err, result) result is true if the table exists
     */
    this.dbExists = function(dbName, next){
        this.openConnection(function(err,conn){
            assert.ok(err === null,err);
            r.dbList().run(conn,function(err,dbs){
                assert.ok(err === null,err);
                conn.close();
                next(null, _.contains(dbs,dbName));
            });
        });
    };

    /**
     * Create a database and list of tables
     * @param tables the names of the tables to create
     * @param config config options for the database
     * @param next
     */
    this.install = function(config, tables, next){
        assert.ok(tables && tables.length > 0, "Be sure to set the tables array on the config");
        this.createDb(config.db, function(err,result){
            assert.ok(err === null, err);
            async.each(tables, this.createTable, function(err) {
                assert.ok(err === null,err);
                next(err,err===null);
            });
        });
    };

    return this;
};

module.exports = new Revision();