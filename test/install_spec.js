/**
 * Created by Andres Monroy (HyveMynd) on 7/27/14.
 */

var db = require('../index');
var _ = require('underscore')._;
var assert = require('assert');
var should = require('should');

describe('Installer', function () {

    beforeEach(function (done) {
        db.connect({ db: 'test'}, function (err, db) {
            db.dropDb('test', function (err, result) {
                done(err);
            });
        });
    });

    describe("creates database", function(){

        beforeEach(function (done) {
            db.createDb('test', function (err, result) {
                done(err);
            });
        });

        it("creates the test db", function(){
            db.dbExists('test', function (err, result) {
                result.should.equal(true);
            });
        });
        
    });

    describe("functional installer", function(){

        beforeEach(function(done){
            db.install(['foo','bar'], function (err, result) {
                done();
            });
        });

        it("creates the test database", function(){
            db.dbExists('test', function (err, result) {
                result.should.equal(true);
            });
        });

        it("creates the foo table", function(){
            db.tableExists('foo', function (err, result) {
                result.should.equal(true);
            });
        });

        it("creates the bar table", function(){
            db.tableExists('bar', function (err, result) {
                result.should.equal(true);
            });
        });

    });

});
