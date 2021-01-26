var JDBC = require('jdbc');
var jinst = require('jdbc/lib/jinst');
var asyncjs = require('async');
var util = require('util');

//create a jvm and specify the jars required in the classpath and other jvm parameters
if (!jinst.isJvmCreated()) {
    jinst.addOption("-Xrs");
    jinst.setupClasspath(['./lib/hive-jdbc-1.2.0-mapr-1601-standalone.jar',
        './lib/hadoop-common-2.7.0-mapr-1607.jar']);
}

//read the input arguments

var server = "localhost";

var port = 1000

var schema = "foodmart";

var username = 'hive';

var password = '';

//specify the hive connection parameters

var conf = {
    //jdbc:hive2://localhost:10000
    url: `jdbc:hive2://localhost:10000/foodmart`,
    drivername: 'org.apache.hive.jdbc.HiveDriver',
    properties: {
    }
};

var hive = new JDBC(conf);

hive.initialize(function (err) {
    if (err) {
        console.log("hubo un error")
    }
    console.log("Connection run")
});

hive.reserve(function (err, connObj) {
    if (connObj) {
        console.log("Connection : " + connObj.uuid);
        let conn = connObj.conn;
        asyncjs.series([
            function (callback) {
                conn.createStatement(function (err, statement) {
                    if (err) {
                        callback(err);
                    } else {
                        console.log("Executing query.");
                        statement.executeQuery("SELECT * FROM employee LIMIT 10",
                            function (err, resultset) {
                                if (err) {
                                    console.log(err);
                                    callback(err);
                                } else {
                                    console.log("Query Output :")
                                    resultset.toObjArray(function (err, result) {
                                        if (result.length > 0) {
                                            console.log("Employee :" + util.inspect(result));
                                        }
                                        callback(null, resultset);
                                    })
                                }
                            });
                    }
                })
            }
        ])
    }
});