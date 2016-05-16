'use strict';
console.log('Loading EmailLog_Rotator::');
console.log('Version 0.1');

var aws = require('aws-sdk');
var ddb = new aws.DynamoDB();

var rtn = false;
var d = new Date();
var YYYY = d.getFullYear().toString();
var MM = ("0" + (d.getMonth()+2)).slice(-2).toString(); //+1 would give the current month. +2 returns next month.
var TableName = "EmailLog-"+YYYY+MM;

exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));
    console.log("TableName: "+TableName);       //DEBUG
    var params = {
        TableName: TableName,
        KeySchema: [
            { AttributeName: "datetime", KeyType: "HASH"} //Partition Key
        ],
        AttributeDefinitions: [
            { AttributeName: "datetime", AttributeType: "S" }
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 5,
            WriteCapacityUnits: 5
        }
    };
    console.log("params:: "+params);        //DEBUG
    ddb.createTable(params, function(err, data) {
       if (err) {
           console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
           context.fail('Error creating table:'+err+err.stack); //An error occurred creating table.
       } else {
           rtn=true;
           console.log("Created table. Table description JSON:", JSON.stringify(data, null, 2));
           context.succeed("true"); //Table successfully created.
       }
    });
    callback(null, event.key1);  // Echo back the first key value
    // callback('Something went wrong');

};
