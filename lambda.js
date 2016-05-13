'use strict';
console.log('Loading EmailLog_Rotator::');
console.log('Version 0.1');

var aws = require('aws-sdk');
var ddb = new aws.DynamoDB();

var d = new Date();
var YYYY = d.getFullYear().toString();
var MM = ("0" + (d.getMonth()+1)).slice(-2).toString();
var TableName = "EmailLog-"+YYYY+MM;

exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));
    console.log("TableName: "+TableName);
    callback(null, event.key1);  // Echo back the first key value
    // callback('Something went wrong');

};
