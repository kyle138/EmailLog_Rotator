'use strict';
console.log('Loading EmailLog_Rotator::');
console.log('Version 0.4');

var aws = require('aws-sdk');
var ddb = new aws.DynamoDB();

var d = new Date();
var newYYYY = d.getFullYear();
var expiredYYYY = d.getFullYear()-1;
var newMM = ("0" + (d.getMonth()+2)).slice(-2); //+2 returns next month.
var expiredMM = ("0" + (d.getMonth()+1)).slice(-2); //+1 returns current month.
var tableNames = [];
tableNames["new"] = "EmailLog-"+newYYYY.toString()+newMM.toString();
tableNames["expired"] = "EmailLog-"+expiredYYYY.toString()+expiredMM.toString();
var created = false;
var deleted = false;

exports.handler = (event, context, callback) => {
    console.log('Received event:', JSON.stringify(event, null, 2));   //DEBUG

    // Checks if specified table exists
    function tableExists(tableName, callback) {
      console.log("Begin tableExists("+tableName+")");   //DEBUG
      ddb.listTables({}, function(err, data) {
        if(err) {
          console.error("Unable to list tables. Error JSON:", JSON.stringify(err, null, 2));
          context.fail('Error listing tables:'+err+err.stack);
        } else {
          //console.log("tables: "+JSON.stringify(data));               //DEBUG
          if(data.TableNames.indexOf(tableName) == -1) {
            console.log("Table not found: "+tableName);   //DEBUG
            callback(false);
          } else {
            console.log("Table found: "+tableName);   //DEBUG
            callback(true);
          }
        }
      });
    }

    // Create the specified table
    function createTable(tableName, callback) {
      console.log("Begin createTable("+tableName+")");   //DEBUG
      tableExists(tableName, function(result) {
        if(result==false) {   //Table does not exist.
          var params = {
              TableName: tableName,
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
          //console.log("createTable params:: "+JSON.stringify(params));        //DEBUG
          ddb.createTable(params, function(err, data) {
             if (err) {
                 console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
                 context.fail('Error creating table:'+err+err.stack); //An error occurred creating table.
             } else {
                 //console.log("Created table. Table description JSON:", JSON.stringify(data, null, 2));
                 ddb.waitFor('tableExists', {TableName: tableName}, function(err, data) {
                   if (err) {
                     console.log("Table "+tableName+" not created.");
                     console.log(err, err.stack);
                     context.fail();
                   } else {
                     created = true;
                     console.log("Table "+tableName+" created.");
                     complete();
                   }
                 });
             }
          });
        } else {  //Table already exists.
          created = true;
          console.error("createTable()::Table already exists.");
          complete();
        }
      });
    }

    // Delete the specified table
    function deleteTable(tableName, callback) {
      console.log("Begin deleteTable("+tableName+")");     //DEBUG
      tableExists(tableName, function(result) {
        if(result==true) {   //Table exists.
          var params = {
            TableName: tableName
          };
          ddb.deleteTable(params, function(err, data) {
            if (err) {
              console.error("Unable to delete table. Error JSON:", JSON.stringify(err, null, 2));
              context.fail('Error deleting table:'+err+err.stack); //An error occurred creating table.
            } else {
              //console.log("Table deleted JSON:", JSON.stringify(data, null, 2));    //DEBUG
              ddb.waitFor('tableNotExists', {TableName: tableName}, function(err, data) {
                if (err) {
                  console.log("Table "+tableName+" not deleted.");
                  console.log(err, err.stack);
                  context.fail();
                } else {
                  deleted = true;
                  console.log("Table "+tableName+" deleted.");
                  complete();
                }
              });
            }
          });
        } else { //Table does not exist.
          console.error("deleteTable()::Table does not exist.");
          deleted = true;
          complete();
        }
      });
    }

    // Handle callbacks for create and delete functions and terminate Lambda function.
    function complete() {
      console.log("Begin complete():: created="+created+" deleted="+deleted); //DEBUG
      if(created && deleted) {
        console.log("Job well done.");
        context.succeed();
      }
    }

    //**************************************************************************
    //Main code begins here.
    deleteTable(tableNames["expired"], complete);
    createTable(tableNames["new"], complete);

    callback(null, event.key1);  // Echo back the first key value
    // callback('Something went wrong');

};
