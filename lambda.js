'use strict';
console.log('Loading EmailLog_Rotator::');
console.log('Version 0.5');

var aws = require('aws-sdk');
var ddb = new aws.DynamoDB();

// Keep track of function completion
var created = false, updated = false, deleted = false;

exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));   //DEBUG

    //Make sure month is always two digits 01-12
    function padMonth(month,cb) {
      if(!Number.isInteger(month)) {  //Mon is required and must be an Integer
        if(typeof cb === 'function' && cb("Error: No month specified.", null));
        return false;
      } else {
        month = ("0" + (month).toString()).slice(-2);
        if(typeof cb === 'function' && cb(null, month));
        return month;
      }
    };

    // Checks if specified table exists
    function tableExists(tableName, cb) {
      if(!tableName) { //tableName is required.
        if(typeof cb === 'function' && cb("Error, tableName is required.", null));
        return false;
      } else {
        ddb.listTables({}, function(err, data) {
          if(err) {
            console.error("Unable to list tables. Error JSON:", JSON.stringify(err, null, 2));
            context.fail('Error listing tables:'+err+err.stack);
          } else {
            if(data.TableNames.indexOf(tableName) == -1) {
              console.log("Table not found: "+tableName);   //DEBUG
              cb(false);
            } else {
              console.log("Table found: "+tableName);   //DEBUG
              cb(true);
            }
          }
        }); //End ddb.listTables
      }
    }; //End tableExists

    // Check if specified table is currently provisioned as r1w1
    function tableDowngraded(tableName, cb) {
      if(!tableName) { //tableName is required
        if(typeof cb === 'function' && cb("Error: tableName is required.", null));
        return false;
      } else {
        tableExists(tableName, function(result) {
          if(result==true) {  //Table exists
            ddb.describeTable({TableName: tableName}, function(err, data) {
              if (err) {
                console.error("Unable to retrieve table description. Error JSON: ",JSON.stringify(err, null, 2));
                context.fail('Error describing table:'+err+err.stack);
              } else {
                if((data.Table.ProvisionedThroughput.ReadCapacityUnits != 1) || (data.Table.ProvisionedThroughput.WriteCapacityUnits != 1)) {
                  cb(true);
                } else {
                  console.log("tableName: "+tableName+" already provisioned r1w1.");
                  cb(false);
                }
              }
            }); //End describeTable
          } else {  // Table does not exist
            cb(false);
          }
        }); //End tableExists
      }
    }; //End tableDowngraded

    // Create the specified table
    function createTable(tableName, cb) {
      //console.log("Begin createTable("+tableName+")");   //DEBUG
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
                  ReadCapacityUnits: 2,
                  WriteCapacityUnits: 5
              }
          };
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
                  cb();
                }
              }); //End waitFor
            }
          }); //End ddb.createTable
        } else {  //Table already exists.
          created = true;
          console.error("createTable()::Table already exists.");
          cb();
        }
      }); //End tableExists
    }; //End createTable

    // Downgrade throughput for specified table
    function downgradeTable(tableName, cb) {
//      console.log("Begin downgradeTable("+tableName+")"); //DEBUG
      tableDowngraded(tableName, function(result) {
        if(result==true) { //Table exists
          var params = {
            TableName: tableName,
            ProvisionedThroughput: {
              ReadCapacityUnits: 1,
              WriteCapacityUnits: 1
            }
          };
          ddb.updateTable(params, function(err, data) {
            if (err) {
              console.error("Unable to update table. Error JSON:", JSON.stringify(err, null, 2));
              context.fail('Error updating table:'+err+err.stack); //An error occurred updating the table.
            } else {
              ddb.waitFor('tableExists', {TableName: tableName}, function(err, data) {
                if (err) {
                  console.log("Table "+tableName+" not updated.");
                  console.log(err, err.stack);
                  context.fail();
                } else {
                  updated = true;
                  console.log("Table "+tableName+" updated.");
                  cb();
                }
              });//End waitFor
            }
          });//End ddb.updateTable
        } else { //Table does not exist.
          console.error("downgradeTable()::Table "+tableName+" does not exist or does not need downgrading.");
          updated = true;
          cb();
        }
      }); //End tableExists
    }; //End downgradeTable

    // Delete the specified table
    function deleteTable(tableName, cb) {
      //console.log("Begin deleteTable("+tableName+")");     //DEBUG
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
              ddb.waitFor('tableNotExists', {TableName: tableName}, function(err, data) {
                if (err) {
                  console.log("Table "+tableName+" not deleted.");
                  console.log(err, err.stack);
                  context.fail();
                } else {
                  deleted = true;
                  console.log("Table "+tableName+" deleted.");
                  cb();
                }
              }); //End waitFor
            }
          }); //End ddb.deleteTable
        } else { //Table does not exist.
          console.error("deleteTable()::Table does not exist.");
          deleted = true;
          cb();
        }
      }); //End tableExists
    }; //End deleteTable

    // Handle callbacks for create and delete functions and terminate Lambda function.
    function complete() {
      console.log("Begin complete():: created="+created+" updated="+updated+" deleted="+deleted); //DEBUG
      if(created && updated && deleted) {
        console.log("Job well done.");
        context.succeed();
      }
    }; //End complete


    //**************************************************************************
    //Main code begins here.
    //**************************************************************************

    // Generate the dates needed for Table names
    var tableNames = [];
    var d = new Date();
    var MM = d.getMonth()+1;  //+1 returns current month
    var YYYY = d.getFullYear();  //returns this year
    switch (MM) { //Handle beginning/end of year cases
      case 12:
        tableNames["new"] = "EmailLog-"+(YYYY+1).toString()+"01";
        tableNames["last"] = "EmailLog-"+(YYYY).toString()+padMonth(MM-1);
        tableNames["expired"] = "EmailLog-"+(YYYY-1).toString()+padMonth(MM);
        break;
      case 1:
        tableNames["new"] = "EmailLog-"+(YYYY).toString()+padMonth(MM+1);
        tableNames["last"] = "EmailLog-"+(YYYY-1).toString()+"12";
        tableNames["expired"] = "EmailLog-"+(YYYY-1).toString()+padMonth(MM);
        break;
      default:
        tableNames["new"] = "EmailLog-"+(YYYY).toString()+padMonth(MM+1);
        tableNames["last"] = "EmailLog-"+(YYYY).toString()+padMonth(MM-1);
        tableNames["expired"] = "EmailLog-"+(YYYY-1).toString()+padMonth(MM);
    }

    //Delete Table from 1 year ago
    deleteTable(tableNames["expired"], complete);
    //Downgrade last month's table's ProvisionedThroughput to r1 w1
    downgradeTable(tableNames["last"], complete)
    //Create next month's table with ProvisionedThroughput r5  w5
    createTable(tableNames["new"], complete);

};
