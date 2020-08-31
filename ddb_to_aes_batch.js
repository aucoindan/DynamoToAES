const AWS = require('aws-sdk');
const logger = require('./../lib/logger');

const docClient = new AWS.DynamoDB.DocumentClient();
const lambda = new AWS.Lambda();

/**
 * Sync the whole DynamodDB Requests table
 * with ES Service. Iterates the table by 1mb batches.
 *
 * @param {Object} event
 * @param {Object} context
 * @param {Function} callback
 * @returns {void}
 */
function syncHandler(event, context, callback) {
  logger('syncHandler', 'Syncing DynamoDB with ES', event);

  const params = {
    TableName: process.env.REQUESTS_TABLE,
  };

  docClient.scan(params, (err, data) => onScan(err, data, params, callback));
}

/**
 *
 * @private
 * @param {Error} err
 * @param {Object} data
 * @param {Object} params
 * @param {Function} callback
 * @returns {void}
 */
function onScan(err, data, params, callback) {
  if (err) {
    logger('onScan', 'Error: While syncing DynamoDB with ES', err);
    callback({
      success: false,
      response: null,
      error: {
        message: err.message,
        type: 'Error',
      },
    });
  } else {
    /* This is where I call another Lambda function that inserts a batch of documents to ES */
    invokeIndexFunction(data.Items) 
      .then(() => {
        if (typeof data.LastEvaluatedKey != 'undefined') {
          logger('onScan', 'Done syncing page, moving to the next.');
          params.ExclusiveStartKey = data.LastEvaluatedKey;
          return docClient.scan(params, (err, data) => onScan(err, data, params, callback));
        } else {
          logger('onScan', 'Syncing finished');
          return callback(null, {
            success: true,
          });
        }
      })
      .catch(err =>
        callback(null, {
          success: false,
          response: null,
          error: {
            message: err.message,
            type: 'Error',
          },
        })
      );
  }
}
