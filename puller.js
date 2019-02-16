// puller.js


'use strict';

const AWS       = require('aws-sdk');
const SES       = new AWS.SES();
const S3        = new AWS.S3();

module.exports.pullFile = (event, context, callback) => {
  const formData = JSON.parse(event.body);
  var params = {
  	Bucket: 'dataretrievallogs',
  	Key: formData.business + '/' + formData.key,
  	CopySource: formData.bucket + '/' + formData.key
  };
  console.log(formData.bucket);
  console.log(formData.key);
  
  S3.copyObject(params, (err, data) => {
  	if (err) throw err;
  	var location = 'https://s3location.amazonaws.com/' + formData.business + '/' + formData.key;
  	sendMail(formData, location)
     replyPage(200, "Files for " + formData.business + " have been uploaded", callback);
  });
};

function replyPage(status, message, callback) {
  const response = {
    statusCode: status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Credentials": true
    },
    body: message
  };

  // Send the response back to the page
  callback(null, response);
  console.log("Response to page has been sent.")
};

function sendMail(formData, bucket, callback) {
  const emailParams = {
    Source: 'data.handling@business.com', // SES SENDING EMAIL
    ReplyToAddresses: ["data.handling@business.com"],
    Destination: {
      ToAddresses: ["data.handling@business.com"], // SES RECEIVING EMAIL
    },
    Message: {
      Body: {
        Html: {
          Charset: 'UTF-8',
          Data: `\n<p>File: ${formData.key} was uploaded. Please find it in: ${bucket}</p>\n`
        },
        Text: {
          Charset: 'UTF-8',
          Data: `\nFile: ${formData.key} was uploaded. Please find it in: ${bucket}\n`
        }
      },
      Subject: {
        Charset: 'UTF-8',
        Data: `Data Retrieval For ${formData.business}`,
      }
    }
  };
  SES.sendEmail(emailParams,  function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else     console.log(data);           // successful response
  });
}
