// handler.js

/*
WORKFLOW
1. Convert the formData object sent from the page into a JSON object.
2. Check if the honeypot field has been submitted, if so fail silently.
3. Check if there in no telephone/keyword value present and verify that a business/origincode value is present
3a. Run pullFiles if so.
3b. If a telephone/keyword value is present run pullRecords.
3c. If none of the above fail with a reply to the page stating so.

TODO
1. Create pullRecords, pullFiles, and sendMail as separate serverless functions
2. Accept comma separated lists in all fields except start_date and end_date
3. Email the file list result for pullFiles or display it on the page

*/


'use strict';

const AWS       = require('aws-sdk');
const SES       = new AWS.SES();
const S3        = new AWS.S3();
var   https     = require('https');
var   fs        = require('fs');
const zlib      = require('zlib');

module.exports.submitData = (event, context, callback) => {
  const formData = JSON.parse(event.body);

  // Return with no response if honeypot is present
  if (formData.honeypot) return;

  // Check here to see if telephone and keyword are both missing
  // If so, run the file pull for business and send the message to the page stating that is happening
  if (!formData.telephone && !formData.keyword && (formData.business || formData.origincode)) {
    replyPage(200, "Files for " + formData.business + formData.origincode + " are being retrieved", callback);
    
    pullFiles(formData);
  } else if (formData.telephone || formData.keyword) {
    replyPage(200, "Files for " + formData.telephone + " are being retrieved", callback);
    
    pullRecords(formData);
  } else {
    replyPage(502, "The proper information was not supplied", callback)
  };
};


function pullFiles(formData) {
  getFilelist(function(fileList) {
    var listedFiles = getFilename(formData,fileList);

    formData.bucket = "business.messagelogs";

    for (var i = 0; i < listedFiles.length; i++) {

      // Pull the contents file from the bucket and convert it to an array
      formData.key = listedFiles[i];

      console.log(formData.key);

      // An object of options to indicate where to post to
      var post_options = {
        hostname: 'serverlessendpoint.amazonaws.com',
        port: 443,
        path: '/production/pull-file',
        method: 'POST'
      };
    
      // Set up the request
      var post_req = https.request(post_options, function(res) {
        res.setEncoding('utf8');
        res.on('data', function (chunk) {
            console.log('Response: ' + chunk);
        });
      });

      // post the data
      post_req.write(JSON.stringify(formData));
      post_req.end();
    };
  });
};


function pullRecords(formData) {
  console.log("Pulling records");
  // Run the query crafting function
  formData.query = craftQuery(formData);
  formData.bucket = "business.messagelogs";

  // Run the function to pull the filelist
  getFilelist(function(fileList) {

    var fileName = getFilename(formData,fileList);

    console.log("Pulling " + fileName);
    // Iterate over the fileName array to run the S3 Select function against each file individually
    for (var i = 0; i < fileName.length; i++) {
      
      formData.key = fileName[i];
      console.log("Submitting " + formData.key + " for retrieval.")
      
      // An object of options to indicate where to post to
      var post_options = {
        hostname: 'serverlessendpoint.amazonaws.com',
        port: 443,
        path: '/production/pull-record',
        method: 'POST'
      };

      // Set up the request
      var post_req = https.request(post_options, function(res) {
        res.setEncoding('utf8');
        res.on('data', function (chunk) {
          console.log('Response: ' + chunk);
        });
      });

      // post the data
      post_req.write(JSON.stringify(formData));
      post_req.end();
    };
  });
};


// Craft the query here using a function
function craftQuery(formData) {
  var query;

  // Run a switch case statement here in the future per Sidra's advice

	if (formData.telephone) {
  	// Build the query based off of the phone number
  	query = `SELECT * FROM s3Object s WHERE s._13 IN ('${formData.telephone}')`;
  	
    // Add the query based off of the keyword number
    if (formData.keyword) {
  		query = query + ` AND s._30 IN ('${formData.keyword}')`;
  	}
    // Check for results in the appropriate date range
    if (formData.start_date && formData.end_date) {
      query = query + ` AND s._8 BETWEEN '${formData.start_date}' AND '${formData.end_date}'`;      
    }
  } else if (formData.keyword) {
  	// Build the query based off of the keyword number
  	query = `SELECT * FROM s3Object s WHERE s._30 IN ('${formData.keyword}')`;

    // Check for results in the appropriate date range
    if (formData.start_date && formData.end_date) {
      query = query + ` AND s._8 BETWEEN '${formData.start_date}' AND '${formData.end_date}'`;      
    }
  } else {
    // Error with missing information error
    console.log("Missing information to craft query");
  	return;
  }
  console.log(query);
  return query;
};


// Craft the filenames here using a function
function getFilelist(callback) {
  // Pull the contents file from the bucket and convert it to an array
  const fn_params = {
    Bucket: "business.messagelogs",
    Key: "fileList.txt"
  };

  S3.getObject(fn_params, (err, data) => {
    if (err) {
      console.log(err, err.stack); // an error occurred
      return;
    } else {
      //convert data.Body to an array
      var fileList = data.Body.toString('utf-8').split("\n")
      
      callback(fileList);
    };
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


function getFilename(formData, fileList) {
  var fileName = [];
  
  // Convert the start_date and end_date into Date objects for use later
  var minDate = new Date(formData.start_date);
  var maxDate = new Date(formData.end_date);

  for (var i = 0; i < fileList.length; i++) {
    if (formData.origincode) {    
      if ( fileList[i].indexOf(formData.business) > -1 || fileList[i].indexOf(formData.origincode) > -1 ) {
        if (formData.start_date && formData.end_date) {
          // Pull the date from the file
          var fileDate = new Date(fileList[i].slice(13,20));

          if (fileDate >= minDate && fileDate <= maxDate) {
            fileName.push(fileList[i]);
          };          
        } else {
          fileName.push(fileList[i]);
        };
      };
    } else {
      if (fileList[i].includes(formData.business)) {
        if (formData.start_date && formData.end_date) {
          var fileDate = new Date(fileList[i].slice(13,20));

          // Pull the date from the file
          if (fileDate >= minDate && fileDate <= maxDate) {
            console.log("Success");
            fileName.push(fileList[i]);
          };
        } else {
          console.log(fileList[i]);
          fileName.push(fileList[i]);
        };
      };
    };
  };

  sendFilelist(formData,fileName);
  return fileName;
};


function sendMail(formData, file, fileName) {

  var request;

  // Check the filled out fields and use that for the subject line
  if (formData.telephone) {
    request = formData.telephone;
  } else if (formData.business) {
    request = formData.business;
  } else if (formData.origincode) {
    request = formData.origincode;
  } else if (formData.keyword) {
    request = formData.keyword
  }

  // send as email attachment
  var ses_mail = "From: Data Handling <data.handling@business.com>\n";
  ses_mail = ses_mail + "To: end.user@business.com\n";
  ses_mail = ses_mail + "Subject: business Data Request for " + request + "\n";
  ses_mail = ses_mail + "MIME-Version: 1.0\n";
  ses_mail = ses_mail + "Content-Type: multipart/mixed; boundary=\"NextPart\"\n\n";
  ses_mail = ses_mail + "--NextPart\n";
  ses_mail = ses_mail + "Content-Type: text/html; charset=us-ascii\n";
  ses_mail = ses_mail + "\n<p>Please see your attached file.</p>\n";
  ses_mail = ses_mail + "--NextPart\n";    
  ses_mail = ses_mail + "Content-Type: application/zip; name=\"" + fileName + "\"\n";
  ses_mail = ses_mail + "Content-Description: " + fileName + "\n";
  ses_mail = ses_mail + "Content-Disposition: attachment;filename=\"" + fileName + "\";\n";
  ses_mail = ses_mail + "Content-Transfer-Encoding: base64\n\n" + file + "\n\n";
  ses_mail = ses_mail + "--NextPart--";
  
  const params = {
    RawMessage: {
      Data: new Buffer(ses_mail)
    },
    Destinations: [
      `end.user@business.com`
    ],
    Source: `data.handling@business.com`
  };
  
  SES.sendRawEmail(params, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else     console.log(data);           // successful response
  });
};

function sendFilelist(formData,fileList) {

  // For each item in fileList, prepend https://s3location.amazonaws.com/${formData.business}
  var prepend = 'https://s3location.amazonaws.com/' + formData.business + '/';

  const emailParams = {
    Source: 'data.handling@business.com', // SES SENDING EMAIL
    ReplyToAddresses: ["data.handling@business.com"],
    Destination: {
      ToAddresses: ["end.user@business.com"]
    },
    Message: {
      Body: {
        Text: {
          Charset: 'UTF-8',
          // Take the fileList and prepend each entry with the correct S3
          // Bucket information. 
          Data: `Your files are being copied to the following bucket: 'https://s3location.amazonaws.com/${formData.business}'
          \n\nPlease use the below links to download your files:\n\n${fileList.map(f => `${prepend}${f}`).join("\n")}`
        },
      },
      Subject: {
        Charset: 'UTF-8',
        Data: `File list per your latest request`
      },
    }
  };

  

  SES.sendEmail(emailParams, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else     console.log(data);           // successful response
  });
};


// References:
// https://docs.aws.amazon.com/AmazonS3/latest/production/SelectObjectContentUsingJava.html
// https://docs.aws.amazon.com/AmazonS3/latest/production/s3-glacier-select-sql-reference-select.html
// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectSELECTContent.html
