// handler.js

/*
WORKFLOW
1. Convert the formData object sent from the page into a JSON object.
2. Check if the honeypot field has been submitted, if so fail silently.
3. Check if there in no MDN/Campaign value present and verify that a Company/Shortcode value is present
3a. Run pullFiles if so.
3b. If a MDN/Campaign value is present run pullRecords.
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

  // Check here to see if MDN and Campaign are both missing
  // If so, run the file pull for company and send the message to the page stating that is happening
  if (!formData.mdn && !formData.campaign && !formData.message_type && !formData.message_uid && (formData.company || formData.shortcode)) {
    replyPage(200, "Files for " + formData.company + formData.shortcode + " are being retrieved", callback);
    
    pullFiles(formData);
  } else if (formData.mdn || formData.campaign || formData.message_type || formData.message_uid) {
    replyPage(200, "Messages for " + formData.mdn + formData.campaign + formData.message_type + formData.message_uid + " are being retrieved", callback);
    
    pullRecords(formData);
  } else {
    replyPage(502, "The proper information was not supplied", callback)
  };
};


function pullFiles(formData) {
  getFilelist(function(fileList) {
    var listedFiles = getFilename(formData,fileList);

    formData.bucket = "archive";

    for (var i = 0; i < listedFiles.length; i++) {

      // Pull the contents file from the bucket and convert it to an array
      formData.key = listedFiles[i];

      // An object of options to indicate where to post to
      var post_options = {
        hostname: 's3bucket.amazonaws.com',
        port: 443,
        path: '/endpoint',
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
  formData.bucket = "company.archive";

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
        hostname: 's3bucket.amazonaws.com',
        port: 443,
        path: '/endpoint',
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

  if (formData.mdn) {
    // Build the query based off of the phone number
    query = `SELECT s._1, s._2, s._3, s._4, s._6, s._7, s._9, s._11, s._13, s._30, s._32 FROM s3Object s WHERE s._13 IN ('${formData.mdn}')`;
    
    // Add the query based off of the campaign number
    if (formData.campaign) {
      query = query + ` AND s._30 IN ('${formData.campaign}')`;
    }
    // Check for results in the appropriate date range
    if (formData.start_date && formData.end_date) {
      query = query + ` AND s._9 BETWEEN '${formData.start_date}' AND '${formData.end_date}'`;      
    }
    // Check for a message type other than all
    if (formData.message_type) {
      query = query + ` AND s._7 = '${formData.message_type}'`;
    }
    if (formData.message_uid) {
      query = query + ` AND s_1 = '${formData.message_uid}'`;
    }
  } else if (formData.campaign) {
    // Build the query based off of the campaign number
    query = `SELECT s._1, s._2, s._3, s._4, s._6, s._7, s._9, s._11, s._13, s._30, s._32 FROM s3Object s WHERE s._30 IN ('${formData.campaign}')`;

    // Check for a message type other than all
    if (formData.message_type) {
      query = query + ` AND s._7 = '${formData.message_type}'`;
    }
    if (formData.message_uid) {
      query = query + ` AND s_1 = '${formData.message_uid}'`;
    }
    // Check for results in the appropriate date range
    if (formData.start_date && formData.end_date) {
      query = query + ` AND s._9 BETWEEN '${formData.start_date}' AND '${formData.end_date}'`;      
    }
  } else if (formData.message_type) {
    // Check for a message type other than all
    query = `SELECT s._1, s._2, s._3, s._4, s._6, s._7, s._9, s._11, s._13, s._30, s._32 FROM s3Object s WHERE s._7 = '${formData.message_type}'`;

    // Check for results in the appropriate date range
    if (formData.start_date && formData.end_date) {
      query = query + ` AND s._9 BETWEEN '${formData.start_date}' AND '${formData.end_date}'`;      
    }
    if (formData.message_uid) {
      query = query + ` AND s_1 = '${formData.message_uid}'`;
    }
  } else if (formData.message_uid) {
    // Build the query based off of the message uid
    query = `SELECT s._1, s._2, s._3, s._4, s._6, s._7, s._9, s._11, s._13, s._30, s._32 FROM s3Object s WHERE s._1 = '${formData.message_uid}'`;
    // Check for results in the appropriate date range
    if (formData.start_date && formData.end_date) {
      query = query + ` AND s._9 BETWEEN '${formData.start_date}' AND '${formData.end_date}'`;      
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
    Bucket: "company.messagelogs",
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
    if (formData.shortcode) {    
      if ( fileList[i].indexOf(formData.company) > -1 || fileList[i].indexOf(formData.shortcode) > -1 ) {
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
      if (fileList[i].includes(formData.company)) {
        if (formData.start_date && formData.end_date) {
          var fileDate = new Date(fileList[i].slice(13,20));
        
          // Pull the date from the file
          if (fileDate >= minDate && fileDate <= maxDate) {
            console.log("Success");
            fileName.push(fileList[i]);
          };
        } else {
          fileName.push(fileList[i]);
        };
      };
    };
  };

  sendFilelist(fileName, formData);
  return fileName;
};


function sendMail(formData, file, fileName) {

  var request;

  // Check the filled out fields and use that for the subject line
  if (formData.mdn) {
    request = formData.mdn;
  } else if (formData.company) {
    request = formData.company;
  } else if (formData.shortcode) {
    request = formData.shortcode;
  } else if (formData.campaign) {
    request = formData.campaign
  }

  // send as email attachment
  var ses_mail = "From: DR <dr@company.com>\n";
  ses_mail = ses_mail + "To: " + formData.email + "\n";
  ses_mail = ses_mail + "Subject: company Data Request for " + request + "\n";
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
      formData.email
    ],
    Source: `dr@company.com`
  };
  
  SES.sendRawEmail(params, function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else     console.log(data);           // successful response
  });
};

function sendFilelist(fileList, formData) {

  const emailParams = {
    Source: 'dr@company.com', // SES SENDING EMAIL
    ReplyToAddresses: ["dr@company.com"],
    Destination: {
      ToAddresses: [formData.email]
    },
    Message: {
      Body: {
        Text: {
          Charset: 'UTF-8',
          Data: `The below files should be sent via individual emails shortly:\n${fileList.join('\n')}`
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
