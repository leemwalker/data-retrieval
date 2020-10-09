// puller.js


'use strict';

const AWS       = require('aws-sdk');
const SES       = new AWS.SES();
const S3        = new AWS.S3();

module.exports.pullFile = (event, context, callback) => {
  const formData = JSON.parse(event.body);

  const f_params = {
    Bucket: formData.bucket,
    Key: formData.key
  };
    //Make this a separate function to avoid memory/timeout issues
  S3.headObject(f_params, (err, data) => {
    if (err) throw err;
    if (data.ContentLength < 9300000) {
      S3.getObject(f_params, (err, data) => {
        if (err) throw err;
        // The file is already zipped and in a csv format
        var file = data.Body.toString('base64');
        sendRawMail(formData, file, formData.key);
        replyPage(200, "Files for " + formData.company + " have been sent", callback);
      });
    } else {
      var params = {
        Bucket: 'archive',
        Key: formData.company + '/' + formData.key,
        CopySource: formData.bucket + '/' + formData.key
      };
      S3.copyObject(params, (err, data) => {
        if (err) throw err;
        var location = 'https://s3.location.amazonaws.com/' + formData.company + '/' + formData.key;
        sendMail(formData, location)
        replyPage(200, "Files for " + formData.company + " have been uploaded", callback);
      });
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

function sendRawMail(formData, file, fileName) {
  // send as email attachment
  var ses_mail = "From: DR <dr@company.com>\n";
  ses_mail = ses_mail + "To: " + formData.email + "\n";
  ses_mail = ses_mail + "Subject: Data Request for " + formData.company + "\n";
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
    replyPage 
  });
};

function sendMail(formData, bucket, callback) {
  const emailParams = {
    Source: 'dr@company.com', // SES SENDING EMAIL
    ReplyToAddresses: ["dr@company.com"],
    Destination: {
      ToAddresses: [formData.email], // SES RECEIVING EMAIL
    },
    Message: {
      Body: {
        Html: {
          Charset: 'UTF-8',
          Data: `\n<p>Please look in: ${bucket}</p>\n`
        },
        Text: {
          Charset: 'UTF-8',
          Data: `\nPlease look in: ${bucket}\n`
        }
      },
      Subject: {
        Charset: 'UTF-8',
        Data: `Data Retrieval For ${formData.company}`,
      }
    }
  };
  SES.sendEmail(emailParams,  function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else     console.log(data);           // successful response
  });
}
