// puller.js


'use strict';

const AWS       = require('aws-sdk');
const SES       = new AWS.SES();
const S3        = new AWS.S3();
const   fs        = require('fs');
const zlib      = require('zlib');

module.exports.pullRecord = (event, context, callback) => {
  const formData = JSON.parse(event.body);

  const params = {
    Bucket: formData.bucket,
    Key: formData.key,
    ExpressionType: 'SQL',
    Expression: formData.query,
    InputSerialization: {
      CSV: {
        AllowQuotedRecordDelimiter: true,
        FieldDelimiter: ',',
        FileHeaderInfo: 'NONE',
        RecordDelimiter: '\n',
        QuoteCharacter: '"',
        QuoteEscapeCharacter: '\\'
      },
      CompressionType: 'GZIP'
    },
    OutputSerialization: {
      CSV: {}
    }
  };
    
  S3.selectObjectContent(params, (err, data) => {
    
    if (err) throw err;
  
    // data.Payload is a Readable Stream
    const eventStream = data.Payload;
    var results = [];
    var sizeCheck;

    // Read events as they are available
    eventStream.on('data', (event) => {
      // Check the top-level field to determine which event this is.
      if (event.Records) {
        // event.Records.Payload is a buffer containing
        // a single record, partial records, or multiple records
        // process.stdout.write(event.Records.Payload.toString());
        results.push(event.Records.Payload.toString());
      } else if (event.Stats) {
        // handle Stats event
        console.log(`Processed ${event.Stats.Details.BytesProcessed} bytes`);

      } else if (event.End) {
        // handle End event
        console.log('SelectObjectContent completed');

        // Check if there are actual results or move on
        if (results == "") {
          console.log("No records matched in this file.");
          sendMail(formData);
        } else {
          
          // Convert the results array into a single object
          var outCSV = results.join('\n');

          // Zip the outCSV object
          zlib.gzip(outCSV, function (error, result) {
            if (error) throw error;
      
            const gzipCSV = result; 
          
            // Write the zipped object to a file in the tmp directory
            fs.writeFile('/tmp/results.csv.gz', gzipCSV, function (err) {
              if (err) {
                return console.log(err);
              } else {

                // Verify the size of the file isn't greater than 10MB
                const stats = fs.statSync('/tmp/results.csv.gz');
                const fileSizeInBytes = stats.size;
                const fileSizeInMegabytes = fileSizeInBytes / 1000000.0
  
                if (fileSizeInMegabytes > 9.999 ) {
                  // In the future send an email with the error stating that the file is too large
                  console.log("File too large");
                  return;
                };
  
                console.log('File size: ' + fileSizeInMegabytes + ' MB');
                
                // Read the file in base64 format
                var file = fs.readFileSync('/tmp/results.csv.gz','base64');

                // Send the mail using the new file object

                sendRawMail(formData, file);
                replyPage(200, "Files for " + formData.telephone + " are being retrieved", callback);
              };
            });
  
            // Handle errors encountered during the API call
            eventStream.on('error', (err) => {
              switch (err.name) {}
            });
            eventStream.on('end', () => {  
              console.log("Pull has finished")
              // Finished recieving events from S3
            });
          });
        };
      };
    });
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


function sendRawMail(formData, file) {
  // send as email attachment
  var ses_mail = "From: Data Handling <data.handling@business.com>\n";
  ses_mail = ses_mail + "To: end.user@business.com\n";
  ses_mail = ses_mail + "Subject: business Data Request for " + formData.telephone + "\n";
  ses_mail = ses_mail + "MIME-Version: 1.0\n";
  ses_mail = ses_mail + "Content-Type: multipart/mixed; boundary=\"NextPart\"\n\n";
  ses_mail = ses_mail + "--NextPart\n";
  ses_mail = ses_mail + "Content-Type: text/html; charset=us-ascii\n";
  ses_mail = ses_mail + "\n<p>Please see the attached file for the records that matched the provided telephone.</p>\n";
  ses_mail = ses_mail + "--NextPart\n";    
  ses_mail = ses_mail + "Content-Type: application/zip; name=\"" + formData.key + "\"\n";
  ses_mail = ses_mail + "Content-Description: " + formData.key + "\n";
  ses_mail = ses_mail + "Content-Disposition: attachment;filename=\"" + formData.key + "\";\n";
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

function sendMail(formData, callback) {
  const emailParams = {
    Source: 'data.handling@business.com', // SES SENDING EMAIL
    ReplyToAddresses: ["data.handling@business.com"],
    Destination: {
      ToAddresses: ["end.user@business.com"], // SES RECEIVING EMAIL
    },
    Message: {
      Body: {
        Html: {
          Charset: 'UTF-8',
          Data: `\n<p>File ${formData.key} has no matching records for ${formData.telephone}</p>\n`
        },
        Text: {
          Charset: 'UTF-8',
          Data: `\nFile ${formData.key} has no matching records for ${formData.telephone}\n`
        }
      },
      Subject: {
        Charset: 'UTF-8',
        Data: `Data Retrieval For ${formData.telephone}`,
      }
    }
  };
  console.log("Mail sent");
  SES.sendEmail(emailParams,  function(err, data) {
    if (err) console.log(err, err.stack); // an error occurred
    else     console.log(data);           // successful response
  });
}