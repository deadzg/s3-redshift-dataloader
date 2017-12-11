var pg = require('pg')

var connectionString = 'postgres://' + process.env.REDSHIFT_USERNAME + ':' + process.env.REDSHIFT_PASSWORD + '@' + process.env.REDSHIFT_ENDPOINT + ':' + process.env.REDSHIFT_PORT +'/' + process.env.REDSHIFT_DB
console.log(connectionString)
exports.handle = function (ev, ctx) {
  console.log('Received event:', JSON.stringify(ev, null, 2))

  // Get the object from the event and show its content type.
  var bucket = ev.Records[0].s3.bucket.name
  var key = decodeURIComponent(ev.Records[0].s3.object.key.replace(/\+/g, ' '))
  var typeMatch = key.match(/\.([^.]*)$/);
    if (!typeMatch) {
        callback("Could not determine the image type.");
        return;
    }
    var fileType = typeMatch[1];
    if (fileType != "csv") {
        callback('Unsupported file type: ${fileType}');
        return;
    }

  console.log("Bucket Name:" + bucket);
  console.log("Key:" + key);

  var s3Path = 's3://' + process.env.SOURCE_BUCKET + '/' + key
  var targetTable = process.env.TARGET_TABLE
  var credentials = process.env.AWS_IAM_S3_REDSHIFT_ROLE
  
  function quote (x) { return "'" + x + "'" }

  var newCopyStatement = [
    'COPY ' + targetTable,
    'FROM ' + quote(s3Path),
    'CREDENTIALS ' + quote(credentials),
    "DELIMITER ','",
    'region '+ quote(process.env.AWS_REGION),
    "CSV IGNOREHEADER 1"
  ].join('\n')
  
  console.log(newCopyStatement);
  
  var client = new pg.Client(connectionString)
  client.connect()


  var query = client.query(newCopyStatement)
  query.on("end", function (result) {
  client.end();
});
}


