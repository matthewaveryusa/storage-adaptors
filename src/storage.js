'use strict';
var fs = require('fs'),
  crypto = require('crypto'),
  aws = require('aws-sdk');

function makeStorageError(status,errorCode) {
  return {'type':'storageError', 'status':status, 'errorCode':errorCode};
}

exports.makeStorageError = makeStorageError;

function makeHTTPExists(request,baseUrl) {
  return function httpExists(filename, callback){
    request.head([baseUrl, filename].join('/'),function(err,data){
        if(err) {
          if(err.statusCode === 404) {
            return callback(makeStorageError(404,'notFound'));
          }
          return callback(err);
        }
        return callback(null, response.headers);
    });
  }
}
exports.makeHTTPExists = makeHTTPExists;

function makeHTTPRead(request,baseUrl) {
  return function HTTPRead(filename) {
    return request.get([baseUrl, filename].join('/'));
  };
}
exports.makeHTTPRead = makeHTTPRead;

function makeHTTPWrite(request,baseUrl) {
  return function HTTPWrite(filename, fileStream, callback) {
    return fileStream.pipe(request.post([baseUrl, filename].join('/')));
  };
}
exports.makeHTTPWrite = makeHTTPWrite;

function makeS3Exists(path) {
  return function s3Exists(filename, callback){
    var s3 = new aws.S3();
    var params = {Bucket: path, Key: filename};
    s3.headObject( params,function(err, data) {
      if (err) {
        if (err.name === 'NotFound') {
          return callback(makeStorageError(404,'notFound'));
        }
        return callback(err);
      }
      return callback(null,{ 'headers': {
        'Accept-Ranges':data.AcceptRanges,
        'Content-Length': data.ContentLength,
        'ETag': data.ETag,
        'Last-Modified': data.LastModified
      }});
    });
  };
}

exports.makeS3Exists = makeS3Exists;

function makeS3Read(path) {
  return function s3Read(filename) {
    var s3 = new aws.S3();
    return s3.getObject({Bucket: path, Key: filename}).createReadStream();
  };
}

exports.makeS3Read = makeS3Read;

function makeS3Write(path) {
  return function s3Write(filename, fileStream, callback) {
    var s3 = new aws.S3();
    s3.upload({Bucket: path, Key: filename, Body: fileStream}).send(callback);
  };
}
exports.makeS3Write = makeS3Write;

function makeFileSystemExists(path) {
  return function fileSystemExists(filename, callback) {
    fs.stat(path + filename, function (err, stat) {
      if (err) {
        return callback(makeStorageError(404,'notFound'));
      }

      if (!stat.isFile()) {
        return callback(makeStorageError(404,'notFound'));
      }

      var fd = fs.createReadStream(path + filename);
      var hash = crypto.createHash('sha1');
      hash.setEncoding('hex');
      fd.on('end', function () {
        hash.end();
        return callback(null, {
          'headers': {
            'Accept-Ranges': 'bytes',
            'Content-Length': stat.size,
            'ETag': hash.read(),
            'Last-Modified': stat.mtime
          }
        });
      });
      fd.on('error', function (err) {
        callback(err);
      });
      fd.pipe(hash);
    });
  };
}

exports.makeFileSystemExists = makeFileSystemExists;

function makeFileSystemRead(path) {
  return function fileSystemRead(filename) {
    return fs.createReadStream(path + filename);
  };
}

exports.makeFileSystemRead = makeFileSystemRead;

function makeFileSystemWrite(path) {
  return function fileSystemWrite(filename, fileStream, callback) {
    var fd = fs.createWriteStream(path + filename);
    fileStream.on('data',function(data){
      fd.write(data);
    })
    .on('end',function() {
      fd.end();
      callback();
    })
    .on('error',callback);
  };
}

exports.makeFileSystemWrite = makeFileSystemWrite;
