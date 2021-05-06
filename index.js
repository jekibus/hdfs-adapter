'use strict';
// FileSystemAdapter
//
// Stores files in local file system
// Requires write access to the server's file system.

var fs = require('fs');
var path = require('path');
var pathSep = require('path').sep;
const axios = require('axios');
const crypto = require("crypto");
const algorithm = 'aes-256-gcm';

function FileSystemAdapter(options) {
  options = options || {};
  this._encryptionKey = null;
  this._filesDir = null;
  this._dataNodeURL = options.dataNode + options.path;
  this._nameNodeURL = options.nameNode + options.path;
}

FileSystemAdapter.prototype.createFile = function(filename, data) {
  return new Promise((resolve, reject) => {
    try{
      const url = this._dataNodeURL + filename + "?op=CREATE&namenoderpcaddress=namenode:8020&createflag&createparent=true&overwrite=false";
      const requestConfig = {
        method: "put",
        url: url,
        data: data
      };
      axios(requestConfig).then((d)=>{
        return resolve(data);
      }).catch(e=>{
        return reject(e);
      });
    }catch(err){
      return reject(err);
    }
  });
}

FileSystemAdapter.prototype.deleteFile = function(filename) {
  const url = this._nameNodeURL + filename + "?op=DELETE&recursive=true";
  return new Promise((resolve, reject) => {
    const requestConfig = {
      method: "delete",
      url: url
    };
    axios(requestConfig).then((d)=>{
      return resolve(d);
    }).catch(e=>{
      return reject(e);
    });
  });
}

FileSystemAdapter.prototype._saveFileBeforeDownload = function(filepath, response) {
  const writer = fs.createWriteStream(filepath);
  response.data.pipe(writer);
  return new Promise((resolve, reject) => {
    const chunks = [];
    writer.on('data', (data) => {
      chunks.push(data);
    });
    writer.on('finish', () => {
      const data = Buffer.concat(chunks);
      return resolve(data);
    });
    writer.on('error', (err) => {
      return reject(err);
    });
  });
}

FileSystemAdapter.prototype.getFileData = async function(filename) {
  // TODO: maybe we can download file without create here
  let filepath = this._getLocalFilePath(filename);
  // check is the file exist here?
  if (!fs.existsSync(filepath)) {
    const url = this._dataNodeURL + filename + "?op=OPEN&namenoderpcaddress=namenode:8020&offset=0";
    const requestConfig = {
      method: "get",
      url: url,
      responseType: 'stream'
    };
    const response = await axios(requestConfig);
    await this._saveFileBeforeDownload(filepath, response);
  }
  const stream = fs.createReadStream(filepath);
  stream.read();
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (data) => {
      chunks.push(data);
    });
    stream.on('end', () => {
      const data = Buffer.concat(chunks);
      return resolve(data);
    });
    stream.on('error', (err) => {
      return reject(err);
    });
  });
}

FileSystemAdapter.prototype.rotateEncryptionKey = function(options = {}) {
  const applicationDir = this._getApplicationDir();
  var fileNames = [];
  var oldKeyFileAdapter = {};
  if (options.oldKey !== undefined) {
    oldKeyFileAdapter = new FileSystemAdapter({filesSubDirectory: this._filesDir, encryptionKey: options.oldKey});
  }else{
    oldKeyFileAdapter = new FileSystemAdapter({filesSubDirectory: this._filesDir});
  }
  if (options.fileNames !== undefined){
    fileNames = options.fileNames;
  }else{
    fileNames = fs.readdirSync(applicationDir); 
    fileNames = fileNames.filter(fileName => fileName.indexOf('.') !== 0); 
  }
  return new Promise((resolve, _reject) => {
    var fileNamesNotRotated = fileNames;
    var fileNamesRotated = [];
    var fileNameTotal = fileNames.length;
    var fileNameIndex = 0;
    fileNames.forEach(fileName => { 
      oldKeyFileAdapter
        .getFileData(fileName)
        .then(plainTextData => {
          //Overwrite file with data encrypted with new key
          this.createFile(fileName, plainTextData)
          .then(() => {
            fileNamesRotated.push(fileName);
            fileNamesNotRotated = fileNamesNotRotated.filter(function(value){ return value !== fileName;})
            fileNameIndex += 1;
            if (fileNameIndex == fileNameTotal){
              resolve({rotated: fileNamesRotated, notRotated: fileNamesNotRotated});
            }
          })
          .catch(() => {
            fileNameIndex += 1;
            if (fileNameIndex == fileNameTotal){
              resolve({rotated: fileNamesRotated, notRotated: fileNamesNotRotated});
            }
          })
      })
      .catch(() => {
        fileNameIndex += 1;
        if (fileNameIndex == fileNameTotal){
          resolve({rotated: fileNamesRotated, notRotated: fileNamesNotRotated});
        }
      });
    });
  });
}

FileSystemAdapter.prototype.getFileLocation = function(config, filename) {
  return this._dataNodeURL + encodeURIComponent(filename) + "?op=OPEN&namenoderpcaddress=namenode:8020&offset=0";
}

/*
  Helpers
 --------------- */
 FileSystemAdapter.prototype._getApplicationDir = function() {
  if (this._filesDir) {
    return path.join('files', this._filesDir);
  } else {
    return 'files';
  }
 }

FileSystemAdapter.prototype._applicationDirExist = function() {
  return fs.existsSync(this._getApplicationDir());
}

FileSystemAdapter.prototype._getLocalFilePath = function(filename) {
  let applicationDir = this._getApplicationDir();
  if (!fs.existsSync(applicationDir)) {
    this._mkdir(applicationDir);
  }
  return path.join(applicationDir, encodeURIComponent(filename));
}

FileSystemAdapter.prototype._mkdir = function(dirPath) {
  // snippet found on -> https://gist.github.com/danherbert-epam/3960169
  let dirs = dirPath.split(pathSep);
  var root = "";

  while (dirs.length > 0) {
    var dir = dirs.shift();
    if (dir === "") { // If directory starts with a /, the first path will be an empty string.
      root = pathSep;
    }
    if (!fs.existsSync(path.join(root, dir))) {
      try {
        fs.mkdirSync(path.join(root, dir));
      }
      catch (e) {
        if ( e.code == 'EACCES' ) {
          throw new Error("PERMISSION ERROR: In order to use the FileSystemAdapter, write access to the server's file system is required.");
        }
      }
    }
    root = path.join(root, dir, pathSep);
  }
}

module.exports = FileSystemAdapter;
module.exports.default = FileSystemAdapter;