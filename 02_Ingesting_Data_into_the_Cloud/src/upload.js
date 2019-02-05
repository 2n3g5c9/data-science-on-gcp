'use strict';
require('dotenv').config();

const fs = require('fs');
const { Storage } = require('@google-cloud/storage');

const storage = new Storage();
const bucketName = 'data-engineering-trial-bucket';

const dataDir = './data';
console.log(`Uploading data from ${dataDir}/csv/...`);

fs.readdir(`${dataDir}/csv/`, (err, files) => {
  if (err) {
    return console.log(err);
  }

  files.forEach(file => {
    storage
      .bucket(bucketName)
      .upload(`${dataDir}/csv/${file}`, {
        destination: `/flights/raw/${file}`,
        gzip: true,
        metadata: {
          cacheControl: 'public, max-age=31536000'
        }
      })
      .catch(err => console.log(err));
  });
});
