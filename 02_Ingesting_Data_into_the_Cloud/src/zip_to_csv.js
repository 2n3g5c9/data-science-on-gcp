'use strict';
const fs = require('fs');
const path = require('path');
const unzipper = require('unzipper');

const dataDir = './data';
if (!fs.existsSync(`${dataDir}/raw_csv`)) {
  fs.mkdirSync(`${dataDir}/raw_csv`, { recursive: true });
}

fs.readdir(`${dataDir}/zip/`, (err, files) => {
  if (err) {
    return console.log(err);
  }

  files.forEach(file => {
    const input = fs.createReadStream(`${dataDir}/zip/${file}`);
    const output = fs.createWriteStream(
      `${dataDir}/raw_csv/${path.parse(file).name}.csv`
    );

    input.pipe(unzipper.Parse()).on('entry', entry => {
      console.log(`Unzipping "${file}" in ${dataDir}/raw_csv/...`);
      entry.pipe(output);
    });
  });
});
