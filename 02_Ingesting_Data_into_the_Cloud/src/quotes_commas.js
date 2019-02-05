'use strict';
const fs = require('fs');
const readline = require('readline');

const lineCleaner = line => {
  return line.slice(0, -1).replace(/"/g, '');
};

const dataDir = './data';

fs.readdir(`${dataDir}/raw_csv/`, (err, files) => {
  if (err) {
    return console.log(err);
  }

  files.forEach(file => {
    const rl = readline.createInterface({
      input: fs.createReadStream(`${dataDir}/raw_csv/${file}`),
      output: fs.createWriteStream(`${dataDir}/csv/${file}`),
      terminal: false
    });

    console.log(`Cleaning file "${file}"...`);

    rl.on('line', line => {
      rl.output.write(`${lineCleaner(line)}\n`);
    });
  });
});
