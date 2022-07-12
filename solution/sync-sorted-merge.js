"use strict";

const {
  cleanUp,
  merge,
  printResult,
  writeToTmpFile,
  createDir,
} = require("./utils");

const outFileName = "tmp/sync/tmp_sort_sync_result.txt";

const drainLogSources = async (logSources) => {
  const tmpFiles = [];
  for (const logSource of logSources) {
    tmpFiles.push(await writeToTmpFile(logSource, tmpFiles.length));
  }
  return tmpFiles;
};

module.exports = (logSources, printer) => {
  return new Promise((resolve) => {
    createDir("tmp");
    createDir("tmp/sync");

    drainLogSources(logSources).then((tmpFiles) => {
      merge(tmpFiles, outFileName).then(() => {
        printResult(printer, outFileName).then(() => {
          cleanUp(tmpFiles).then(() => {
            resolve(console.log("Sync sort complete."));
          });
        });
      });
    });
  });
};
