"use strict";

const {
  cleanUp,
  merge,
  printResult,
  writeToTmpFile,
  createDir,
} = require("./utils");

const outFileName = "tmp/async/tmp_sort_async_result.txt";

const drainLogSources = async (logSources) => {
  const promises = [];
  let i = 0;
  for (const logSource of logSources) {
    promises.push(writeToTmpFile(logSource, i, true));
    i++;
  }
  const tmpFiles = await Promise.all(promises);
  return tmpFiles;
};

module.exports = (logSources, printer) => {
  return new Promise((resolve) => {
    createDir("tmp");
    createDir("tmp/async");

    drainLogSources(logSources).then((tmpFiles) => {
      merge(tmpFiles, outFileName).then(() => {
        printResult(printer, outFileName).then(() => {
          cleanUp(tmpFiles).then(() => {
            resolve(console.log("Async sort complete."));
          });
        });
      });
    });
  });
};
