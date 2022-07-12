"use strict";

const {
  createWriteStream,
  createReadStream,
  rm,
  existsSync,
  mkdirSync,
} = require("fs");
const { join } = require("path");
const { pipeline } = require("stream/promises");
const readline = require("readline");

// Control constants
// Configure as per machine capacity. This are descrete values to start with
const BUFFER_CAPACITY = 10_000_000;
const MAX_LOGS_IN_MEMORY = 1_000;

/**
 * Merge sort tmp files
 * @param {String[]} tmpFiles List of tmp files to sort and merge
 * @param {String} outFileName File to output the merge sort result
 * @returns
 */
async function merge(tmpFiles, outFileName) {
  const resultFileName = join(__dirname, outFileName);
  const file = createWriteStream(resultFileName, {
    highWaterMark: BUFFER_CAPACITY,
  });

  const activeReaders = tmpFiles.map((name) =>
    readline
      .createInterface({
        input: createReadStream(name, { highWaterMark: BUFFER_CAPACITY }),
        crlfDelay: Infinity,
      })
      [Symbol.asyncIterator]()
  );

  const values = await Promise.all(
    activeReaders.map((r) => r.next().then((e) => JSON.parse(e.value)))
  );
  let record = 1;
  return pipeline(async function* () {
    while (activeReaders.length > 0) {
      console.log(`Sorting record ${record} into ${outFileName}`);
      const [minVal, i] = values.reduce(
        (prev, cur, idx) => {
          const curDate = new Date(cur.date);
          const previousDate = new Date(prev[0].date);

          return curDate.getTime() < previousDate.getTime() ? [cur, idx] : prev;
        },
        [{ date: new Date().toISOString(), msg: "Current Date" }, -1]
      );
      yield `${JSON.stringify(minVal)}\n`;

      const res = await activeReaders[i].next();
      if (!res.done) {
        values[i] = JSON.parse(res.value);
      } else {
        values.splice(i, 1);
        activeReaders.splice(i, 1);
      }
      record++;
    }
  }, file);
}

/**
 * Prints the result of the merge sort process
 * @param {Object} printer Printer instance
 * @param {String} srcFileName Name of the source file to read the logs and print
 */
const printResult = async (printer, srcFileName) => {
  const file = createReadStream(join(__dirname, srcFileName), {
    highWaterMark: BUFFER_CAPACITY,
  });
  const lines = readline.createInterface({
    input: file,
    crlfDelay: Infinity,
  });

  for await (let line of lines) {
    const log = JSON.parse(line);
    log.date = new Date(log.date);
    printer.print(log);
  }
  printer.done();
};

/**
 * Drains and prints to a tpm file from a logSource
 * @param {Object} logSource Log source
 * @param {String} sufix Sufix for the tpm file name
 * @param {Boolean} asyncGetter Indicates whether or not the logs getter funcion is async
 * @returns
 */
const writeToTmpFile = async (logSource, sufix, asyncGetter = false) => {
  const prefix = asyncGetter ? "async" : "sync";
  let tmpFile = join(__dirname, `tmp/${prefix}/tmp_sort_sync_${sufix}.txt`);
  let lines = [];
  let size = 0;
  let log;

  console.log(
    asyncGetter
      ? `Draining async log source #${sufix}`
      : `Draining sync log source #${sufix}`
  );

  while (!logSource.drained) {
    log = asyncGetter ? await logSource.popAsync() : logSource.pop();

    if (log) {
      size = size + 1;
      lines.push(`${JSON.stringify(log)}\n`);
      // TODO: Make this more fancy, currently is just based on number of records (chunks),
      // but it could be more precise in terms of memory
      if (size > MAX_LOGS_IN_MEMORY) {
        await pipeline(
          lines,
          createWriteStream(tmpFile, {
            highWaterMark: BUFFER_CAPACITY,
          })
        );
        size = 0;
        lines = [];
      }
    }
  }

  if (size > 0) {
    await pipeline(
      lines,
      createWriteStream(tmpFile, {
        highWaterMark: BUFFER_CAPACITY,
      })
    );
  }

  return tmpFile;
};

/**
 * Simple clean up function to delete tmp files
 * @param {String[]} tmpFiles List of tmp files to remove
 * @returns
 */
const cleanUp = async (tmpFiles) => {
  return Promise.all(tmpFiles.map((f) => rm(f, () => {})));
};

/**
 * Simple helper function to create directories
 * @param {String} tmpDir Name of the directory
 */
const createDir = (tmpDir) => {
  const dir = join(__dirname, tmpDir);
  if (!existsSync(dir)) {
    mkdirSync(dir);
  }
};

module.exports = {
  BUFFER_CAPACITY,
  MAX_LOGS_IN_MEMORY,
  merge,
  printResult,
  cleanUp,
  writeToTmpFile,
  createDir,
};
