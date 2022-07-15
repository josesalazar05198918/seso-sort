"use strict";

// Print all entries, across all of the sources, in chronological order.
const MinHeap = require("./minHeap");

module.exports = async (logSources, printer) => {
  const minHeap = new MinHeap();
  let promises = [];

  //Get initial head of all log sources
  for (const logSource of logSources) {
    promises.push(logSource.popAsync());
  }

  const heads = await Promise.all(promises);

  let j = 0;
  for (const h of heads) {
    minHeap.insert({
      source: logSources[j],
      head: h.date,
    });
    j++;
  }

  let index;
  let i = 0;
  let currentLogSource;
  let logItem;
  let skip;
  let keepDraining = true;
  while (keepDraining) {
    skip = false;
    index = minHeap.remove();

    if (index) {
      currentLogSource = index.source;

      if (!currentLogSource) {
        break;
      }

      //Print head date when applicable or just keep it in the heap
      if (
        minHeap.getMin() &&
        currentLogSource.last.date > minHeap.getMin().head
      ) {
        minHeap.insert({
          source: currentLogSource,
          head: currentLogSource.last.date,
        });
        skip = true;
      } else {
        printer.print(currentLogSource.last);
        i++;
      }

      // Drain current source until is greater than the heap's min value
      while (!currentLogSource.drained && !skip) {
        logItem = await currentLogSource.popAsync();

        if (logItem) {
          // Contrast against head of heap
          if (minHeap.getMin() && logItem.date > minHeap.getMin().head) {
            // Insert new date
            minHeap.insert({
              source: currentLogSource,
              head: logItem.date,
            });

            skip = true;
          } else {
            printer.print(logItem);
            i++;
          }
        }
      }
    } else {
      keepDraining = false;
    }
  }
  printer.done();
};
