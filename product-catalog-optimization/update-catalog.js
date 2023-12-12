const fs = require("fs");
const csv = require("csv-parser");

const { MongoClient } = require("mongodb");

const { memory } = require("./helpers/memory");
const { timing } = require("./helpers/timing");
const { Metrics } = require("./helpers/metrics");
const { Product } = require("./product");

const MONGO_URL = "mongodb://localhost:27017/test-product-catalog";
const catalogUpdateFile = "updated-catalog.csv";

async function main() {
  const mongoClient = new MongoClient(MONGO_URL);
  const connection = await mongoClient.connect();
  const db = connection.db();
  await memory("Update dataset", () =>
    timing("Update dataset", () => updateDataset(db))
  );
}

async function updateDataset(db) {
  const metrics = Metrics.zero();
  let bulkOps = [];
  let rows = 0;

  function updateMetrics(updateResult) {
    if (updateResult.nModified) {
      metrics.updatedCount = updateResult.nModified;
    }
    if (updateResult.nUpserted) {
      metrics.addedCount = updateResult.nUpserted;
    }
    if (updateResult.nRemoved) {
      metrics.deletedCount = updateResult.nRemoved;
    }
  }
  await new Promise((resolve, reject) => {
    fs.createReadStream(catalogUpdateFile)
      .pipe(csv())
      .on("data", async (row) => {
        const product = new Product(
          row._id,
          row.label,
          row.price,
          row.createdAt,
          row.updatedAt,
          row.deletedAt ? row.deletedAt : null
        );
        if (product.deletedAt) {
          bulkOps.push({ deleteOne: { filter: { _id: product._id } } });
        } else {
          bulkOps.push({
            updateOne: {
              filter: { _id: product._id },
              update: { $set: product },
              upsert: true,
            },
          });
        }
        rows++;
        if (bulkOps.length === 100000) {
          const bulkWriteResult = await db
            .collection("Products")
            .bulkWrite(bulkOps);
          console.log(`[INFO] Processed ${rows} CSV rows.`);
          updateMetrics(bulkWriteResult);
          bulkOps = [];
        }
      })
      .on("end", async () => {
        if (bulkOps.length > 0) {
          const bulkWriteResult = await db
            .collection("Products")
            .bulkWrite(bulkOps);
          updateMetrics(bulkWriteResult);
        }
        logMetrics(rows, metrics);
        resolve();
      })
      .on("error", reject);
  });
}

function logMetrics(numberOfProcessedRows, metrics) {
  console.info(`[INFO] Processed ${numberOfProcessedRows} CSV rows.`);
  console.info(`[INFO] Added ${metrics.addedCount} new products.`);
  console.info(`[INFO] Updated ${metrics.updatedCount} existing products.`);
  console.info(`[INFO] Deleted ${metrics.deletedCount} products.`);
}

if (require.main === module) {
  main()
    .then(() => {
      console.log("SUCCESS");
      process.exit(0);
    })
    .catch((err) => {
      console.log("FAIL");
      console.error(err);
      process.exit(1);
    });
}
