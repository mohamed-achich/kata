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
  const products = [];
  const productIds = new Set();

  function updateMetrics(updateResult) {
    if (updateResult.nModified) {
      metrics.updatedCount = updateResult.nModified;
    }
    if (updateResult.upsertedCount) {
      metrics.addedCount = updateResult.nUpserted;
    }
  }
  await new Promise((resolve, reject) => {
    fs.createReadStream(catalogUpdateFile)
      .pipe(csv())
      .on("data", (row) => {
        const product = new Product({ row });
        products.push(product);
        productIds.add(product._id);
      })
      .on("end", async () => {
        // todo
        const bulkOps = products.map((product) => ({
          updateOne: {
            filter: { _id: product._id },
            update: { $set: product },
            upsert: true,
          },
        }));
        const bulkWriteResult = await db
          .collection("Products")
          .bulkWrite(bulkOps);
        console.log(bulkWriteResult);
        updateMetrics(bulkWriteResult);
        const dbIds = (
          await db.collection("Products").find({}, { _id: 1 }).toArray()
        ).map((o) => o._id);
        const deletedProductIds = dbIds.filter((id) => !productIds.has(id));
        if (deletedProductIds.length > 0) {
          const deleteResult = await db
            .collection("Products")
            .deleteMany({ _id: { $in: deletedProductIds } });
          console.log(deleteResult);

          metrics.deletedCount = deleteResult.deletedCount;
        }

        logMetrics(products.length, metrics);
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
