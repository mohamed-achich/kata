const fs = require("fs");
const uuidv4 = require("uuid").v4;
const { MongoClient } = require("mongodb");
const minimist = require("minimist");

const { timing } = require("./helpers/timing");
const { memory } = require("./helpers/memory");
const { Metrics } = require("./helpers/metrics");
const { Product } = require("./product");

const DATABASE_NAME = "test-product-catalog";
const MONGO_URL = `mongodb://localhost:27017/${DATABASE_NAME}`;
const catalogUpdateFile = "updated-catalog.csv";

const { size } = minimist(process.argv.slice(2));
if (!size) {
  throw new Error("Missing 'size' parameter");
}
if (fs.existsSync(catalogUpdateFile)) {
  fs.rmSync(catalogUpdateFile);
}

const stream = fs.createWriteStream(catalogUpdateFile, { flags: "a" });

async function main() {
  const mongoClient = new MongoClient(MONGO_URL);
  const connection = await mongoClient.connect();
  const db = connection.db();
  // For running the script several times without manually cleaning the data
  await clearExistingData(db);

  await memory("Generate dataset", () =>
    timing("Generate dataset", async () => await generateDataset(db, size))
  );
  stream.end();
  return new Promise((resolve) => {
    stream.on("finish", resolve);
  });
}

async function clearExistingData(db) {
  const listDatabaseResult = await db.admin().listDatabases({ nameOnly: 1 });
  if (listDatabaseResult.databases.find((d) => d.name === DATABASE_NAME)) {
    await db.dropDatabase();
  }
}

async function generateDataset(db, catalogSize) {
  await writeCsvHeaders();

  const metrics = Metrics.zero();
  const createdAt = new Date();
  const products = [];
  for (let i = 0; i < catalogSize; i++) {
    const product = generateProduct(i, createdAt);
    // replaced individual write opearations with a bulk insert 1
    products.push(product);

    // insert in updated dataset (csv) with a tweak
    const updatedProduct = generateUpdate(product, i, catalogSize);
    metrics.merge(await writeProductUpdateToCsv(product, updatedProduct));

    const progressPercentage = (i * 100) / catalogSize;
    if (progressPercentage % 10 === 0) {
      console.debug(`[DEBUG] Processing ${progressPercentage}%...`);
    }
  }

  // replaced individual write opearations with a bulk insert 2
  await db.collection("Products").insertMany(products);

  logMetrics(catalogSize, metrics);
}

async function writeCsvHeaders() {
  // removed blocking operations

  stream.write(Object.keys(generateProduct(-1, null)).join(",") + "\n");
}

function generateProduct(index, createdAt) {
  return new Product(
    uuidv4(),
    `Product_${index}`,
    generatePrice(),
    createdAt,
    createdAt
  );
}

function generatePrice() {
  return Math.round(Math.random() * 1000 * 100) / 100;
}

const productEvent = {
  pDelete: 10, // probability of deleting the product
  pUpdate: 10, // probability of updating the product
  pAdd: 20, // probability of adding a new product
};

function generateUpdate(product, index, catalogSize) {
  const rand = Math.random() * 100; // float in [0; 100]
  if (rand < productEvent.pDelete) {
    // [0; pDelete[
    // Delete product
    return new Product(
      product._id,
      `Product_${index + catalogSize}`,
      generatePrice(),
      product.createdAt,
      product.createdAt,
      new Date()
    );
  }
  if (rand < productEvent.pDelete + productEvent.pUpdate) {
    // [pDelete; pUpdate[
    // Update product
    return new Product(
      product._id,
      `Product_${index + catalogSize}`,
      generatePrice(),
      product.createdAt,
      new Date()
    );
  }
  if (rand < productEvent.pDelete + productEvent.pUpdate + productEvent.pAdd) {
    // [pUpdate; pAdd[
    // Add new product
    return generateProduct(index + catalogSize, new Date());
  }

  // Unchanged product
  return product; // [pAdd; 100]
}

async function writeProductUpdateToCsv(product, updatedProduct) {
  if (updatedProduct && !updatedProduct.deletedAt) {
    if (updatedProduct._id === product._id) {
      // Updated product or no modification => add this line
      // removed blocking operations

      stream.write(updatedProduct.toCsv() + "\n");
      return updatedProduct.updatedAt !== updatedProduct.createdAt
        ? Metrics.updated()
        : Metrics.zero();
    } else {
      // add new product
      // removed blocking operations

      stream.write(updatedProduct.toCsv() + "\n");
      return Metrics.added();
    }
  } else {
    stream.write(updatedProduct.toCsv() + "\n");
    return Metrics.deleted();
  }
}

function logMetrics(catalogSize, metrics) {
  console.info(`[INFO] ${catalogSize} products inserted in DB.`);
  console.info(`[INFO] ${metrics.addedCount} products to be added.`);
  console.info(
    `[INFO] ${metrics.updatedCount} products to be updated ${(
      (metrics.updatedCount * 100) /
      catalogSize
    ).toFixed(2)}%.`
  );
  console.info(
    `[INFO] ${metrics.deletedCount} products to be deleted ${(
      (metrics.deletedCount * 100) /
      catalogSize
    ).toFixed(2)}%.`
  );
}

if (require.main === module) {
  main()
    .then(() => {
      console.log("SUCCESS");
      process.exit(0);
    })
    .catch((err) => {
      console.log("FAIL");
      stream.end();
      console.error(err);
      process.exit(1);
    });
}
