class Product {
  constructor(id, label, price, createdAt, updatedAt, deletedAt = null) {
    this._id = id;
    this.label = label;
    this.price = price;
    this.createdAt = createdAt;
    this.updatedAt = updatedAt;
    this.deletedAt = deletedAt;
  }

  toCsv() {
    return `${this._id},${this.label},${
      this.price
    },${this.createdAt.toISOString()},${this.updatedAt.toISOString()},${
      this.deletedAt ? this.deletedAt.toISOString() : ""
    }`;
  }

  static fromCsv(csvLine) {
    const parts = csvLine.split(",");
    return new Product(
      parts[0],
      parts[1],
      Number(parts[2]),
      new Date(parts[3]),
      new Date(parts[4]),
      parts[5] ? new Date(parts[5]) : null
    );
  }
}

module.exports = {
  Product,
};
