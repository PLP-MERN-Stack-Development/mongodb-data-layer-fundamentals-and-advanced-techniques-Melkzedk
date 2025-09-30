// insert_books.js - Script to populate MongoDB with sample book data
// and demonstrate CRUD operations, advanced queries, aggregation, and indexing

const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

const books = [
  { title: 'To Kill a Mockingbird', author: 'Harper Lee', genre: 'Fiction', published_year: 1960, price: 12.99, in_stock: true, pages: 336, publisher: 'J. B. Lippincott & Co.' },
  { title: '1984', author: 'George Orwell', genre: 'Dystopian', published_year: 1949, price: 10.99, in_stock: true, pages: 328, publisher: 'Secker & Warburg' },
  { title: 'The Great Gatsby', author: 'F. Scott Fitzgerald', genre: 'Fiction', published_year: 1925, price: 9.99, in_stock: true, pages: 180, publisher: 'Charles Scribner\'s Sons' },
  { title: 'Brave New World', author: 'Aldous Huxley', genre: 'Dystopian', published_year: 1932, price: 11.50, in_stock: false, pages: 311, publisher: 'Chatto & Windus' },
  { title: 'The Hobbit', author: 'J.R.R. Tolkien', genre: 'Fantasy', published_year: 1937, price: 14.99, in_stock: true, pages: 310, publisher: 'George Allen & Unwin' },
  { title: 'The Catcher in the Rye', author: 'J.D. Salinger', genre: 'Fiction', published_year: 1951, price: 8.99, in_stock: true, pages: 224, publisher: 'Little, Brown and Company' },
  { title: 'Pride and Prejudice', author: 'Jane Austen', genre: 'Romance', published_year: 1813, price: 7.99, in_stock: true, pages: 432, publisher: 'T. Egerton, Whitehall' },
  { title: 'The Lord of the Rings', author: 'J.R.R. Tolkien', genre: 'Fantasy', published_year: 1954, price: 19.99, in_stock: true, pages: 1178, publisher: 'Allen & Unwin' },
  { title: 'Animal Farm', author: 'George Orwell', genre: 'Political Satire', published_year: 1945, price: 8.50, in_stock: false, pages: 112, publisher: 'Secker & Warburg' },
  { title: 'The Alchemist', author: 'Paulo Coelho', genre: 'Fiction', published_year: 1988, price: 10.99, in_stock: true, pages: 197, publisher: 'HarperOne' },
  { title: 'Moby Dick', author: 'Herman Melville', genre: 'Adventure', published_year: 1851, price: 12.50, in_stock: false, pages: 635, publisher: 'Harper & Brothers' },
  { title: 'Wuthering Heights', author: 'Emily Brontë', genre: 'Gothic Fiction', published_year: 1847, price: 9.99, in_stock: true, pages: 342, publisher: 'Thomas Cautley Newby' }
];

async function run() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Reset collection
    const count = await collection.countDocuments();
    if (count > 0) {
      await collection.drop();
      console.log('Old collection dropped');
    }

    // Insert sample books
    await collection.insertMany(books);
    console.log(`${books.length} books inserted\n`);

    // -----------------------------
    // Task 2: Basic CRUD Operations
    // -----------------------------
    console.log("Task 2: Basic CRUD");

    // Find by genre
    console.log("\nBooks in Fiction genre:");
    console.log(await collection.find({ genre: "Fiction" }).toArray());

    // Find published after 1950
    console.log("\nBooks published after 1950:");
    console.log(await collection.find({ published_year: { $gt: 1950 } }).toArray());

    // Find by author
    console.log("\nBooks by George Orwell:");
    console.log(await collection.find({ author: "George Orwell" }).toArray());

    // Update price
    await collection.updateOne({ title: "The Great Gatsby" }, { $set: { price: 15.99 } });
    console.log("\nUpdated The Great Gatsby price");

    // Delete by title
    await collection.deleteOne({ title: "Animal Farm" });
    console.log("\nDeleted Animal Farm");

    // -----------------------------
    // Task 3: Advanced Queries
    // -----------------------------
    console.log("\nTask 3: Advanced Queries");

    // In stock and after 2010
    console.log("\nIn-stock books after 2010:");
    console.log(await collection.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray());

    // Projection
    console.log("\nProjection (title, author, price):");
    console.log(await collection.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray());

    // Sorting
    console.log("\nBooks sorted by price ascending:");
    console.log(await collection.find().sort({ price: 1 }).toArray());

    console.log("\nBooks sorted by price descending:");
    console.log(await collection.find().sort({ price: -1 }).toArray());

    // Pagination (page 1 & 2, 5 books each)
    console.log("\nPage 1 (5 books):");
    console.log(await collection.find().limit(5).toArray());

    console.log("\nPage 2 (next 5 books):");
    console.log(await collection.find().skip(5).limit(5).toArray());

    // -----------------------------
    // Task 4: Aggregation Pipeline
    // -----------------------------
    console.log("\nTask 4: Aggregation");

    // Avg price by genre
    console.log("\nAverage price by genre:");
    console.log(await collection.aggregate([{ $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }]).toArray());

    // Author with most books
    console.log("\nAuthor with most books:");
    console.log(await collection.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray());

    // Books grouped by decade
    console.log("\nBooks grouped by decade:");
    console.log(await collection.aggregate([
      {
        $group: {
          _id: { $subtract: [{ $divide: ["$published_year", 10] }, { $mod: [{ $divide: ["$published_year", 10] }, 1] }] },
          count: { $sum: 1 }
        }
      },
      { $project: { decade: { $multiply: ["$_id", 10] }, count: 1, _id: 0 } },
      { $sort: { decade: 1 } }
    ]).toArray());

    // -----------------------------
    // Task 5: Indexing
    // -----------------------------
    console.log("\nTask 5: Indexing");

    // Create index on title
    await collection.createIndex({ title: 1 });
    console.log("Index created on title");

    // Compound index on author + year
    await collection.createIndex({ author: 1, published_year: 1 });
    console.log("Compound index created on author + published_year");

    // Explain query with index
    const explain = await collection.find({ title: "The Hobbit" }).explain("executionStats");
    console.log("\nExplain output for title search:");
    console.log(JSON.stringify(explain.executionStats, null, 2));

  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
    console.log("\nConnection closed");
  }
}

run();
