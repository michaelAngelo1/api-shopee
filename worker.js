const { Worker } = require('bullmq');
const { fetchAndProcessOrders } = require('./processor');
require('dotenv').config();

const connection = {
    connection: {
        url: process.env.REDIS_URL,
        connectTimeout: 30000,
    }
}

console.log("Worker is starting!");

const orderProcessor = async (job) => {
    switch(job.name) {
        case 'fetch-daily-orders':
        case 'manual-fetch':
            return fetchAndProcessOrders();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}

const orderWorker = new Worker("order-processing", orderProcessor, connection);

orderWorker.on('completed', (job) => {
    console.log(`Job with ID ${job.id} has completed.`);
});

orderWorker.on('failed', (job, err) => {
    console.error(`Job with ID ${job.id} has failed. Error:`, err);
});

orderWorker.on('error', (err) => {
    console.error('Worker encountered an error:', err);
});

const gracefulShutdown = async () => {
    console.log("Shutting down worker...");
    await orderWorker.close();
    console.log("Worker shut down complete.");
    process.exit(0);
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

// 251001VCHANUW9