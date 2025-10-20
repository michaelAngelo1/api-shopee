import { fetchAndProcessOrders } from './processor.js';
import { Worker } from 'bullmq';
import 'dotenv/config';
import express from 'express';

const workerApp = express();
const port = process.env.PORT || 8080;

workerApp.get('/', (req, res) => {
    res.status(200).send("Worker is healthy");
});

workerApp.listen(port, () => {
    console.log("Health check server listening on port: ", port);
});

const workerOptions = {
    connection: {
        url: process.env.REDIS_URL,
        connectTimeout: 30000,
    },
    lockDuration: 5400000,
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

const orderWorker = new Worker("order-processing", orderProcessor, workerOptions);

orderWorker.on('active', (job) => {
    console.log(`ACTIVE: Picked up job with ID ${job.id}.`);
});

orderWorker.on('completed', (job) => {
    console.log(`INDEXJS: Job with ID ${job.id} has completed.`);
});

orderWorker.on('ready', (job) => {
    console.log("INDEXJS: Worker is ready to listen.");
});

orderWorker.on('failed', (job, err) => {
    console.error(`INDEXJS: Job with ID ${job.id} has failed. Error:`, err);
});

orderWorker.on('error', (err) => {
    console.error('INDEXJS: Worker encountered an error:', err);
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