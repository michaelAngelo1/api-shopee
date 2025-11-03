import { fetchAndProcessOrdersMD } from './sample-fetch/md_processor.js';
import { fetchAndProcessOrders } from './processor.js';
import { fetchAndProcessOrdersSHRD } from './sample-fetch/shrd_processor.js';
import { Worker } from 'bullmq';
import 'dotenv/config';
import express from 'express';
import { fetchAndProcessOrdersCLEV } from './sample-fetch/clev_processor.js';
import { fetchAndProcessOrdersDRJOU } from './sample-fetch/drjou_processor.js';
import { fetchAndProcessOrdersMOSS } from './sample-fetch/moss_processor.js';

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

const workerOptionsMD = {
    connection: {
        url: process.env.REDIS_URL,
        connectTimeout: 30000,
    },
    lockDuration: 5400000,
}

const workerOptionsSHRD = {
    connection: {
        url: process.env.REDIS_URL,
        connectTimeout: 30000,
    },
    lockDuration: 5400000,
}

const workerOptionsCLEV = {
    connection: {
        url: process.env.REDIS_URL,
        connectTimeout: 30000,
    },
    lockDuration: 5400000,
}

const workerOptionsDRJOU = {
    connection: {
        url: process.env.REDIS_URL,
        connectTimeout: 30000,
    },
    lockDuration: 5400000,
}

const workerOptionsMOSS = {
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

const mdOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-md':
            return fetchAndProcessOrdersMD();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const mdWorker = new Worker("fetch-orders-md", mdOrderProcessor, workerOptionsMD);

const shrdOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-shrd':
            return fetchAndProcessOrdersSHRD();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const shrdWorker = new Worker("fetch-orders-shrd", shrdOrderProcessor, workerOptionsSHRD);

const clevOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-clev':
            return fetchAndProcessOrdersCLEV();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const clevWorker = new Worker("fetch-orders-clev", clevOrderProcessor, workerOptionsCLEV);

const drjouOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-drjou':
            return fetchAndProcessOrdersDRJOU();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const drjouWorker = new Worker("fetch-orders-drjou", drjouOrderProcessor, workerOptionsDRJOU);

const mossOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-moss':
            return fetchAndProcessOrdersMOSS();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const mossWorker = new Worker("fetch-orders-moss", mossOrderProcessor, workerOptionsMOSS);

// Eileen Grace worker events
orderWorker.on('active', (job) => {
    console.log(`[eg-worker] Picked up job with ID ${job.id}.`);
});
orderWorker.on('completed', (job) => {
    console.log(`[eg-worker] Job with ID ${job.id} has completed.`);
});
orderWorker.on('ready', (job) => {
    console.log("[eg-worker] Worker is ready to listen.");
});
orderWorker.on('failed', (job, err) => {
    console.error(`[eg-worker] Job with ID ${job.id} has failed. Error:`, err);
});
orderWorker.on('error', (err) => {
    console.error('[eg-worker] Worker encountered an error:', err);
});

// Miss Daisy worker events
mdWorker.on('active', (job) => {
    console.log(`[fetch-orders-md] ACTIVE: Job ${job.id}.`);
});
mdWorker.on('completed', (job) => {
    console.log(`[fetch-orders-md] COMPLETED: Job ${job.id}.`);
});
mdWorker.on('ready', (job) => {
    console.log("[fetch-orders-md] MD Worker is ready to listen");
});
mdWorker.on('failed', (job, err) => {
    console.error(`[fetch-orders-md] FAILED: Job ${job.id}.`, err);
});

// SHRD
shrdWorker.on('active', (job) => {
    console.log(`[fetch-orders-shrd] ACTIVE: Job ${job.id}.`);
});
shrdWorker.on('completed', (job) => {
    console.log(`[fetch-orders-shrd] COMPLETED: Job ${job.id}.`);
});
shrdWorker.on('ready', (job) => {
    console.log("[fetch-orders-shrd] SHRD Worker is ready to listen");
});
shrdWorker.on('failed', (job, err) => {
    console.error(`[fetch-orders-shrd] FAILED: Job ${job.id}.`, err);
});

// Cleviant
clevWorker.on('active', (job) => {
    console.log(`[fetch-orders-clev] ACTIVE: Job ${job.id}.`);
});
clevWorker.on('completed', (job) => {
    console.log(`[fetch-orders-clev] COMPLETED: Job ${job.id}.`);
});
clevWorker.on('ready', (job) => {
    console.log("[fetch-orders-clev] CLEV Worker is ready to listen");
});
clevWorker.on('failed', (job, err) => {
    console.error(`[fetch-orders-clev] FAILED: Job ${job.id}.`, err);
});

// Dr. Jou
drjouWorker.on('active', (job) => {
    console.log(`[fetch-orders-drjou] ACTIVE: Job ${job.id}.`);
});
drjouWorker.on('completed', (job) => {
    console.log(`[fetch-orders-drjou] COMPLETED: Job ${job.id}.`);
});
drjouWorker.on('ready', (job) => {
    console.log("[fetch-orders-drjou] DRJOU Worker is ready to listen");
});
drjouWorker.on('failed', (job, err) => {
    console.error(`[fetch-orders-drjou] FAILED: Job ${job.id}.`, err);
});

const gracefulShutdown = async () => {
    console.log("Shutting down worker...");

    await Promise.all([
        orderWorker.close(),
        mdWorker.close(),
        shrdWorker.close()
    ])
    
    console.log("Worker shut down complete.");
    process.exit(0);
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

// 251001VCHANUW9