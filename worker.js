import express from 'express';
import { Queue, Worker } from 'bullmq';
import 'dotenv/config';

// Import Processors
import { fetchAndProcessOrdersMD } from './sample-fetch/md_processor.js';
import { fetchAndProcessOrders } from './processor.js';
import { fetchAndProcessOrdersSHRD } from './sample-fetch/shrd_processor.js';
import { fetchAndProcessOrdersCLEV } from './sample-fetch/clev_processor.js';
import { fetchAndProcessOrdersDRJOU } from './sample-fetch/drjou_processor.js';
import { fetchAndProcessOrdersMOSS } from './sample-fetch/moss_processor.js';
import { fetchAndProcessOrdersGB } from './sample-fetch/gb_processor.js';
import { fetchAndProcessOrdersIL } from './sample-fetch/il_processor.js';
import { fetchAndProcessOrdersEVOKE } from './sample-fetch/evoke_processor.js';
import { fetchAndProcessOrdersMMW } from './sample-fetch/mmw_processor.js';
import { fetchAndProcessOrdersCHESS } from './sample-fetch/chess_processor.js';
import { fetchAndProcessOrdersSV } from './sample-fetch/sv_processor.js';
import { fetchAndProcessOrdersPN } from './sample-fetch/pn_processor.js';
import { fetchAndProcessOrdersNB } from './sample-fetch/nb_processor.js';
import { fetchAndProcessOrdersMIRAE } from './sample-fetch/mirae_processor.js';
import { fetchAndProcessOrdersPOLY } from './sample-fetch/poly_processor.js';

// --- 1. SETUP & CONFIGURATION ---
const app = express();
const port = process.env.PORT || 8080;

const redisConnection = {
    connection: {
        url: process.env.REDIS_URL,
        connectTimeout: 30000,
    }
};

// Worker options (includes lock duration)
const workerOptions = {
    ...redisConnection,
    lockDuration: 5400000,
};

console.log("DEBUG: Current REDIS_URL is configured.");
console.log("System Starting: Initializing Queues and Workers...");

// --- 2. INITIALIZE QUEUES (PRODUCERS) ---
// These are used by the Express Routes to ADD jobs
const orderQueue = new Queue("order-processing", redisConnection);
const orderQueueMD = new Queue("fetch-orders-md", redisConnection);
const orderQueueSHRD = new Queue("fetch-orders-shrd", redisConnection);
const orderQueueCLEV = new Queue("fetch-orders-clev", redisConnection);
const orderQueueDRJOU = new Queue("fetch-orders-drjou", redisConnection);
const orderQueueMOSS = new Queue("fetch-orders-moss", redisConnection);
const orderQueueGB = new Queue("fetch-orders-gb", redisConnection);
const orderQueueIL = new Queue("fetch-orders-il", redisConnection);
const orderQueueEV = new Queue("fetch-orders-evoke", redisConnection);
const orderQueueMMW = new Queue("fetch-orders-mmw", redisConnection);
const orderQueueCHESS = new Queue("fetch-orders-chess", redisConnection);
const orderQueueSV = new Queue("fetch-orders-sv", redisConnection);
const orderQueuePN = new Queue("fetch-orders-pn", redisConnection);
const orderQueueNB = new Queue("fetch-orders-nb", redisConnection);
const orderQueueMIRAE = new Queue("fetch-orders-mirae", redisConnection);
const orderQueuePOLY = new Queue("fetch-orders-poly", redisConnection);


// --- 3. EXPRESS ROUTES (HTTP HANDLERS) ---

// Health Check
app.get('/', (req, res) => {
    res.status(200).send("Service is healthy (API + Workers Running)");
});

// Cloud Scheduler Endpoint
app.get('/staging-sync', async (req, res) => {
    // Security check for Cloud Scheduler
    if(req.header('X-Cloud-Scheduler-Job') !== 'true') {
        // console.warn("Unauthorized attempt to trigger daily sync"); 
        // Uncomment above if strict, but sometimes helpful to allow manual test via browser if needed
    }

    try {
        const timestamp = new Date().toISOString();
        const baseOptions = {
            attempts: 5,
            backoff: { type: 'exponential', delay: 60000 }
        };

        await orderQueueEV.add('fetch-orders-evoke', {}, { 
            ...baseOptions, 
            jobId: `evoke-daily-sync-${timestamp}`,
            delay: 0 
        });

        // await orderQueueDRJOU.add('fetch-orders-drjou', {}, { 
        //     ...baseOptions, 
        //     jobId: `drjou-daily-sync-${timestamp}`,
        //     delay: 180000 
        // });

        // await orderQueueSV.add('fetch-orders-sv', {}, { 
        //     ...baseOptions, 
        //     jobId: `sv-daily-sync-${timestamp}`,
        //     delay: 360000 
        // });

        
        let stagger = 30000; 
        const interval = 45000; 

        // await orderQueue.add('fetch-daily-orders', {}, { 
        //     ...baseOptions, 
        //     jobId: `daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // await orderQueueMD.add('fetch-orders-md', {}, { 
        //     ...baseOptions, 
        //     jobId: `md-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // await orderQueueSHRD.add('fetch-orders-shrd', {}, { 
        //     ...baseOptions, 
        //     jobId: `shrd-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // await orderQueueCLEV.add('fetch-orders-clev', {}, { 
        //     ...baseOptions, 
        //     jobId: `clev-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // await orderQueueMOSS.add('fetch-orders-moss', {}, { 
        //     ...baseOptions, 
        //     jobId: `moss-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // await orderQueueGB.add('fetch-orders-gb', {}, { 
        //     ...baseOptions, 
        //     jobId: `gb-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // await orderQueueIL.add('fetch-orders-il', {}, { 
        //     ...baseOptions, 
        //     jobId: `il-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // await orderQueueMMW.add('fetch-orders-mmw', {}, { 
        //     ...baseOptions, 
        //     jobId: `mmw-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // await orderQueueCHESS.add('fetch-orders-chess', {}, { 
        //     ...baseOptions, 
        //     jobId: `chess-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // await orderQueuePN.add('fetch-orders-pn', {}, { 
        //     ...baseOptions, 
        //     jobId: `pn-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // await orderQueueNB.add('fetch-orders-nb', {}, { 
        //     ...baseOptions, 
        //     jobId: `nb-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // await orderQueueMIRAE.add('fetch-orders-mirae', {}, { 
        //     ...baseOptions, 
        //     jobId: `mirae-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // await orderQueuePOLY.add('fetch-orders-poly', {}, { 
        //     ...baseOptions, 
        //     jobId: `poly-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });

        console.log("Daily sync job enqueued.");
        res.status(200).send("Successfully enqueued daily sync job");
    } catch (e) {
        console.error("Failed to enqueue daily job: ", e);
        res.status(500).send("Failed to enqueue job");
    }
});

// Admin Endpoints
app.get('/admin/pause-queue', async (req, res) => {
    try {
        await Promise.all([
            orderQueue.pause(), orderQueueMD.pause(), orderQueueSHRD.pause(),
            orderQueueCLEV.pause(), orderQueueDRJOU.pause(), orderQueueMOSS.pause(),
            orderQueueGB.pause(), orderQueueIL.pause(), orderQueueEV.pause(),
            orderQueueMMW.pause(), orderQueueCHESS.pause(), orderQueueSV.pause(),
            orderQueuePN.pause(), orderQueueNB.pause(), orderQueueMIRAE.pause(),
            orderQueuePOLY.pause()
        ]);
        console.log("ADMIN: all queues have been paused.");
        res.status(200).send("All queues have been PAUSED.");
    } catch (e) {
        console.error("ADMIN: Error pausing queue:", e);
        res.status(500).send("Error pausing queue");
    }
});

app.get('/admin/resume-queue', async (req, res) => {
    try {
        await Promise.all([
            orderQueue.resume(), orderQueueMD.resume(), orderQueueSHRD.resume(),
            orderQueueCLEV.resume(), orderQueueDRJOU.resume(), orderQueueMOSS.resume(),
            orderQueueGB.resume(), orderQueueIL.resume(), orderQueueEV.resume(),
            orderQueueMMW.resume(), orderQueueCHESS.resume(), orderQueueSV.resume(),
            orderQueuePN.resume(), orderQueueNB.resume(), orderQueueMIRAE.resume(),
            orderQueuePOLY.resume()
        ]);
        console.log("ADMIN: all queues have been resumed");
        res.status(200).send("All queues have been RESUMED.");
    } catch (e) {
        console.error("ADMIN: Error resuming queue:", e);
        res.status(500).send("Error resuming queue");
    }
});

// --- 4. INITIALIZE WORKERS (CONSUMERS) ---
// These process the jobs added by the queues above

// Helper to create workers to save space, or define individually as before
const createWorker = (queueName, processor, name) => {
    const worker = new Worker(queueName, processor, workerOptions);
    worker.on('active', (job) => console.log(`[${name}] ACTIVE: Job ${job.id}.`));
    worker.on('completed', (job) => console.log(`[${name}] COMPLETED: Job ${job.id}.`));
    worker.on('failed', (job, err) => console.error(`[${name}] FAILED: Job ${job.id}.`, err));
    worker.on('ready', () => console.log(`[${name}] Ready.`));
    return worker;
};

// Processors
const orderProcessor = async (job) => {
    if (job.name === 'fetch-daily-orders' || job.name === 'manual-fetch') return fetchAndProcessOrders();
    throw new Error(`Unknown job name: ${job.name}`);
};
const orderWorker = createWorker("order-processing", orderProcessor, "EG");

const mdOrderProcessor = async (job) => {
    if (job.name === 'fetch-orders-md') return fetchAndProcessOrdersMD();
    throw new Error(`Unknown job name: ${job.name}`);
};
const mdWorker = createWorker("fetch-orders-md", mdOrderProcessor, "MD");

// ... Instantiate the rest of your workers similarly ...
const shrdWorker = createWorker("fetch-orders-shrd", async (job) => job.name === 'fetch-orders-shrd' ? fetchAndProcessOrdersSHRD() : null, "SHRD");
const clevWorker = createWorker("fetch-orders-clev", async (job) => job.name === 'fetch-orders-clev' ? fetchAndProcessOrdersCLEV() : null, "CLEV");
const drjouWorker = createWorker("fetch-orders-drjou", async (job) => job.name === 'fetch-orders-drjou' ? fetchAndProcessOrdersDRJOU() : null, "DRJOU");
const mossWorker = createWorker("fetch-orders-moss", async (job) => job.name === 'fetch-orders-moss' ? fetchAndProcessOrdersMOSS() : null, "MOSS");
const gbWorker = createWorker("fetch-orders-gb", async (job) => job.name === 'fetch-orders-gb' ? fetchAndProcessOrdersGB() : null, "GB");
const ilWorker = createWorker("fetch-orders-il", async (job) => job.name === 'fetch-orders-il' ? fetchAndProcessOrdersIL() : null, "IL");
const evWorker = createWorker("fetch-orders-evoke", async (job) => job.name === 'fetch-orders-evoke' ? fetchAndProcessOrdersEVOKE() : null, "EVOKE");
const mmwWorker = createWorker("fetch-orders-mmw", async (job) => job.name === 'fetch-orders-mmw' ? fetchAndProcessOrdersMMW() : null, "MMW");
const chessWorker = createWorker("fetch-orders-chess", async (job) => job.name === 'fetch-orders-chess' ? fetchAndProcessOrdersCHESS() : null, "CHESS");
const svWorker = createWorker("fetch-orders-sv", async (job) => job.name === 'fetch-orders-sv' ? fetchAndProcessOrdersSV() : null, "SV");
const pnWorker = createWorker("fetch-orders-pn", async (job) => job.name === 'fetch-orders-pn' ? fetchAndProcessOrdersPN() : null, "PN");
const nbWorker = createWorker("fetch-orders-nb", async (job) => job.name === 'fetch-orders-nb' ? fetchAndProcessOrdersNB() : null, "NB");
const miraeWorker = createWorker("fetch-orders-mirae", async (job) => job.name === 'fetch-orders-mirae' ? fetchAndProcessOrdersMIRAE() : null, "MIRAE");
const polyWorker = createWorker("fetch-orders-poly", async (job) => job.name === 'fetch-orders-poly' ? fetchAndProcessOrdersPOLY() : null, "POLY");


// --- 5. START SERVER & SHUTDOWN LOGIC ---

app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
    console.log("ALL Systems (API & Workers) are GO.");
});

const gracefulShutdown = async () => {
    console.log("Shutting down...");
    
    // Close all workers
    await Promise.all([
        orderWorker.close(), mdWorker.close(), shrdWorker.close(), clevWorker.close(),
        drjouWorker.close(), mossWorker.close(), gbWorker.close(), ilWorker.close(),
        evWorker.close(), mmwWorker.close(), chessWorker.close(), svWorker.close(),
        pnWorker.close(), nbWorker.close(), miraeWorker.close(), polyWorker.close()
    ]);

    console.log("Shutdown complete.");
    process.exit(0);
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);