import express from 'express';
import { Queue, Worker } from 'bullmq';
import 'dotenv/config';
import Redis from 'ioredis'; 

// Import Processors
import { fetchAndProcessOrdersMD } from './workers/md_processor.js';
import { fetchAndProcessOrders } from './processor.js';
import { fetchAndProcessOrdersSHRD } from './workers/shrd_processor.js';
import { fetchAndProcessOrdersCLEV } from './workers/clev_processor.js';
import { fetchAndProcessOrdersDRJOU } from './workers/drjou_processor.js';
import { fetchAndProcessOrdersMOSS } from './workers/moss_processor.js';
import { fetchAndProcessOrdersGB } from './workers/gb_processor.js';
import { fetchAndProcessOrdersIL } from './workers/il_processor.js';
import { fetchAndProcessOrdersEVOKE } from './workers/evoke_processor.js';
import { fetchAndProcessOrdersMMW } from './workers/mmw_processor.js';
import { fetchAndProcessOrdersCHESS } from './workers/chess_processor.js';
import { fetchAndProcessOrdersSV } from './workers/sv_processor.js';
import { fetchAndProcessOrdersPN } from './workers/pn_processor.js';
import { fetchAndProcessOrdersNB } from './workers/nb_processor.js';
import { fetchAndProcessOrdersMIRAE } from './workers/mirae_processor.js';
import { fetchAndProcessOrdersPOLY } from './workers/poly_processor.js';

// --- 1. SETUP & CONFIGURATION ---
const app = express();
app.use(express.json());
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
const mockQueue = new Queue('mock-run', redisConnection);
const orderQueue = new Queue("realtime-order-processing", redisConnection);
const orderQueueMD = new Queue("realtime-fetch-orders-md", redisConnection);
const orderQueueSHRD = new Queue("realtime-fetch-orders-shrd", redisConnection);
const orderQueueCLEV = new Queue("realtime-fetch-orders-clev", redisConnection);
const orderQueueDRJOU = new Queue("realtime-fetch-orders-drjou", redisConnection);
const orderQueueMOSS = new Queue("realtime-fetch-orders-moss", redisConnection);
const orderQueueGB = new Queue("realtime-fetch-orders-gb", redisConnection);
const orderQueueIL = new Queue("realtime-fetch-orders-il", redisConnection);
const orderQueueEV = new Queue("realtime-fetch-orders-evoke", redisConnection);
const orderQueueMMW = new Queue("realtime-fetch-orders-mmw", redisConnection);
const orderQueueCHESS = new Queue("realtime-fetch-orders-chess", redisConnection);
const orderQueueSV = new Queue("realtime-fetch-orders-sv", redisConnection);
const orderQueuePN = new Queue("realtime-fetch-orders-pn", redisConnection);
const orderQueueNB = new Queue("realtime-fetch-orders-nb", redisConnection);
const orderQueueMIRAE = new Queue("realtime-fetch-orders-mirae", redisConnection);
const orderQueuePOLY = new Queue("realtime-fetch-orders-poly", redisConnection);


// --- 3. EXPRESS ROUTES (HTTP HANDLERS) ---

// Health Check
app.get('/', (req, res) => {
    res.status(200).send("Service is healthy (API + Workers Running)");
});

app.get('/mock', (req, res) => {
    res.status(200).send("Mock test")
});

app.get('/mock/run', async (req, res) => {
    const text = handleMockRun();
    res.status(200).send(text);
    // const timestamp = new Date().toISOString();
    // const baseOptions = {
    //     attempts: 5,
    //     backoff: { type: 'exponential', delay: 60000 }
    // };
    // await mockQueue.add('mock-run', {}, { 
    //     ...baseOptions, 
    //     jobId: `mock-run-${timestamp}`,
    //     delay: 0 
    // });
});


// Cloud Scheduler Endpoint - FULLY POPULATED
app.get('/realtime-sync', async (req, res) => {
    // Security check for Cloud Scheduler
    if(req.header('X-Cloud-Scheduler-Job') !== 'true') {
        // console.warn("Unauthorized attempt to trigger daily sync"); 
        // return res.status(403).send('Forbidden');
    }

    try {
        const timestamp = new Date().toISOString();
        const baseOptions = {
            attempts: 5,
            backoff: { type: 'exponential', delay: 60000 }
        };

        // // --- GROUP 1: Shared Account Risk ---
        // 1. Evoke: Starts Immediately
        await orderQueueEV.add('realtime-fetch-orders-evoke', {}, { 
            ...baseOptions, 
            jobId: `evoke-daily-sync-${timestamp}`,
            delay: 0 
        });

        // 2. Dr. Jou: Starts +3 minutes later
        await orderQueueDRJOU.add('realtime-fetch-orders-drjou', {}, { 
            ...baseOptions, 
            jobId: `drjou-daily-sync-${timestamp}`,
            delay: 180000 
        });

        // // 3. Swissvita: Starts +6 minutes later
        // await orderQueueSV.add('realtime-fetch-orders-sv', {}, { 
        //     ...baseOptions, 
        //     jobId: `sv-daily-sync-${timestamp}`,
        //     delay: 360000 
        // });

        // // --- GROUP 2: Independent Brands ---
        let stagger = 30000; 
        const interval = 45000; 

        // Eileen Grace
        await orderQueue.add('realtime-fetch-daily-orders', {}, { 
            ...baseOptions, 
            jobId: `daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // // Miss Daisy
        // await orderQueueMD.add('realtime-fetch-orders-md', {}, { 
        //     ...baseOptions, 
        //     jobId: `md-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // // SH-RD
        // await orderQueueSHRD.add('realtime-fetch-orders-shrd', {}, { 
        //     ...baseOptions, 
        //     jobId: `shrd-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // // Cleviant
        // await orderQueueCLEV.add('realtime-fetch-orders-clev', {}, { 
        //     ...baseOptions, 
        //     jobId: `clev-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // // Mosseru
        // await orderQueueMOSS.add('realtime-fetch-orders-moss', {}, { 
        //     ...baseOptions, 
        //     jobId: `moss-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // // G-Belle
        // await orderQueueGB.add('realtime-fetch-orders-gb', {}, { 
        //     ...baseOptions, 
        //     jobId: `gb-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // // Ivy & Lily
        // await orderQueueIL.add('realtime-fetch-orders-il', {}, { 
        //     ...baseOptions, 
        //     jobId: `il-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // // Mamaway
        // await orderQueueMMW.add('realtime-fetch-orders-mmw', {}, { 
        //     ...baseOptions, 
        //     jobId: `mmw-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // // Chess
        // await orderQueueCHESS.add('realtime-fetch-orders-chess', {}, { 
        //     ...baseOptions, 
        //     jobId: `chess-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // // Past Nine
        // await orderQueuePN.add('realtime-fetch-orders-pn', {}, { 
        //     ...baseOptions, 
        //     jobId: `pn-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // // Nutri Beyond
        await orderQueueNB.add('realtime-fetch-orders-nb', {}, { 
            ...baseOptions, 
            jobId: `nb-daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // // Mirae
        // await orderQueueMIRAE.add('realtime-fetch-orders-mirae', {}, { 
        //     ...baseOptions, 
        //     jobId: `mirae-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });
        // stagger += interval;

        // // Polynia
        // await orderQueuePOLY.add('realtime-fetch-orders-poly', {}, { 
        //     ...baseOptions, 
        //     jobId: `poly-daily-sync-${timestamp}`, 
        //     delay: stagger 
        // });

        console.log("Daily sync job enqueued by Cloud Scheduler with Staggered Delays");
        res.status(200).send("Successfully enqueued daily sync job");
    } catch (e) {
        console.error("Failed to enqueue daily job: ", e);
        res.status(500).send("Failed to enqueue job");
    }
});

// Manual Fetch Endpoint
app.get("/orders", async (req, res) => {
    try {
        await orderQueue.add('manual-fetch', {}).catch(err => {
            throw new Error("Failed to enqueue job: " + err.message);
        });
        console.log("Manual fetch enqueued.");
        res.json({
            message: "Job to fetch and process orders has been enqueued." 
        });
    } catch (e) {
        res.status(500).json({ error: "Failed to enqueue job", details: e.message });
    }
});

// Admin: Pause All
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

// Admin: Resume All
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

// Admin: Remove Job
app.get('/admin/remove-job', async (req, res) => {
    const { jobId } = req.query; 

    if (!jobId) {
        return res.status(400).send("Missing 'jobId' query parameter.");
    }

    try {
        // Attempt to find the job in the main queue (original logic)
        // If you need to search ALL queues, you would need to loop through them,
        // but keeping original logic for now to prevent breaking changes.
        const job = await orderQueue.getJob(jobId);
        if (job) {
            await job.remove();
            console.log(`ADMIN: Successfully removed job ${jobId}.`);
            res.status(200).send(`Successfully removed job ${jobId}.`);
        } else {
            console.log(`ADMIN: Job ${jobId} not found in main queue.`);
            res.status(404).send(`Job ${jobId} not found in main queue.`);
        }
    } catch (e) {
        console.error(`ADMIN: Error removing job ${jobId}:`, e);
        res.status(500).send(`Error removing job ${jobId}`);
    }
});

// Admin: Stop & Clean All
app.get('/admin/stop-all-jobs', async (req, res) => {
    try {
        const queues = [
            orderQueue, orderQueueMD, orderQueueSHRD, orderQueueCLEV, 
            orderQueueDRJOU, orderQueueMOSS, orderQueueGB, orderQueueIL, 
            orderQueueEV, orderQueueMMW, orderQueueCHESS, orderQueueSV, 
            orderQueuePN, orderQueueNB, orderQueueMIRAE, orderQueuePOLY
        ];

        console.log("ADMIN: Pausing all queues...");
        await Promise.all(queues.map(q => q.pause()));

        console.log("ADMIN: Nuke sequence initiated...");

        // Loop through every queue
        for (const q of queues) {
            // 1. Drain "Waiting" and "Delayed" (This is the built-in fast wipe)
            await q.drain(); 
            await q.drain(true);

            // 2. Forcefully remove "Active" (Zombie) jobs that are stuck running
            // We fetch ALL jobs in these states and delete them manually
            const zombieJobs = await q.getJobs(['active', 'wait', 'delayed', 'failed']);
            for (const job of zombieJobs) {
                await job.remove().catch(err => console.error(`Failed to remove job ${job.id}: ${err.message}`));
            }
            
            // 3. Clean history (Completed/Failed)
            await q.clean(0, 1000, 'failed');
            await q.clean(0, 1000, 'completed');
        }

        console.log("ADMIN: All queues have been NUKED. Zero jobs remain.");
        res.status(200).send("Queue PAUSED and ALL jobs (Active/Wait/Delayed/Failed) have been REMOVED.");

    } catch (e) {
        console.error("ADMIN: Error stopping all jobs:", e);
        res.status(500).send("Error stopping all jobs");
    }
});

app.get('/admin/flush-redis', async (req, res) => {
    try {
        console.log("ADMIN: Connecting to Redis for manual flush...");
        const connection = new Redis(process.env.REDIS_URL);
        
        // Send the FLUSHDB command directly
        await connection.flushdb();
        
        // Close this temporary connection
        await connection.quit();

        console.log("ADMIN: Redis FLUSHDB executed. All keys deleted.");
        res.status(200).send("REDIS FLUSHED. The database is empty. You may now Start Fresh.");
    } catch (e) {
        console.error("ADMIN: Error flushing Redis:", e);
        res.status(500).send("Error flushing Redis: " + e.message);
    }
});


// --- 4. INITIALIZE WORKERS (CONSUMERS) ---

const createWorker = (queueName, processor, name) => {
    const worker = new Worker(queueName, processor, workerOptions);
    worker.on('active', (job) => console.log(`[${name}] ACTIVE: Job ${job.id}.`));
    worker.on('completed', (job) => console.log(`[${name}] COMPLETED: Job ${job.id}.`));
    worker.on('failed', (job, err) => console.error(`[${name}] FAILED: Job ${job.id}.`, err));
    worker.on('ready', () => console.log(`[${name}] Ready.`));
    return worker;
};

function handleMockRun() {
    return {
        message: "Mock function is running!",
        description: "This internal app fetches data from Shopee and Tiktok Marketing API and merges it to our BigQuery. Deployed on Cloud Run. It is as simple as that."
    };
}

// --- Worker Definitions ---
const mockProcessor = async (job) => {
    if(job.name === "mock-run") return handleMockRun();
    throw new Error(`Unknown job name: ${job.name}`);
};
const mockWorker = createWorker("mock-run", mockProcessor, "MOCK");

const orderProcessor = async (job) => {
    if (job.name === 'realtime-fetch-daily-orders' || job.name === 'manual-fetch') return fetchAndProcessOrders();
    throw new Error(`Unknown job name: ${job.name}`);
};
const orderWorker = createWorker("realtime-order-processing", orderProcessor, "EG");

const mdOrderProcessor = async (job) => {
    if (job.name === 'realtime-fetch-orders-md') return fetchAndProcessOrdersMD();
    throw new Error(`Unknown job name: ${job.name}`);
};
const mdWorker = createWorker("realtime-fetch-orders-md", mdOrderProcessor, "MD");

const shrdWorker = createWorker("realtime-fetch-orders-shrd", async (job) => {
    if (job.name === 'realtime-fetch-orders-shrd') return fetchAndProcessOrdersSHRD();
    throw new Error(`Unknown job name: ${job.name}`);
}, "SHRD");

const clevWorker = createWorker("realtime-fetch-orders-clev", async (job) => {
    if (job.name === 'realtime-fetch-orders-clev') return fetchAndProcessOrdersCLEV();
    throw new Error(`Unknown job name: ${job.name}`);
}, "CLEV");

const drjouWorker = createWorker("realtime-fetch-orders-drjou", async (job) => {
    if (job.name === 'realtime-fetch-orders-drjou') return fetchAndProcessOrdersDRJOU();
    throw new Error(`Unknown job name: ${job.name}`);
}, "DRJOU");

const mossWorker = createWorker("realtime-fetch-orders-moss", async (job) => {
    if (job.name === 'realtime-fetch-orders-moss') return fetchAndProcessOrdersMOSS();
    throw new Error(`Unknown job name: ${job.name}`);
}, "MOSS");

const gbWorker = createWorker("realtime-fetch-orders-gb", async (job) => {
    if (job.name === 'realtime-fetch-orders-gb') return fetchAndProcessOrdersGB();
    throw new Error(`Unknown job name: ${job.name}`);
}, "GB");

const ilWorker = createWorker("realtime-fetch-orders-il", async (job) => {
    if (job.name === 'realtime-fetch-orders-il') return fetchAndProcessOrdersIL();
    throw new Error(`Unknown job name: ${job.name}`);
}, "IL");

const evWorker = createWorker("realtime-fetch-orders-evoke", async (job) => {
    if (job.name === 'realtime-fetch-orders-evoke') return fetchAndProcessOrdersEVOKE();
    throw new Error(`Unknown job name: ${job.name}`);
}, "EVOKE");

const mmwWorker = createWorker("realtime-fetch-orders-mmw", async (job) => {
    if (job.name === 'realtime-fetch-orders-mmw') return fetchAndProcessOrdersMMW();
    throw new Error(`Unknown job name: ${job.name}`);
}, "MMW");

const chessWorker = createWorker("realtime-fetch-orders-chess", async (job) => {
    if (job.name === 'realtime-fetch-orders-chess') return fetchAndProcessOrdersCHESS();
    throw new Error(`Unknown job name: ${job.name}`);
}, "CHESS");

const svWorker = createWorker("realtime-fetch-orders-sv", async (job) => {
    if (job.name === 'realtime-fetch-orders-sv') return fetchAndProcessOrdersSV();
    throw new Error(`Unknown job name: ${job.name}`);
}, "SV");

const pnWorker = createWorker("realtime-fetch-orders-pn", async (job) => {
    if (job.name === 'realtime-fetch-orders-pn') return fetchAndProcessOrdersPN();
    throw new Error(`Unknown job name: ${job.name}`);
}, "PN");

const nbWorker = createWorker("realtime-fetch-orders-nb", async (job) => {
    if (job.name === 'realtime-fetch-orders-nb') return fetchAndProcessOrdersNB();
    throw new Error(`Unknown job name: ${job.name}`);
}, "NB");

const miraeWorker = createWorker("realtime-fetch-orders-mirae", async (job) => {
    if (job.name === 'realtime-fetch-orders-mirae') return fetchAndProcessOrdersMIRAE();
    throw new Error(`Unknown job name: ${job.name}`);
}, "MIRAE");

const polyWorker = createWorker("realtime-fetch-orders-poly", async (job) => {
    if (job.name === 'realtime-fetch-orders-poly') return fetchAndProcessOrdersPOLY();
    throw new Error(`Unknown job name: ${job.name}`);
}, "POLY");


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