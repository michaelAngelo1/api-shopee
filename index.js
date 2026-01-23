import express from 'express';
import { Queue } from 'bullmq';
import 'dotenv/config';
import Redis from 'ioredis';

const app = express();
// const port = 3000;
const port = process.env.PORT || 8080;

const redisConnection = {
    connection: {
        url: process.env.REDIS_URL,
        connectTimeout: 30000,
    }
}

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

app.get('/trigger-daily-sync', async (req, res) => {

    if(req.header('X-Cloud-Scheduler-Job') !== 'true') {
        console.warn("Unauthorized attempt to trigger daily sync");
        return res.status(403).send('Forbidden');
    }

    try {
        const timestamp = new Date().toISOString();
        
        // Base options to keep code clean
        const baseOptions = {
            attempts: 5,
            backoff: { type: 'exponential', delay: 60000 }
        };

        // --- GROUP 1: Shared Account Risk (Evoke, DrJou, Swissvita) ---
        // These MUST NOT run together. We force a 3-minute gap.

        // 1. Evoke: Starts Immediately
        await orderQueueEV.add('fetch-orders-evoke', {}, { 
            ...baseOptions, 
            jobId: `evoke-daily-sync-${timestamp}`,
            delay: 0 
        });

        // 2. Dr. Jou: Starts +3 minutes later
        await orderQueueDRJOU.add('fetch-orders-drjou', {}, { 
            ...baseOptions, 
            jobId: `drjou-daily-sync-${timestamp}`,
            delay: 180000 
        });

        // 3. Swissvita: Starts +6 minutes later
        await orderQueueSV.add('fetch-orders-sv', {}, { 
            ...baseOptions, 
            jobId: `sv-daily-sync-${timestamp}`,
            delay: 360000 
        });

        // --- GROUP 2: Independent Brands ---
        // Stagger these by 45 seconds so they don't hit the API all at once.
        
        let stagger = 30000; // Start the first one 30 seconds in
        const interval = 45000; // Add 45 seconds for each subsequent brand

        // Eileen Grace
        await orderQueue.add('fetch-daily-orders', {}, { 
            ...baseOptions, 
            jobId: `daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // Miss Daisy
        await orderQueueMD.add('fetch-orders-md', {}, { 
            ...baseOptions, 
            jobId: `md-daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // SH-RD
        await orderQueueSHRD.add('fetch-orders-shrd', {}, { 
            ...baseOptions, 
            jobId: `shrd-daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // Cleviant
        await orderQueueCLEV.add('fetch-orders-clev', {}, { 
            ...baseOptions, 
            jobId: `clev-daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // Mosseru
        await orderQueueMOSS.add('fetch-orders-moss', {}, { 
            ...baseOptions, 
            jobId: `moss-daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // G-Belle
        await orderQueueGB.add('fetch-orders-gb', {}, { 
            ...baseOptions, 
            jobId: `gb-daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // Ivy & Lily
        await orderQueueIL.add('fetch-orders-il', {}, { 
            ...baseOptions, 
            jobId: `il-daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // Mamaway
        await orderQueueMMW.add('fetch-orders-mmw', {}, { 
            ...baseOptions, 
            jobId: `mmw-daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // Chess
        await orderQueueCHESS.add('fetch-orders-chess', {}, { 
            ...baseOptions, 
            jobId: `chess-daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // Past Nine
        await orderQueuePN.add('fetch-orders-pn', {}, { 
            ...baseOptions, 
            jobId: `pn-daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // Nutri Beyond
        await orderQueueNB.add('fetch-orders-nb', {}, { 
            ...baseOptions, 
            jobId: `nb-daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // Mirae
        await orderQueueMIRAE.add('fetch-orders-mirae', {}, { 
            ...baseOptions, 
            jobId: `mirae-daily-sync-${timestamp}`, 
            delay: stagger 
        });
        stagger += interval;

        // Polynia
        await orderQueuePOLY.add('fetch-orders-poly', {}, { 
            ...baseOptions, 
            jobId: `poly-daily-sync-${timestamp}`, 
            delay: stagger 
        });

        console.log("Daily sync job enqueued by Cloud Scheduler with Staggered Delays");
        res.status(200).send("Successfully enqueued daily sync job");
    } catch (e) {
        console.error("Failed to enqueue daily job: ", e);
        res.status(500).send("Failed to enqueue job");
    }
});

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


// 1. ENDPOINT TO PAUSE THE QUEUE
app.get('/admin/pause-queue', async (req, res) => {
    try {
        await orderQueue.pause();
        await orderQueueMD.pause();
        await orderQueueSHRD.pause();
        await orderQueueCLEV.pause();

        console.log("ADMIN: all queues have been paused.");
        res.status(200).send("All queues have been PAUSED.");
    } catch (e) {
        console.error("ADMIN: Error pausing queue:", e);
        res.status(500).send("Error pausing queue");
    }
});

// 2. ENDPOINT TO REMOVE A SPECIFIC JOB
app.get('/admin/remove-job', async (req, res) => {
    const { jobId } = req.query; // Get jobId from query: ?jobId=...

    if (!jobId) {
        return res.status(400).send("Missing 'jobId' query parameter.");
    }

    try {
        const job = await orderQueue.getJob(jobId);
        if (job) {
            await job.remove();
            console.log(`ADMIN: Successfully removed job ${jobId}.`);
            res.status(200).send(`Successfully removed job ${jobId}.`);
        } else {
            console.log(`ADMIN: Job ${jobId} not found.`);
            res.status(404).send(`Job ${jobId} not found.`);
        }
    } catch (e) {
        console.error(`ADMIN: Error removing job ${jobId}:`, e);
        res.status(500).send(`Error removing job ${jobId}`);
    }
});

// 3. ENDPOINT TO RESUME THE QUEUE
app.get('/admin/resume-queue', async (req, res) => {
    try {
        await orderQueue.resume();
        await orderQueueMD.resume();
        await orderQueueSHRD.resume();
        await orderQueueCLEV.resume();
        await orderQueueDRJOU.resume();
        await orderQueueMOSS.resume();
        await orderQueueGB.resume();
        await orderQueueIL.resume();
        await orderQueueEV.resume();
        await orderQueueMMW.resume();
        await orderQueueCHESS.resume();
        await orderQueueSV.resume();
        await orderQueuePN.resume();
        await orderQueueNB.resume();
        await orderQueueMIRAE.resume();
        await orderQueuePOLY.resume();
        
        console.log("ADMIN: all queues have been resumed");
        res.status(200).send("All queues have been RESUMED.");
    } catch (e) {
        console.error("ADMIN: Error resuming queue:", e);
        res.status(500).send("Error resuming queue");
    }
});


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

// ... Your app.listen(...) ...


app.listen(port, async () => {
    console.log(`Server is running on http://localhost:${port}`);
    // scheduleDailyJob()
    //     .catch(err => console.error("Failed to schedule daily job:", err));  
})