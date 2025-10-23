import express from 'express';
import { Queue } from 'bullmq';
import 'dotenv/config';

const app = express();
// const port = 3000;
const port = process.env.PORT || 8080;

const orderQueue = new Queue("order-processing", {
    connection: {
        url: process.env.REDIS_URL,
        connectTimeout: 30000,
    }
});

const orderQueueMD = new Queue("fetch-orders-md", {
    connection: {
        url: process.env.REDIS_URL,
        connectTimeout: 30000,
    }
});

app.get('/trigger-daily-sync', async (req, res) => {

    if(req.header('X-Cloud-Scheduler-Job') !== 'true') {
        console.warn("Unauthorized attempt to trigger daily sync");
        return res.status(403).send('Forbidden');
    }

    try {
        
        await orderQueue.add('fetch-daily-orders', {}, {
            jobId: `daily-sync-${new Date().toISOString()}`, 
            attempts: 3,
            backoff: {
                type: 'exponential',
                delay: 60000,
            }
        });

        await orderQueueMD.add('fetch-orders-md', {}, {
            jobId: `md-daily-sync-${new Date().toISOString()}`, 
            attempts: 3,
            backoff: {
                type: 'exponential',
                delay: 60000,
            }
        });

        console.log("Daily sync job enqueued by Cloud Scheduler");
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
        console.log("ADMIN: Queue 'order-processing' has been PAUSED.");
        res.status(200).send("Queue 'order-processing' has been PAUSED.");
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
        console.log("ADMIN: Queue 'order-processing' has been RESUMED.");
        res.status(200).send("Queue 'order-processing' has been RESUMED.");
    } catch (e) {
        console.error("ADMIN: Error resuming queue:", e);
        res.status(500).send("Error resuming queue");
    }
});

// ... Your other admin endpoints ...

//
// ADD THIS NEW ENDPOINT TO STOP AND CLEAN ALL JOBS
//
app.get('/admin/stop-all-jobs', async (req, res) => {
    try {
        // 1. Pause the queue. This stops workers from picking up NEW jobs.
        await orderQueue.pause();
        console.log("ADMIN: Queue 'order-processing' has been PAUSED.");

        // 2. Clean all jobs in these states.
        // This clears all retries and jobs waiting to run.
        await orderQueue.clean(0, 'wait'); // Clears all waiting jobs
        await orderQueue.clean(0, 'delayed'); // Clears all delayed (retry) jobs
        await orderQueue.clean(0, 'failed'); // Clears all failed jobs

        console.log("ADMIN: CLEARED all waiting, delayed, and failed jobs.");

        res.status(200).send("Queue PAUSED and all waiting/delayed/failed jobs have been CLEARED.");

    } catch (e) {
        console.error("ADMIN: Error stopping all jobs:", e);
        res.status(500).send("Error stopping all jobs");
    }
});

// ... Your app.listen(...) ...


app.listen(port, async () => {
    console.log(`Server is running on http://localhost:${port}`);
    // scheduleDailyJob()
    //     .catch(err => console.error("Failed to schedule daily job:", err));  
})