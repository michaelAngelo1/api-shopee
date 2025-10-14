import express from 'express';
import { Queue } from 'bullmq';
import 'dotenv/config';

const app = express();
const port = 3000;

const redisConnection = {
    connection: {
        url: process.env.REDIS_URL,
        connectTimeout: 30000,
    }
};

const orderQueue = new Queue("order-processing", redisConnection);

//ver 3
async function scheduleDailyJob() {
    const jobName = "fetch-daily-orders";
    const jobDefinition = {
        // pattern: "55 15 * * *",
        pattern: "33 20 * * *",
        tz: "Asia/Jakarta"
    };

    // 1. Get all active schedulers using the correct, modern API
    const schedulers = await orderQueue.getJobSchedulers();

    // 2. Find the scheduler for this specific job by its name
    // const existingScheduler = schedulers.find(
    //     scheduler => scheduler.name === jobName
    // );
       
    for(const sched of schedulers) {
        if(sched.name === jobName) {
            await orderQueue.removeJobScheduler(sched.name, sched.cron) 
        }
    };

    await orderQueue.add(jobName, {}, {
        repeat: jobDefinition,
        jobId: jobName, // This jobId helps differentiate schedulers if needed
        attempts: 3,
        backoff: {
            type: 'exponential',
            delay: 60000,
        }
    });
    console.log(`Scheduled daily job "${jobName}" with pattern "${jobDefinition.pattern}".`);

    // let needsUpdate = false;

    // if (!existingScheduler) {
    //     console.log(`Job scheduler for "${jobName}" not found. Creating it.`);
    //     needsUpdate = true;
    // } else if (existingScheduler.cron !== jobDefinition.pattern || existingScheduler.tz !== jobDefinition.tz) {
    //     console.log(`Job scheduler for "${jobName}" has a different schedule. Updating it.`);
        
    //     // Remove the old, incorrect scheduler using its name and cron pattern
    //     await orderQueue.removeJobScheduler(existingScheduler.name, existingScheduler.cron);
    //     console.log(`Removed old scheduler for "${jobName}".`);
    //     needsUpdate = true;
    // }

    // if (needsUpdate) {
    //     // 3. Add the new job/scheduler since it doesn't exist or was incorrect
    //     await orderQueue.add(jobName, {}, {
    //         repeat: jobDefinition,
    //         jobId: jobName, // This jobId helps differentiate schedulers if needed
    //         attempts: 3,
    //         backoff: {
    //             type: 'exponential',
    //             delay: 60000,
    //         }
    //     });
    //     console.log(`Scheduled daily job "${jobName}" with pattern "${jobDefinition.pattern}".`);
    // } else {
    //     console.log(`Job scheduler for "${jobName}" is already configured correctly. No action needed.`);
    // }
}

app.get('/trigger-daily-sync', async (req, res) => {

    if(req.header('X-Cloud-Scheduler-Job') !== 'true') {
        console.warn("Unauthorized attempt to trigger daily sync");
        return res.status(403).send('Forbidden');
    }

    try {
        
        await orderQueue.add('fetch-daily-orders', {}, {
            jobId: `daily-sync-${new Date().toISOString().split('T')[0]}`, 
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

app.listen(port, async () => {
    console.log(`Server is running on http://localhost:${port}`);
    scheduleDailyJob()
        .catch(err => console.error("Failed to schedule daily job:", err));  
})