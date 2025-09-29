const express = require("express");
const { Queue } = require("bullmq");
require("dotenv").config();

const app = express();
const port = 3000;

const redisConnection = {
    connection: {
        url: process.env.REDIS_URL,
    }
};

const orderQueue = new Queue("order-processing", redisConnection);

// ver 1
// async function scheduleDailyJob() {

//     const jobName = "fetch-daily-orders";

//     const jobDefinition = {
//         pattern: "55 11 * * *",
//         tz: "Asia/Jakarta"
//     }

//     await orderQueue.removeJobScheduler(jobName, jobDefinition);
//     console.log("Attempted to remove any old schedule.");

//     await orderQueue.add("fetch-daily-orders", {}, {
//         repeat: {
//             pattern: "55 10 * * *",
//             tz: 'Asia/Jakarta'
//         },
//         jobId: "fetch-daily-orders",
//         attempts: 3,
//         backoff: {
//             type: 'exponential',
//             delay: 60000,
//         }
//     });

//     console.log("Scheduled daily job to fetch and merge orders at 00:05 Asia/Jakarta time.");
// }

// ver 2
// async function scheduleDailyJob() {
//     const jobName = "fetch-daily-orders";
//     const jobDefinition = {
//         pattern: "40 11 * * *",
//         tz: "Asia/Jakarta"
//     };       

//     const repeatableJobs = await orderQueue.getJobSchedulers();

//     const jobExists = repeatableJobs.some(job => job.name === jobName && job.pattern === jobDefinition.pattern && job.tz === jobDefinition.tz);
    
//     if(!jobExists) {
        
//         console.log("Job " + jobName + " does not exist. Scheduling new job.");
//         const oldJobs = repeatableJobs.filter(job => job.name === jobName);
//         for(const job of oldJobs) {
//             await orderQueue.removeJobScheduler(job.name, { pattern: job.pattern, tz: job.tz });
//             console.log("Removed old job with pattern: " + job.pattern + " and tz: " + job.tz);
//         }

//         await orderQueue.add(jobName, {}, {
//             repeat: jobDefinition,
//             jobId: jobName,
//             attempts: 3,
//             backoff: {
//                 type: 'exponential',
//                 delay: 60000,
//             }
//         });

//         console.log(`Scheduled daily job "${jobName}" with pattern "${jobDefinition.pattern}".`);
//     } else {
//         console.log(`Job "${jobName}" is already scheduled. No action taken.`);
//     }
// }

//ver 3
async function scheduleDailyJob() {
    const jobName = "fetch-daily-orders";
    const jobDefinition = {
        pattern: "05 12 * * *", // Example: 11:59 AM on every weekday (Mon-Fri)
        tz: "Asia/Jakarta"
    };

    // 1. Get all active schedulers using the correct, modern API
    const schedulers = await orderQueue.getJobSchedulers();

    // 2. Find the scheduler for this specific job by its name
    const existingScheduler = schedulers.find(
        scheduler => scheduler.name === jobName
    );

    let needsUpdate = false;

    if (!existingScheduler) {
        console.log(`Job scheduler for "${jobName}" not found. Creating it.`);
        needsUpdate = true;
    } else if (existingScheduler.cron !== jobDefinition.pattern || existingScheduler.tz !== jobDefinition.tz) {
        console.log(`Job scheduler for "${jobName}" has a different schedule. Updating it.`);
        
        // Remove the old, incorrect scheduler using its name and cron pattern
        await orderQueue.removeJobScheduler(existingScheduler.name, existingScheduler.cron);
        console.log(`Removed old scheduler for "${jobName}".`);
        needsUpdate = true;
    }

    if (needsUpdate) {
        // 3. Add the new job/scheduler since it doesn't exist or was incorrect
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
    } else {
        console.log(`Job scheduler for "${jobName}" is already configured correctly. No action needed.`);
    }
}

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