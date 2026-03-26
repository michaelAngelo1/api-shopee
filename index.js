import express from 'express';
import 'dotenv/config';

const app = express();
const port = process.env.PORT || 8080;
// Make sure this is set in your GCP Cloud Run Environment Variables!
const WORKER_URL = "https://shopee-worker-231801348950.asia-southeast2.run.app";

app.get('/transactions-breakdown', async (req, res) => {
    if(req.header('X-Cloud-Scheduler-Job') !== 'true') {
        console.warn("Unauthorized attempt to trigger daily sync");
        return res.status(403).send('Forbidden');
    }
    try {
        await fetch(`${WORKER_URL}/process/transactions-breakdown`, { method: 'POST' });
        res.status(200).send("Transactions Breakdown has been triggered");
    } catch (e) {
        console.error("Failed Transactions Breakdown: ", e);
        res.status(500).send("Failed to trigger Transactions Breakdown");
    }
});

app.get('/tiktok-withdrawal', async (req, res) => {
    if(req.header('X-Cloud-Scheduler-Job') !== 'true') {
        console.warn("Unauthorized attempt to trigger daily sync");
        return res.status(403).send('Forbidden');
    }
    try {
        // We await the fetch to ensure Cloud Run doesn't spin down the container
        await fetch(`${WORKER_URL}/process/tiktok-withdrawal`, { method: 'POST' });
        res.status(200).send("Tiktok Withdrawal has been triggered");
    } catch (e) {
        console.error("Failed Tiktok Withdrawal: ", e);
        res.status(500).send("Failed to trigger Tiktok Withdrawal");
    }
});

app.get('/trigger-daily-sync', async (req, res) => {
    if(req.header('X-Cloud-Scheduler-Job') !== 'true') {
        console.warn("Unauthorized attempt to trigger daily sync");
        return res.status(403).send('Forbidden');
    }

    try {
        console.log("Passing daily sync trigger to Worker...");
        // This process will take ~16 minutes total.
        await fetch(`${WORKER_URL}/process/daily-sync`, { method: 'POST' });
        
        res.status(200).send("Successfully triggered daily sync job sequence");
    } catch (e) {
        console.error("Failed to trigger daily job: ", e);
        res.status(500).send("Failed to trigger job");
    }
});

app.get("/orders", async (req, res) => {
    try {
        await fetch(`${WORKER_URL}/process/orders`, { method: 'POST' });
        res.json({ message: "Job to fetch and process orders completed." });
    } catch (e) {
        res.status(500).json({ error: "Failed to trigger job", details: e.message });
    }
});

// Since Redis/BullMQ are gone, admin routes are obsolete. 
// I've kept the routes so anyone hitting them gets a clear message instead of a 404.
app.get('/admin/pause-queue', (req, res) => res.status(200).send("Queues removed. N/A"));
app.get('/admin/remove-job', (req, res) => res.status(200).send("Queues removed. N/A"));
app.get('/admin/resume-queue', (req, res) => res.status(200).send("Queues removed. N/A"));
app.get('/admin/stop-all-jobs', (req, res) => res.status(200).send("Queues removed. N/A"));
app.get('/admin/flush-redis', (req, res) => res.status(200).send("Redis removed. N/A"));

app.listen(port, () => {
    console.log(`API Gateway is running on http://localhost:${port}`);
});