import { fetchAndProcessOrders } from './processor.js';
import { fetchAndProcessOrdersMMW } from './workers/mmw_processor.js';
import { fetchAndProcessOrdersSHRD } from './workers/shrd_processor.js';
import { fetchAndProcessOrdersMD } from './workers/md_processor.js';
import { fetchAndProcessOrdersCHESS } from './workers/chess_processor.js';
import { fetchAndProcessOrdersPOLY } from './workers/poly_processor.js';
import { fetchAndProcessOrdersCLEV } from './workers/clev_processor.js';
import { fetchAndProcessOrdersMOSS } from './workers/moss_processor.js';
import { fetchAndProcessOrdersEVOKE } from './workers/evoke_processor.js';
import { fetchAndProcessOrdersDRJOU } from './workers/drjou_processor.js';
import { fetchAndProcessOrdersMIRAE } from './workers/mirae_processor.js';
import { fetchAndProcessOrdersSV } from './workers/sv_processor.js';
import { fetchAndProcessOrdersGB } from './workers/gb_processor.js';
import { fetchAndProcessOrdersPN } from './workers/pn_processor.js';
import { fetchAndProcessOrdersNB } from './workers/nb_processor.js';
import { fetchAndProcessOrdersIL } from './workers/il_processor.js';

import { mainTiktokFinance } from './functions/handleFinance.js';
import 'dotenv/config';
import express from 'express';
import { mainTransactionsBreakdown } from './functions/transactionsBreakdown.js';

const workerApp = express();
const port = process.env.PORT || 8080;
const DELAY_MS = 5000;

workerApp.use(express.json());

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

workerApp.get('/', (req, res) => res.status(200).send("Worker is healthy"));

workerApp.post('/process/transactions-breakdown', async (req, res) => {
    res.status(200).send("Transactions Breakdown is running in the background");
    try {
        await mainTransactionsBreakdown();
        res.status(200).send("Transactions Breakdown Completed")
    } catch (e) {
        console.log("[transactions-breakdown] Failed: ", e);
        res.status(500).send("Failed");
    }
});

workerApp.post('/process/tiktok-withdrawal', async (req, res) => {
    res.status(200).send("Tiktok Withdrawal is starting in background.");
    try {
        await mainTiktokFinance();
        res.status(200).send("Tiktok Withdrawal Completed");
    } catch (err) {
        console.error('[tiktok-withdrawal] Failed:', err);
        res.status(500).send("Failed");
    }
});

workerApp.post('/process/orders', async (req, res) => {
    try {
        await fetchAndProcessOrders();
        res.status(200).send("Completed");
    } catch (err) {
        console.error('[manual-fetch] Failed:', err);
        res.status(500).send("Failed");
    }
});

workerApp.post('/process/daily-sync', async (req, res) => {
    res.status(200).send("Daily sync acknowledged and starting in background.");
    try {
        const tasks = [
            { name: 'Eileen Grace', fn: fetchAndProcessOrders },
            { name: 'Mamaway', fn: fetchAndProcessOrdersMMW },
            { name: 'SHRD', fn: fetchAndProcessOrdersSHRD },
            { name: 'Miss Daisy', fn: fetchAndProcessOrdersMD },
            { name: 'CHESS', fn: fetchAndProcessOrdersCHESS },
            { name: 'Polynia', fn: fetchAndProcessOrdersPOLY },
            { name: 'Cléviant', fn: fetchAndProcessOrdersCLEV },
            { name: 'Mossèru', fn: fetchAndProcessOrdersMOSS },
            { name: 'Evoke', fn: fetchAndProcessOrdersEVOKE },
            { name: 'Dr Jou', fn: fetchAndProcessOrdersDRJOU },
            { name: 'Mirae', fn: fetchAndProcessOrdersMIRAE },
            { name: 'Swissvita', fn: fetchAndProcessOrdersSV },
            { name: 'G-Belle', fn: fetchAndProcessOrdersGB },
            { name: 'Past Nine', fn: fetchAndProcessOrdersPN },
            { name: 'Nutri & Beyond', fn: fetchAndProcessOrdersNB },
            { name: 'Ivy & Lily', fn: fetchAndProcessOrdersIL },
        ];

        for (const task of tasks) {
            try {
                await task.fn();
            } catch (err) {
                console.error(`[${task.name}] Failed:`, err);
            }
            
            if (task !== tasks[tasks.length - 1]) {
                await delay(DELAY_MS);
            }
        }
    } catch (e) {
        console.error("Critical error in daily sync pipeline:", e);
        if (!res.headersSent) res.status(500).send("Pipeline failed");
    }
});

workerApp.listen(port, () => console.log("Worker server listening on port: ", port));