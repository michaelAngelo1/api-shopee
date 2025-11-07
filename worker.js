import { fetchAndProcessOrdersMD } from './sample-fetch/md_processor.js';
import { fetchAndProcessOrders } from './processor.js';
import { fetchAndProcessOrdersSHRD } from './sample-fetch/shrd_processor.js';
import { Worker } from 'bullmq';
import 'dotenv/config';
import express from 'express';
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

const mdOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-md':
            return fetchAndProcessOrdersMD();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const mdWorker = new Worker("fetch-orders-md", mdOrderProcessor, workerOptions);

const shrdOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-shrd':
            return fetchAndProcessOrdersSHRD();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const shrdWorker = new Worker("fetch-orders-shrd", shrdOrderProcessor, workerOptions);

const clevOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-clev':
            return fetchAndProcessOrdersCLEV();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const clevWorker = new Worker("fetch-orders-clev", clevOrderProcessor, workerOptions);

const drjouOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-drjou':
            return fetchAndProcessOrdersDRJOU();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const drjouWorker = new Worker("fetch-orders-drjou", drjouOrderProcessor, workerOptions);

const mossOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-moss':
            return fetchAndProcessOrdersMOSS();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const mossWorker = new Worker("fetch-orders-moss", mossOrderProcessor, workerOptions);

const gbOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-gb':
            return fetchAndProcessOrdersGB();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const gbWorker = new Worker("fetch-orders-gb", gbOrderProcessor, workerOptions);

const ilOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-il':
            return fetchAndProcessOrdersIL();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const ilWorker = new Worker("fetch-orders-il", ilOrderProcessor, workerOptions);

const evOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-evoke':
            return fetchAndProcessOrdersEVOKE();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const evWorker = new Worker("fetch-orders-evoke", evOrderProcessor, workerOptions);

const mmwOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-mmw':
            return fetchAndProcessOrdersMMW();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const mmwWorker = new Worker("fetch-orders-mmw", mmwOrderProcessor, workerOptions);

const chessOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-chess':
            return fetchAndProcessOrdersCHESS();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const chessWorker = new Worker("fetch-orders-chess", chessOrderProcessor, workerOptions);

const svOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-sv':
            return fetchAndProcessOrdersSV();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const svWorker = new Worker("fetch-orders-sv", svOrderProcessor, workerOptions);

const pnOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-pn':
            return fetchAndProcessOrdersPN();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const pnWorker = new Worker("fetch-orders-pn", pnOrderProcessor, workerOptions);

const nbOrderProcessor = async (job) => {
    switch (job.name) {
        case 'fetch-orders-nb':
            return fetchAndProcessOrdersNB();
        default:
            throw new Error(`Unknown job name: ${job.name}`);
    }
}
const nbWorker = new Worker("fetch-orders-nb", nbOrderProcessor, workerOptions);

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
    console.log(`[MD] ACTIVE: Job ${job.id}.`);
});
mdWorker.on('completed', (job) => {
    console.log(`[MD] COMPLETED: Job ${job.id}.`);
});
mdWorker.on('ready', (job) => {
    console.log("[MD] MD Worker is ready to listen");
});
mdWorker.on('failed', (job, err) => {
    console.error(`[MD] FAILED: Job ${job.id}.`, err);
});

// SHRD
shrdWorker.on('active', (job) => {
    console.log(`[SHRD] ACTIVE: Job ${job.id}.`);
});
shrdWorker.on('completed', (job) => {
    console.log(`[SHRD] COMPLETED: Job ${job.id}.`);
});
shrdWorker.on('ready', (job) => {
    console.log("[SHRD] SHRD Worker is ready to listen");
});
shrdWorker.on('failed', (job, err) => {
    console.error(`[SHRD] FAILED: Job ${job.id}.`, err);
});

// Cleviant
clevWorker.on('active', (job) => {
    console.log(`[CLEV] ACTIVE: Job ${job.id}.`);
});
clevWorker.on('completed', (job) => {
    console.log(`[CLEV] COMPLETED: Job ${job.id}.`);
});
clevWorker.on('ready', (job) => {
    console.log("[CLEV] CLEV Worker is ready to listen");
});
clevWorker.on('failed', (job, err) => {
    console.error(`[CLEV] FAILED: Job ${job.id}.`, err);
});

// Dr. Jou
drjouWorker.on('active', (job) => {
    console.log(`[DRJOU] ACTIVE: Job ${job.id}.`);
});
drjouWorker.on('completed', (job) => {
    console.log(`[DRJOU] COMPLETED: Job ${job.id}.`);
});
drjouWorker.on('ready', (job) => {
    console.log("[DRJOU] DRJOU Worker is ready to listen");
});
drjouWorker.on('failed', (job, err) => {
    console.error(`[DRJOU] FAILED: Job ${job.id}.`, err);
});

// Mosseru
mossWorker.on('active', (job) => {
    console.log(`[MOSS] ACTIVE: Job ${job.id}.`);
});
mossWorker.on('completed', (job) => {
    console.log(`[MOSS] COMPLETED: Job ${job.id}.`);
});
mossWorker.on('ready', (job) => {
    console.log("[MOSS] MOSS Worker is ready to listen");
});
mossWorker.on('failed', (job, err) => {
    console.error(`[MOSS] FAILED: Job ${job.id}.`, err);
});

// G-Belle
gbWorker.on('active', (job) => {
    console.log(`[GBELLE] ACTIVE: Job ${job.id}.`);
});
gbWorker.on('completed', (job) => {
    console.log(`[GBELLE] COMPLETED: Job ${job.id}.`);
});
gbWorker.on('ready', (job) => {
    console.log("[GBELLE] GBELLE Worker is ready to listen");
});
gbWorker.on('failed', (job, err) => {
    console.error(`[GBELLE] FAILED: Job ${job.id}.`, err);
});

// Ivy & Lily
ilWorker.on('active', (job) => {
    console.log(`[IVYLILY] ACTIVE: Job ${job.id}.`);
});
ilWorker.on('completed', (job) => {
    console.log(`[IVYLILY] COMPLETED: Job ${job.id}.`);
});
ilWorker.on('ready', (job) => {
    console.log("[IVYLILY] IVYLILY Worker is ready to listen");
});
ilWorker.on('failed', (job, err) => {
    console.error(`[IVYLILY] FAILED: Job ${job.id}.`, err);
});

// Evoke
evWorker.on('active', (job) => {
    console.log(`[EVOKE] ACTIVE: Job ${job.id}.`);
});
evWorker.on('completed', (job) => {
    console.log(`[EVOKE] COMPLETED: Job ${job.id}.`);
});
evWorker.on('ready', (job) => {
    console.log("[EVOKE] EVOKE Worker is ready to listen");
});
evWorker.on('failed', (job, err) => {
    console.error(`[EVOKE] FAILED: Job ${job.id}.`, err);
});

// MMW
mmwWorker.on('active', (job) => {
    console.log(`[MMW] ACTIVE: Job ${job.id}.`);
});
mmwWorker.on('completed', (job) => {
    console.log(`[MMW] COMPLETED: Job ${job.id}.`);
});
mmwWorker.on('ready', (job) => {
    console.log("[MMW] MMW Worker is ready to listen");
});
mmwWorker.on('failed', (job, err) => {
    console.error(`[MMW] FAILED: Job ${job.id}.`, err);
});

// CHESS
chessWorker.on('active', (job) => {
    console.log(`[CHESS] ACTIVE: Job ${job.id}.`);
});
chessWorker.on('completed', (job) => {
    console.log(`[CHESS] COMPLETED: Job ${job.id}.`);
});
chessWorker.on('ready', (job) => {
    console.log("[CHESS] CHESS Worker is ready to listen");
});
chessWorker.on('failed', (job, err) => {
    console.error(`[CHESS] FAILED: Job ${job.id}.`, err);
});

// SV
svWorker.on('active', (job) => {
    console.log(`[SV] ACTIVE: Job ${job.id}.`);
});
svWorker.on('completed', (job) => {
    console.log(`[SV] COMPLETED: Job ${job.id}.`);
});
svWorker.on('ready', (job) => {
    console.log("[SV] SV Worker is ready to listen");
});
svWorker.on('failed', (job, err) => {
    console.error(`[SV] FAILED: Job ${job.id}.`, err);
});

// PN
pnWorker.on('active', (job) => {
    console.log(`[PN] ACTIVE: Job ${job.id}.`);
});
pnWorker.on('completed', (job) => {
    console.log(`[PN] COMPLETED: Job ${job.id}.`);
});
pnWorker.on('ready', (job) => {
    console.log("[PN] PN Worker is ready to listen");
});
pnWorker.on('failed', (job, err) => {
    console.error(`[PN] FAILED: Job ${job.id}.`, err);
});

// NB
nbWorker.on('active', (job) => {
    console.log(`[NB] ACTIVE: Job ${job.id}.`);
});
nbWorker.on('completed', (job) => {
    console.log(`[NB] COMPLETED: Job ${job.id}.`);
});
nbWorker.on('ready', (job) => {
    console.log("[NB] NB Worker is ready to listen");
});
nbWorker.on('failed', (job, err) => {
    console.error(`[NB] FAILED: Job ${job.id}.`, err);
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