import { fetchAndProcessOrders } from "../processor.js";
import { fetchAndProcessOrdersCHESS } from "../workers/chess_processor.js";
import { fetchAndProcessOrdersCLEV } from "../workers/clev_processor.js";
import { fetchAndProcessOrdersDRJOU } from "../workers/drjou_processor.js";
import { fetchAndProcessOrdersEVOKE } from "../workers/evoke_processor.js";
import { fetchAndProcessOrdersGB } from "../workers/gb_processor.js";
import { fetchAndProcessOrdersIL } from "../workers/il_processor.js";
import { fetchAndProcessOrdersMD } from "../workers/md_processor.js";
import { fetchAndProcessOrdersMIRAE } from "../workers/mirae_processor.js";
import { fetchAndProcessOrdersMMW } from "../workers/mmw_processor.js";
import { fetchAndProcessOrdersMOSS } from "../workers/moss_processor.js";
import { fetchAndProcessOrdersNB } from "../workers/nb_processor.js";
import { fetchNewBrands } from "../workers/newBrandsProcessor.js";
import { fetchAndProcessOrdersPN } from "../workers/pn_processor.js";
import { fetchAndProcessOrdersPOLY } from "../workers/poly_processor.js";
import { fetchAndProcessOrdersSHRD } from "../workers/shrd_processor.js";
import { fetchAndProcessOrdersSV } from "../workers/sv_processor.js";
import { mainM2 } from "../workers/m2_processor.js";
import { mainTiktokAffiliate, testAffiliateM2 } from "./handleTiktokAffiliate.js";

const DELAY_MS = 5000;
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// refetch all on main
async function refetchAll() {
    try {
        const tasks = [
            // { name: 'Eileen Grace', fn: fetchAndProcessOrders },
            // { name: 'Mamaway', fn: fetchAndProcessOrdersMMW },
            // { name: 'SHRD', fn: fetchAndProcessOrdersSHRD },
            // { name: 'Miss Daisy', fn: fetchAndProcessOrdersMD },
            // { name: 'CHESS', fn: fetchAndProcessOrdersCHESS },
            // { name: 'Polynia', fn: fetchAndProcessOrdersPOLY },
            // { name: 'Cléviant', fn: fetchAndProcessOrdersCLEV },
            // { name: 'Mossèru', fn: fetchAndProcessOrdersMOSS },
            // { name: 'Evoke', fn: fetchAndProcessOrdersEVOKE },
            // { name: 'Dr Jou', fn: fetchAndProcessOrdersDRJOU },
            // { name: 'Mirae', fn: fetchAndProcessOrdersMIRAE },
            // { name: 'Swissvita', fn: fetchAndProcessOrdersSV },
            // { name: 'G-Belle', fn: fetchAndProcessOrdersGB },
            // { name: 'Past Nine', fn: fetchAndProcessOrdersPN },
            // { name: 'Nutri & Beyond', fn: fetchAndProcessOrdersNB },
            // { name: 'Ivy & Lily', fn: fetchAndProcessOrdersIL },
            // { name: "M2", fn: mainM2 },
            { name: "New Brands w/o M2", fn: fetchNewBrands },
            // { name: "Tiktok Affiliate", fn: mainTiktokAffiliate },
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
        console.log("Error refetch all: ", e);
    }
}

await refetchAll();

// Third run. Expected result: 
// 1. May 1 - 6 Tiktok ads data & PGMV Max breakdown data done
// 2. Shopee ads: March 1 - 30. 

// After run, do not forget to:
// 1. Change back start and end date
// 2. Check credentials in Cloud Run
