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
import { fetchAffiliateData } from "./amsProcessor.js";

const DELAY_MS = 5000;
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// refetch all on main
async function refetchAll() {
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
            { name: "M2", fn: mainM2 },
            { name: "New Brands w/o M2", fn: fetchNewBrands },
            { name: "Tiktok Affiliate", fn: mainTiktokAffiliate },
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

export async function fetchAllAffiliateShopee() {
    const brands = [
        { brand: "Eileen Grace",    shop_id: parseInt(process.env.SHOP_ID),          sleepValue: 1000 },
        { brand: "Mamaway",         shop_id: parseInt(process.env.MMW_SHOP_ID),       sleepValue: 1500 },
        { brand: "SH-RD",           shop_id: parseInt(process.env.SHRD_SHOP_ID),      sleepValue: 2000 },
        { brand: "Miss Daisy",      shop_id: parseInt(process.env.MD_SHOP_ID),        sleepValue: 2500 },
        { brand: "Chess",           shop_id: parseInt(process.env.CHESS_SHOP_ID),     sleepValue: 3500 },
        { brand: "Cleviant",        shop_id: parseInt(process.env.CLEVIANT_SHOP_ID),  sleepValue: 4000 },
        { brand: "Mosseru",         shop_id: parseInt(process.env.MOSS_SHOP_ID),      sleepValue: 5000 },
        { brand: "Evoke",           shop_id: parseInt(process.env.EVOKE_SHOP_ID),     sleepValue: 5500 },
        { brand: "Dr.Jou",          shop_id: parseInt(process.env.DRJOU_SHOP_ID),     sleepValue: 6000 },
        { brand: "Mirae",           shop_id: parseInt(process.env.MIRAE_SHOP_ID),     sleepValue: 6500 },
        { brand: "Swissvita",       shop_id: parseInt(process.env.SV_SHOP_ID),        sleepValue: 7000 },
        { brand: "Polynia",         shop_id: parseInt(process.env.POLY_SHOP_ID),      sleepValue: 7400 },
        { brand: "G-Belle",         shop_id: parseInt(process.env.GB_SHOP_ID),        sleepValue: 7500 },
        { brand: "Past Nine",       shop_id: parseInt(process.env.PN_SHOP_ID),        sleepValue: 8000 },
        { brand: "Nutri & Beyond",  shop_id: parseInt(process.env.NB_SHOP_ID),        sleepValue: 8500 },
        { brand: "Ivy & Lily",      shop_id: parseInt(process.env.IL_SHOP_ID),        sleepValue: 9000 },
        { brand: "Naruko",          shop_id: 1638001566 },
        { brand: "Relove",          shop_id: 1684312913 },
        { brand: "Joey & Roo",      shop_id: 1682176843 },
    ];

    for (const { brand, shop_id, sleepValue } of brands) {
        try {
            await fetchAffiliateData(brand, shop_id, sleepValue);
        } catch (err) {
            console.error(`[fetchAllAffiliateShopee] Failed for ${brand}:`, err);
        }
    }
}

// Backfill 2026-05-12 to 2026-05-14 due to changing API requirements
// Later: change all 2026-05-12 and 2026-05-12 to yesterdayStr or similar, before deploying again. 
// await refetchAll();
