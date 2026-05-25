import { BigQuery } from '@google-cloud/bigquery';
import { backfillEndDate, backfillStartDate } from './fetchTiktokBasicAds.js';
const bigquery = new BigQuery();

let tableNameMap = {
    "Chess": "chess_tiktok_ads",
    "Cleviant": "cleviant_tiktok_ads",
    "Dr.Jou": "dr_jou_tiktok_ads",
    "Evoke": "evoke_tiktok_ads",
    "G-Belle": "gbelle_tiktok_ads",
    "Ivy & Lily": "ivy_lily_tiktok_ads",
    "Naruko": "naruko_tiktok_ads",
    "Miss Daisy": "miss_daisy_tiktok_ads",
    "Mirae": "mirae_tiktok_ads",
    "Mamaway": "mamaway_tiktok_ads",
    "Mosseru": "mosseru_tiktok_ads",
    "Nutri & Beyond": "nutri_beyond_tiktok_ads",
    "Past Nine": "past_nine_tiktok_ads",
    "Polynia": "polynia_tiktok_ads",
    "SHRD": "shrd_tiktok_ads",
    "Swissvita": "swissvita_tiktok_ads",
    "Eileen Grace": "eileen_grace_tiktok_ads",
    "Rocketindo Shop": "rocketindo_shop_tiktok_ads",
    "Relove": "relove_tiktok_ads",
    "Joey & Roo": "joey_roo_tiktok_ads",
    "Enchante": "enchante_tiktok_ads",
    "M2": "m2_tiktok_ads"
}

export async function handleTiktokAdsData(basicAdsData, pgmvMaxData, lgmvMaxData, brand) {
    console.log(`Handle Tiktok Ads Brand ${brand}`);

    if(basicAdsData === undefined || pgmvMaxData === undefined || lgmvMaxData === undefined) {
        console.log("[TIKTOK-ADS] Merge aborted due to missing or undefined data. On: ", brand);
        throw new Error(`[TIKTOK-ADS] Merge aborted due to missing or undefined data. On: ${brand}`)
    }

    const yesterday = new Date();

    yesterday.setDate(yesterday.getDate() - 1);
    
    const yyyy = yesterday.getFullYear();
    const mm = String(yesterday.getMonth() + 1).padStart(2, '0');
    const dd = String(yesterday.getDate()).padStart(2, '0');
    const yesterdayStr = `${yyyy}-${mm}-${dd}`;

    let dataTiktokAds = [];
    
    let currentDate = new Date(yesterdayStr);
    let endDate = new Date(yesterdayStr);

    // let currentDate = new Date(yesterdayStr);
    // let endDate = new Date(yesterdayStr);

    while(currentDate <= endDate) {
        let tiktokAds = {
            "date": currentDate.toISOString().substring(0, 10),
            "basic_cost": 0,
            "pgmax_cost": 0,
            "lgmax_cost": 0,
            "pgmax_gmv": 0,
            "lgmax_gmv": 0,
        }
        dataTiktokAds.push(tiktokAds);
        currentDate.setDate(currentDate.getDate() + 1);
    }

    // Process basicAdsData
    dataTiktokAds.forEach((d) => {
        const match = basicAdsData.find((b) => b.date.substring(0, 10) === d.date);
        if(match) {
            d.basic_cost = match.basic_cost;
        }
    });

    // Process pgmvMaxData
    dataTiktokAds.forEach((d) => {
        const match = pgmvMaxData.find((b) => b.date.substring(0, 10) === d.date);
        if(match) {
            d.pgmax_cost = match.pgmax_cost;
            d.pgmax_gmv = match.pgmax_gmv;
        }
    });

    // Process lgmvMaxData
    dataTiktokAds.forEach((d) => {
        const match = lgmvMaxData.find((b) => b.date.substring(0, 10) === d.date);
        if(match) {
            d.lgmax_cost = match.lgmax_cost;
            d.lgmax_gmv = match.lgmax_gmv;
        }
    });

    console.log("TO MERGE - Data Tiktok Ads: ", brand);
    // Added 'brand' to arguments so logging works
    await mergeTiktokAdsData(dataTiktokAds, tableNameMap[brand], brand);
}

async function mergeTiktokAdsData(data, tableName, brand) {
    console.log("Merging data to table: ", tableName);

    const datasetId = "tiktok_api_us";

    try {
        for(const d of data) {
            // --- NEW: DUPLICATION CHECK ---
            // Check if this date already exists in the table
            const checkQuery = `
                SELECT date 
                FROM \`${datasetId}.${tableName}\` 
                WHERE date = '${d.date}'
            `;
            const [existingRows] = await bigquery.query({ query: checkQuery });

            if (existingRows.length > 0) {
                console.log(`[SKIP] Data for ${d.date} already exists in ${tableName}. Skipping to prevent duplication.`);
                continue; // Skip this insertion
            }

            // If not exists, proceed to insert
            await bigquery
                .dataset(datasetId)
                .table(tableName)
                .insert({
                    date: d.date,
                    basic_cost: d.basic_cost, 
                    pgmax_cost: d.pgmax_cost,
                    lgmax_cost: d.lgmax_cost,
                    pgmax_gmv: d.pgmax_gmv,
                    lgmax_gmv: d.lgmax_gmv,
                    process_dttm: new Date(Date.now() + 7 * 60 * 60 * 1000).toISOString().replace('T', ' ').substring(0, 19)
                });
        }
        console.log(`Successfully processed ${data.length} row(s) for ${tableName}`);
    } catch (e) {
        console.log("Error merge tiktok ads data on: ", brand, "error: ", e);
    }
}
