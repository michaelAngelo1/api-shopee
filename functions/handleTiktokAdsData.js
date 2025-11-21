import { BigQuery } from '@google-cloud/bigquery';
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
    "Eileen Grace": "eileen_grace_tiktok_ads"
}

export async function handleTiktokAdsData(basicAdsData, pgmvMaxData, lgmvMaxData, brand) {
    console.log(`Handle Tiktok Ads Brand ${brand}`);

    if (basicAdsData && pgmvMaxData && lgmvMaxData) {
        // 1. Calculate Yesterday
        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() - 1);
        const yyyy = yesterday.getFullYear();
        const mm = String(yesterday.getMonth() + 1).padStart(2, '0');
        const dd = String(yesterday.getDate()).padStart(2, '0');
        const yesterdayStr = `${yyyy}-${mm}-${dd}`;

        // 2. Initialize the single object (No loop needed for 1 day)
        let dailyData = {
            "date": yesterdayStr,
            "basic_cost": 0,
            "pgmax_cost": 0,
            "lgmax_cost": 0
        };

        // 3. Map Data (Using yesterdayStr directly)
        // Check basicAdsData
        const basicMatch = basicAdsData.find((b) => b.date.substring(0, 10) === yesterdayStr);
        if (basicMatch) dailyData.basic_cost = basicMatch.basic_cost;

        // Check pgmvMaxData
        const pgMatch = pgmvMaxData.find((b) => b.date.substring(0, 10) === yesterdayStr);
        if (pgMatch) dailyData.pgmax_cost = pgMatch.pgmax_cost;

        // Check lgmvMaxData
        const lgMatch = lgmvMaxData.find((b) => b.date.substring(0, 10) === yesterdayStr);
        if (lgMatch) dailyData.lgmax_cost = lgMatch.lgmax_cost;

        // 4. Merge (Pass brand for logging)
        // We wrap dailyData in an array [] because insert expects rows
        await mergeTiktokAdsData([dailyData], tableNameMap[brand], brand);
    }
}

// Updated merge function
async function mergeTiktokAdsData(data, tableName, brand) {
    console.log("Merging data to table: ", tableName);
    const datasetId = "tiktok_api_us";

    try {
        // OPTIMIZATION: Insert all rows at once, outside of a loop.
        // BigQuery accepts an array of objects.
        await bigquery
            .dataset(datasetId)
            .table(tableName)
            .insert(data);
            
        console.log(`Successfully merged ${data.length} data to ${tableName}`);
    } catch (e) {
        // Now 'brand' is defined here
        console.log("Error merge tiktok ads data on: ", brand, "error: ", e);
        
        // Helpful for debugging BigQuery partial failures
        if (e.name === 'PartialFailureError') {
             console.log("Partial errors:", JSON.stringify(e.errors, null, 2));
        }
    }
}