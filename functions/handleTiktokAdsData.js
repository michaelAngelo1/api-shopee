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
    if(basicAdsData && pgmvMaxData && lgmvMaxData) {

        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() - 1);
        const yyyy = yesterday.getFullYear();
        const mm = String(yesterday.getMonth() + 1).padStart(2, '0');
        const dd = String(yesterday.getDate()).padStart(2, '0');
        const yesterdayStr = `${yyyy}-${mm}-${dd}`;

        let dataTiktokAds = [];
        // Process basicAdsData first, then pgmvMax, then lgmvMax

        let startDate = new Date("2025-11-01");
        let endDate = new Date("2025-11-20");
        let currentDate = new Date(startDate);

        while(currentDate <= endDate) {

            let tiktokAds = {
                "date": currentDate.toISOString().substring(0, 10),
                "basic_cost": 0,
                "pgmax_cost": 0,
                "lgmax_cost": 0
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
            }
        });

        // Process lgmvMaxData
        dataTiktokAds.forEach((d) => {
            const match = lgmvMaxData.find((b) => b.date.substring(0, 10) === d.date);
            if(match) {
                d.lgmax_cost = match.lgmax_cost;
            }
        });

        await mergeTiktokAdsData(dataTiktokAds, tableNameMap[brand]);
    }
}

async function mergeTiktokAdsData(data, tableName) {
    console.log("Merging data to table: ", tableName);

    const datasetId = "tiktok_api_us";

    try {
        for(const d of data) {
            await bigquery
                .dataset(datasetId)
                .table(tableName)
                .insert({
                    date: d.date,
                    basic_cost: d.basic_cost, 
                    pgmax_cost: d.pgmax_cost,
                    lgmax_cost: d.lgmax_cost
                });
        }
        console.log(`Successfully merged ${data.length} data to ${tableName}`);
    } catch (e) {
        console.log("Error merge tiktok ads data on: ", brand, "error: ", e);
    }
}