import axios from 'axios';
import { formatToDDMMYYYY } from './fetchGMVMaxSpending.js';
import { BigQuery } from '@google-cloud/bigquery';
import { backfillEndDate, backfillStartDate } from './fetchTiktokBasicAds.js';
const bigquery = new BigQuery();

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export async function fetchLiveGMVMax(brand, advertiser_id, sleepValue=4000) {

    await sleep(sleepValue);

    console.log(`[LIVE] GMV MAX - ${brand}`);
    let access_token = process.env.TIKTOK_MARKETING_ACCESS_TOKEN;
    let brandName = brand.toLowerCase().replace(/\s/g, "");
    let tableName = `${brandName}_productgmvmax`;

    const yesterday = new Date();

    yesterday.setDate(yesterday.getDate() - 1);

    const yyyy = yesterday.getFullYear();
    const mm = String(yesterday.getMonth() + 1).padStart(2, '0');
    const dd = String(yesterday.getDate()).padStart(2, '0');
    const yesterdayStr = `${yyyy}-${mm}-${dd}`;

    let storeIdAcc = {
        "eileengrace": "7494055813163943155",
        "shrd": "7494060372131481134",
        "missdaisy": "7494083757647759179",
        "polynia": "7494718012797651378",
        "cleviant": "7495299579063405468",
        "mosseru": "7495297011747293899",
        "mirae": "7495819231306943483",
        "mamaway": "7494499456018189063",
        "chess": "7494919612596259170", 
        "nutribeyond": "7496045913194138312",
        "evoke": "7495667268174318445",
        "drjou": "7495803189501659725",
        "swissvita": "7494835443584567449",
        "gbelle": "7495908629104331053",
        "pastnine": "7495997119882693518",
        "ivylily": "7496045415576275429",
        "naruko": "7496241553706617176"
    }

    let success = false;
    let retries = 10;
    try {   

        while(!success && retries > 0) {

            const params = {
                advertiser_id: advertiser_id,
                store_ids: JSON.stringify([storeIdAcc[brandName]]),
                start_date: yesterdayStr,
                end_date: yesterdayStr,
                dimensions: JSON.stringify(["advertiser_id", "stat_time_day"]),
                metrics: JSON.stringify(["cost", "orders", "net_cost", "gross_revenue"]),
                filtering: JSON.stringify({ gmv_max_promotion_types: ["LIVE"] }),
                page: 1,
                page_size: 1000
            }

            const url = "https://business-api.tiktok.com/open_api/v1.3/gmv_max/report/get/";
            const response = await axios.get(url, {
                headers: {
                    'Access-Token': access_token
                },
                params
            });

            console.log(`[LIVE] response on brand ${brandName}`);
            // console.log(response.data);

            if(response && response.data && response.data.data && response.data.data.list) {
                success = true;
                const costList = response.data.data.list;
                let processedCostList = [];

                costList.forEach(c => {
                    if(c.metrics.cost !== "0") {
                        let costElement = {
                            "date": c.dimensions.stat_time_day,
                            "lgmax_cost": parseInt(c.metrics.cost),
                            "lgmax_gmv": parseInt(c.metrics.gross_revenue)
                        }
                        processedCostList.push(costElement);
                    }
                });

                if(processedCostList) {
                    console.log(`[LIVE] ${brandName} PROCESSEDCOSTLIST EXISTS`);
                    return processedCostList;
                }
            } else {
                retries -= 1;
                console.log(`[LIVE] ${brandName} does not exist. Retries left: ${retries}`);
                console.log("[LIVE] Failed response: ", response?.data);
                if(retries > 0) await sleep(3000)
                else return [];
            }
        }
    } catch (e) {
        retries -= 1;
        console.log(`[LIVE] Error fetching Product GMV Max spending on ${brandName}: ${e}`)
        // --- ACTION: HARD WAIT ON RATE LIMIT ---
        if (e.response?.status === 429 || e.message.includes('40100')) {
             console.log("[PRODUCT] Hit Rate Limit. Sleeping 15s before retry...");
             await sleep(15000);
        } else {
             if(retries > 0) await sleep(5000);
        }

        // --- THE CRITICAL FIX ---
        // If we ran out of retries, THROW THE ERROR.
        // Do NOT let the function finish and return undefined.
        if (retries === 0) {
            throw new Error(`[STRICT MODE] Failed to fetch data for ${brand} after all retries. Failing job to trigger BullMQ backoff.`);
        }
    }
}

async function mergeProductGMVMax(brand, costList) {
    const datasetId = "tiktok_api_us";

    console.log("\n");
    console.log("[LGMVMAX] Data: ", brand);
    console.log("Data: ", costList);
    console.log("\n");
}