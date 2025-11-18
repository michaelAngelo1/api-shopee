import axios from 'axios';
import { BigQuery } from '@google-cloud/bigquery';
const bigquery = new BigQuery();

export function formatToDDMMYYYY(dateString) {
    // Expects input like "2025-11-12 00:00:00"
    const [datePart] = dateString.split(' ');
    const [year, month, day] = datePart.split('-');
    return `${day}-${month}-${year}`;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export async function fetchGMVMaxSpending(brand, advertiser_id) {

    await sleep(1000);

    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yyyy = yesterday.getFullYear();
    const mm = String(yesterday.getMonth() + 1).padStart(2, '0');
    const dd = String(yesterday.getDate()).padStart(2, '0');
    const yesterdayStr = `${yyyy}-${mm}-${dd}`;

    let access_token = process.env.TIKTOK_MARKETING_ACCESS_TOKEN;
    let brandName = brand.toLowerCase().replace(/\s/g, "");
    let tableName = `${brandName}_gmvmax`;
    
    let multiBrandAcc = {
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
    if(
        brand == "Eileen Grace" ||
        brand == "SHRD" ||
        brand == "Miss Daisy" ||
        brand == "Polynia" ||
        brand == "Cleviant" ||
        brand == "Mosseru" ||
        brand == "Mirae"
    ) {
        console.log("Fetching single brand account");
        const url = 'https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/';
        const params = {
            advertiser_id: advertiser_id,
            service_type: 'AUCTION',
            report_type: 'TT_SHOP',
            data_level: 'AUCTION_ADVERTISER',
            dimensions: JSON.stringify(["advertiser_id", "country_code", "stat_time_day"]),
            metrics: JSON.stringify(["spend", "billed_cost"]),
            start_date: yesterdayStr,
            end_date: yesterdayStr,
            page: 1,
            page_size: 100
        };

        try {
            const response = await axios.get(url, {
                headers: {
                    'Access-Token': access_token
                },
                params
            });

            const costList = response.data.data.list;
            let processedCostList = [];

            costList.forEach(c => {
                if(c.metrics.spend !== "0") {
                    let costElement = {
                        "Tanggal_Dibuat": formatToDDMMYYYY(c.dimensions.stat_time_day),
                        "Spending": parseInt(c.metrics.spend)
                    }
                    processedCostList.push(costElement);
                }
            });

            if(processedCostList) {
                await mergeGMVMax(tableName, processedCostList);
            }
        } catch (e) {
            console.error("[SINGLE] Error fetching GMV Max Spending: ", e);
        }
    } else {
        console.log("Fetching multiple brand account: ", brandName);
        const url = 'https://business-api.tiktok.com/open_api/v1.3/gmv_max/report/get/';
        const params = {
            advertiser_id,
            store_ids: JSON.stringify([multiBrandAcc[brandName]]),
            start_date: yesterdayStr,
            end_date: yesterdayStr,
            dimensions: JSON.stringify(["advertiser_id", "stat_time_day"]),
            metrics: JSON.stringify(["cost", "orders", "cost_per_order", "gross_revenue", "roi", "net_cost"]),
            page: 1,
            page_size: 1000
        }

        try {
            let success = false;
            let retries = 3;

            while(!success && retries > 0) {

                const response = await axios.get(url, {
                    headers: {
                        'Access-Token': access_token
                    },
                    params
                });
    
                if(response && response.data && response.data.data && response.data.data.list) {
                    success = true;
                    const costList = response.data.data.list;
                    let processedCostList = [];
        
                    costList.forEach(c => {
                        if(c.metrics.net_cost !== "0") {
                            let costElement = {
                                "Tanggal_Dibuat": formatToDDMMYYYY(c.dimensions.stat_time_day),
                                "Spending": parseInt(c.metrics.net_cost)
                            }
                            processedCostList.push(costElement);
                        }
                    });
        
                    if(processedCostList) {
                        // console.log(`[MULTI] Cost list to merge on ${brandName}\n`);
                        // console.log(processedCostList);
                        await mergeGMVMax(tableName, processedCostList);
                    }
                } else {
                    retries -= 1;
                    console.log(`[MULTI] Response does not exist on brand: ${brand}`)
                    if (retries > 0) await sleep(3000);
                }

            }
        } catch (e) {
            retries -= 1;
            console.error(`[MULTIPLE] Error getting store list on ${brandName}: ${e}, retries left: ${retries}`);
            if (retries > 0) await sleep(3000);
        }
    }
}

async function mergeGMVMax(tableName, costList) {
    const datasetId = "tiktok_api_us";
    console.log("\n");
    console.log("Table name: ", tableName);
    console.log("Cost length: ", costList.length);
    console.log("\n");

    if(tableName == "eileengrace_gmvmax") tableName = "eileen_grace_gmvmax";
    if(tableName == "missdaisy_gmvmax") tableName = "miss_daisy_gmvmax";

    try {
        if (!costList.length) return;

        const dates = costList.map(c => c.Tanggal_Dibuat);
        const query = `
            SELECT Tanggal_Dibuat
            FROM \`${datasetId}.${tableName}\`
            WHERE Tanggal_Dibuat IN UNNEST(@dates)
        `;
        const options = {
            query,
            params: { dates }
        };

        const [rows] = await bigquery.query(options);
        const existingDates = new Set(rows.map(r => r.Tanggal_Dibuat));
        const newRows = costList.filter(c => !existingDates.has(c.Tanggal_Dibuat));

        if (!newRows.length) {
            console.log("All rows already exist, nothing to insert.");
            return;
        }

        await bigquery
            .dataset(datasetId)
            .table(tableName)
            .insert(newRows)

        console.log(`Merged ${newRows.length} cost element to ${tableName} table`);
    } catch (e) {
        console.log(`Failed to merge to bigquery on ${tableName}: ${e}`);
    }
}