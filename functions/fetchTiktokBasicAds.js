import axios from 'axios';
import { formatToDDMMYYYY } from './fetchGMVMaxSpending.js';
import { BigQuery } from '@google-cloud/bigquery';
const bigquery = new BigQuery();

export async function fetchTiktokBasicAds(brand, advertiser_id) {
    let multiBrandAcc = [
        "mamaway",
        "chess",
        "nutribeyond",
        "evoke",
        "drjou",
        "swissvita",
        "gbelle",
        "pastnine",
        "ivylily",
        "naruko"
    ];

    const access_token = process.env.TIKTOK_MARKETING_ACCESS_TOKEN;
    let brandName = brand.toLowerCase().replace(/\s/g, "");
    let tableName = `${brandName}_basicads`;
    
    if(multiBrandAcc.includes(brandName)) {

        // 1. Get campaigns by advertiser_id
        const cbyAurl = "https://business-api.tiktok.com/open_api/v1.3/campaign/get/"

        // 2. Get ad spend by advertiser_id on campaign level
        const spendByCurl = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/";
        
        let resData1 = [];
        let resData2 = [];

        // 1
        try {
            const params = { advertiser_id };
            const response = await axios.get(cbyAurl, {
                headers: {
                    'Access-Token': access_token
                },
                params
            });
            resData1 = resData1.concat(response.data.data.list);
        } catch (e) {
            console.log(`Failed to get campaigns by ads id on brand ${brandName}: ${e}`);
        }

        // 2
        try {
            const params = {
                advertiser_id: advertiser_id,
                service_type: "AUCTION",
                report_type: "BASIC",
                data_level: "AUCTION_CAMPAIGN",
                dimensions: JSON.stringify(["campaign_id", "stat_time_day"]),
                metrics: JSON.stringify(["spend", "impressions", "reach"]),
                start_date: "2025-11-01",
                end_date: "2025-11-18",
                page: 1,
                page_size: 200
            };

            const response = await axios.get(spendByCurl, {
                headers: {
                    'Access-Token': access_token
                },
                params
            });
            
            // console.log(`[BASIC - GROUPED] response on ${brandName}`);
            // console.log(response.data.data.list);

            resData2 = resData2.concat(response.data.data.list);
        } catch (e) {
            console.log(`Failed to get ads spend on campaign level on brand ${brandName}: ${e}`);
        }

        if(resData1.length > 0 && resData2.length > 0) {
            await processData(brandName, tableName, resData1, resData2);
        }
    } else {
        console.log("[BASIC] Fetching single brand account");
        
        const singleUrl = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/";

        try {
            const params = {
                advertiser_id: advertiser_id,
                service_type: "AUCTION",
                report_type: "BASIC",
                data_level: "AUCTION_ADVERTISER",
                dimensions: JSON.stringify(["advertiser_id", "stat_time_day"]),
                metrics: JSON.stringify(["spend", "impressions", "reach"]),
                start_date: "2025-11-01",
                end_date: "2025-11-18",
                page: 1,
                page_size: 200
            }

            const response = await axios.get(singleUrl, {
                headers: {
                    'Access-Token': access_token
                },
                params
            });

            console.log(`[BASIC-SINGLE] Res on ${brandName}`);
            
            if(response && response.data && response.data.data && response.data.data.list) {
                const costList = response.data.data.list;
                let singleCostList = [];

                costList.forEach(c => {
                    if(c.metrics.spend !== "0") {
                        let costElement = {
                            "Tanggal_Dibuat": formatToDDMMYYYY(c.dimensions.stat_time_day),
                            "Spending": parseInt(c.metrics.spend),
                            "Reach": parseInt(c.metrics.reach),
                            "Impressions": parseInt(c.metrics.impressions)
                        }
                        singleCostList.push(costElement);
                    }
                });

                if(singleCostList) {
                    await mergeBasicAds(tableName, singleCostList);
                }
            } else {
                console.log(`[BASIC-SINGLE] Data on brand ${brandName} does not exist.`);
            }
        } catch (e) {
            console.log(`Error getting basic ads data on brand: ${brandName}: ${e}`);
        }
    }
}

async function processData(brandName, tableName, resData1, resData2) {
    console.log("Processing data: ", brandName);

    // 1. Define the mapping logic from normalized brandName to Campaign Prefix
    let campaignPrefixes = [];
    switch (brandName) {
        case "nutribeyond":
            campaignPrefixes = ["NB"];
            break;
        case "chess":
            campaignPrefixes = ["CHESS"];
            break;
        case "mamaway":
            campaignPrefixes = ["MMW", "Mamaway", "MAMAWAY"];
            break;
        case "evoke":
            campaignPrefixes = ["Evoke"];
            break;
        case "drjou":
            campaignPrefixes = ["Dr Jou"];
            break;
        case "swissvita":
            campaignPrefixes = ["Swissvita", "SWVT"];
            break;
        case "gbelle":
            campaignPrefixes = ["Gbelle"];
            break;
        case "pastnine":
            campaignPrefixes = ["Past Nine", "Past 9"];
            break;
        case "ivylily":
            campaignPrefixes = ["Ivy & Lily", "IL"];
            break;
        case "naruko":
            campaignPrefixes = ["Naruko"];
            break;
        default:
            console.warn(`No specific campaign prefix defined for brand: ${brandName}`);
            return [];
    }

    // 2. Create a Map of Campaign IDs to Campaign Names (for quick lookup)
    const campaignIdToNameMap = new Map();
    resData1.forEach(campaign => {
        console.log("[PROCESS] CAMPAIGN NAME: ", campaign.campaign_name);
        campaignIdToNameMap.set(campaign.campaign_id, campaign.campaign_name);
    });

    // 3. Filter and Transform the Spend Data (resData2)
    const filteredSpending = [];

    resData2.forEach(reportItem => {
        const campaignId = reportItem.dimensions.campaign_id;
        const campaignName = campaignIdToNameMap.get(campaignId);

        // Check if the campaign name exists and starts with the required prefix
        if (campaignName) {
            const isMatch = campaignPrefixes.some(prefix => campaignName.startsWith(prefix));

            if(isMatch) {
                const spending = parseInt(reportItem.metrics.spend);
                const dateStr = reportItem.dimensions.stat_time_day;
                
                // console.log("CAMPAIGN NAME: ", campaignName, "SPENDING: ", spending);
    
                if(spending > 0) {
                    filteredSpending.push({
                        "Tanggal_Dibuat": formatToDDMMYYYY(dateStr),
                        "Spending": spending,
                        "Reach": parseInt(reportItem.metrics.reach),
                        "Impressions": parseInt(reportItem.metrics.impressions)
                    });
                }
            }
        }
    });

    console.log(`Successfully filtered ${filteredSpending.length} records for ${brandName} (Prefix: ${campaignPrefixes})`);
    
    await mergeBasicAds(tableName, filteredSpending);
    
    return filteredSpending; 
}

async function mergeBasicAds(tableName, data) {
    
    const datasetId = "tiktok_api_us";
    
    if(tableName == "eileengrace_basicads") tableName = "eileen_grace_basicads";
    
    console.log("\n");
    console.log("Merging data to: ", tableName);
    console.log("Data length: ", data.length);
    console.log("\n");
    
    try {
        if(!data.length) return;

        const dates = data.map(c => c.Tanggal_Dibuat);
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
        const newRows = data.filter(c => !existingDates.has(c.Tanggal_Dibuat));

        if(!newRows.length) {
            console.log("[BASIC] All rows already exist. Nothing to insert.");
        }

        // console.log(`${brandName} new rows: ${newRows}`);
        await bigquery
            .dataset(datasetId)
            .table(tableName)
            .insert(newRows)
        
        console.log(`[BASIC] Merged ${newRows.length} cost element to ${tableName} table`);
    } catch (e) {
        console.log(`[BASIC] Failed to merge to bigquery on ${tableName}: ${e}`);
    }
}