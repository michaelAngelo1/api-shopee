import axios from 'axios';
import { formatToDDMMYYYY } from './fetchGMVMaxSpending.js';

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
            console.log(`[BASIC - 1] response on ${brandName}`);
            
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
                end_date: "2025-11-17",
                page: 1,
                page_size: 200
            };

            const response = await axios.get(spendByCurl, {
                headers: {
                    'Access-Token': access_token
                },
                params
            });
            
            console.log(`[BASIC - 2] response on ${brandName}`);
            // console.log(response.data.data.list);

            resData2 = resData2.concat(response.data.data.list);
        } catch (e) {
            console.log(`Failed to get ads spend on campaign level on brand ${brandName}: ${e}`);
        }

        if(resData1.length > 0 && resData2.length > 0) {
            processData(brandName, tableName, resData1, resData2);
        }
    } else {
        console.log("[BASIC] Fetching single brand account");
        
    }
}

function processData(brandName, tableName, resData1, resData2) {
    console.log("Processing data: ", brandName);

    // 1. Define the mapping logic from normalized brandName to Campaign Prefix
    let campaignPrefix;
    switch (brandName) {
        case "nutribeyond":
            campaignPrefix = "NB";
            break;
        case "chess":
            campaignPrefix = "CHESS";
            break;
        case "mamaway":
            campaignPrefix = "MMW";
            break;
        // Add other mappings if necessary, following your multiBrandAcc list
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
        if (campaignName && campaignName.startsWith(campaignPrefix)) {
            const spending = parseInt(reportItem.metrics.spend);
            const dateStr = reportItem.dimensions.stat_time_day;
            
            console.log("CAMPAIGN NAME: ", campaignName, "SPENDING: ", spending);

            // if (spending > 0) {
            filteredSpending.push({
                "Tanggal Dibuat": formatToDDMMYYYY(dateStr),
                "Spending": spending
            });
            // }
        }
    });

    console.log(`Successfully filtered ${filteredSpending.length} records for ${brandName} (Prefix: ${campaignPrefix})`);
    
    // The final result is the array 'filteredSpending'
    console.log("Filtered Spending Data:\n", filteredSpending);
    
    // NOTE: This function would typically return 'filteredSpending' or call the merge function. 
    // Since you only asked for the data structure, we output it to the console.
    // If you need to integrate this with the BigQuery function, you would call it here:
    // mergeGMVMax(tableName, filteredSpending); 
    
    return filteredSpending; 
}