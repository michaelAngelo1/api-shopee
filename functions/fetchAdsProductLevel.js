import axios from 'axios';
import crypto from 'crypto';
import { fetchAdsTotalBalance } from './fetchAdsTotalBalance.js';
import { BigQuery } from '@google-cloud/bigquery';
import { backfillEndDate, backfillStartDate } from './fetchTiktokBasicAds.js';
const bigquery = new BigQuery();

export async function fetchAdsProductLevel(brand, partner_id, partner_key, access_token, shop_id) {
    let brandName = brand.toLowerCase().replace(/\s/g, "");
    let tableName = {
        "eileengrace": "eileen_grace_product_ads",
        "shrd": "shrd_product_ads",
        "missdaisy": "miss_daisy_product_ads",
        "polynia": "polynia_product_ads",
        "cleviant": "cleviant_product_ads",
        "mosseru": "mosseru_product_ads",
        "mirae": "mirae_product_ads",
        "mamaway": "mamaway_product_ads",
        "chess": "chess_product_ads", 
        "nutribeyond": "nutri_beyond_product_ads",
        "evoke": "evoke_product_ads",
        "drjou": "dr_jou_product_ads",
        "swissvita": "swissvita_product_ads",
        "gbelle": "gbelle_product_ads",
        "pastnine": "past_nine_product_ads",
        "ivylily": "ivy_lily_product_ads",
        "naruko": "naruko_product_ads"
    }

    const HOST = "https://partner.shopeemobile.com";

    // 1. Get campaign id list
    const campaignIdPath = "/api/v2/ads/get_product_level_campaign_id_list";

    // 2. Get product-level campaign daily performance
    const productCampaignPath = "/api/v2/ads/get_product_campaign_daily_performance";

    const yesterday = new Date(Date.now() - 86400000 * 1);
    const day = String(yesterday.getDate()).padStart(2, '0');
    const month = String(yesterday.getMonth() + 1).padStart(2, '0'); 
    const year = yesterday.getFullYear();
    const yesterdayString = `${day}-${month}-${year}`;
    
    // Get campaign id list per brand
    let campaignIdList = [];
    try {
        // Common parameters
        const timestamp = Math.floor(Date.now() / 1000);
        const baseString = `${partner_id}${campaignIdPath}${timestamp}${access_token}${shop_id}`;
        const sign = crypto.createHmac('sha256', partner_key)
            .update(baseString)
            .digest('hex');

        // Request parameters
        const params = new URLSearchParams({
            partner_id: partner_id,
            timestamp,
            access_token: access_token,
            shop_id: shop_id,
            sign,
        });

        const fullUrl = `${HOST}${campaignIdPath}?${params.toString()}`;
        console.log(`[SHOPEE-PRODUCT] Hitting Campaign Id List for ${brand}: `, fullUrl);

        const response = await axios.get(fullUrl, {
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if(response && response?.data && response?.data?.response) {
            console.log("[SHOPEE-PRODUCT] Campaign Id List for brand: ", brand, "\n");
            // console.log(response.data.response.campaign_list);

            campaignIdList = campaignIdList.concat(response.data.response.campaign_list);
        }

    } catch (e) {
        console.error(`[SHOPEE-PRODUCT] Error fetch campaign id list on ${brand}\n`);
        console.error(e);
    }

    console.log("Campaign Id List on brand: ", brand, "\n");
    console.log(campaignIdList);
    console.log("\n");
    
    let campaignPerformanceList = []

    if(campaignIdList.length > 0) {

        const idList = campaignIdList.map(c => String(c.campaign_id));
        const css = idList.join(',');


        try {
            console.log('[SHOPEE-PRODUCT] Comma Separated Campaign Ids: ', css);

            let startDate = new Date(backfillStartDate);
            let endDate = new Date(backfillEndDate);

            while(startDate <= endDate) {
                
                const timestamp = Math.floor(Date.now() / 1000);
                const baseString = `${partner_id}${productCampaignPath}${timestamp}${access_token}${shop_id}`;
                const sign = crypto.createHmac('sha256', partner_key)
                .update(baseString)
                .digest('hex');
                
                const stYear = startDate.getFullYear();
                const stMont = String(startDate.getMonth() + 1).padStart(2, '0');
                const stDay = String(startDate.getDate()).padStart(2, '0');
                const stString = `${stDay}-${stMont}-${stYear}`;

                const params = new URLSearchParams({
                    partner_id: partner_id,
                    timestamp,
                    access_token: access_token,
                    shop_id: shop_id,
                    sign,
                    start_date: stString,
                    end_date: stString,
                    campaign_id_list: css
                });
    
                const fullUrl = `${HOST}${productCampaignPath}?${params.toString()}`;
                console.log(`[SHOPEE-PRODUCT] Hitting Product-level Campaign for ${brand}: `, fullUrl);
    
                const response = await axios.get(fullUrl, {
                    headers: {
                        'Content-Type': 'application/json'
                    }
                });
    
                if(response && response.data && response.data.response) {

                    let campaignList = response.data.response.campaign_list;
                    campaignList.forEach(c => {
                        if(c.metrics_list[0].expense > 0) {
                            let obj = {
                                date: c.metrics_list[0].date,
                                prod_name: c.ad_name,
                                expense: c.metrics_list[0].expense,
                            }
                            campaignPerformanceList.push(obj);
                        }
                    })
                }

                startDate.setDate(startDate.getDate() + 1);
            }

        } catch (e) {
            console.error(`[SHOPEE-PRODUCT] Error fetch product-level campaign expenses on brand ${brand}\n`);
            console.error(e);
        }

    }


    // Process and transform data

    const totalExpense = await fetchAdsTotalBalance(brand, partner_id, partner_key, access_token, shop_id);

    console.log("PERIOD: 2025-11-01 to 2025-11-26")
    console.log("Campaign Performance List on brand: ", brand, "\n");
    console.log("Total Expense\n");

    let totalExpenseMerged = []
    totalExpense.forEach(t => {
        let obj = {
            date: t.date,
            prod_name: "Iklan Toko",
            expense: t.expense,
        }
        totalExpenseMerged.push(obj);
    });

    let productExpenseMap = {};
    campaignPerformanceList.forEach(item => {
        if (!productExpenseMap[item.date]) {
            productExpenseMap[item.date] = 0;
        }
        productExpenseMap[item.date] += item.expense;
    });

    let newTotalExpenseMerged = [];
    totalExpenseMerged.forEach(t => {
        const totalProductExpenseOnDate = productExpenseMap[t.date] || 0;
        const actualIklanTokoExpense = t.expense - totalProductExpenseOnDate;

        let obj = {
            date: t.date,
            prod_name: "Iklan Toko",
            expense: actualIklanTokoExpense > 0 ? actualIklanTokoExpense : 0, 
        }
        newTotalExpenseMerged.push(obj);
    });

    let dataToMerge = campaignPerformanceList.concat(newTotalExpenseMerged);

    await mergeProductShopeeAds(tableName[brandName], dataToMerge);
}

async function mergeProductShopeeAds(tableName, data) {
    console.log(`[PRODUCT-SHOPEE] Merging to ${tableName}`)
    // data.forEach(d => {
    //     console.log(`Date: ${d.date}. Prod Name: ${d.prod_name}. Expense: ${d.expense}`);
    // })

    try {
        const datasetId = "shopee_api";

        for(const d of data) {

            const checkQuery = `
                SELECT date 
                FROM \`${datasetId}.${tableName}\` 
                WHERE date = @date 
                AND prod_name = @prod_name
            `;
            
            const options = {
                query: checkQuery,
                params: {
                    date: d.date.split('-').reverse().join('-'),
                    prod_name: d.prod_name,
                }
            };

            const [existingRows] = await bigquery.query(options);

            if (existingRows.length > 0) {
                continue; // Skip this insertion
            }

            await bigquery
                .dataset(datasetId)
                .table(tableName)
                .insert({
                    date: d.date.split('-').reverse().join('-'),
                    prod_name: d.prod_name,
                    cost: parseInt(d.expense),
                    process_dttm: new Date(Date.now() + 7 * 60 * 60 * 1000).toISOString().replace('T', ' ').substring(0, 19)
                });
        }
        console.log(`[PRODUCT-SHOPEE] Successfully processed ${data.length} row(s) for ${tableName}`);
    } catch (e) {
        console.log(`Error merging product-level shopee ads on ${tableName}`);
        console.log(e);
    }
} 