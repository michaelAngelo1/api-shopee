import axios from 'axios';
import { BigQuery } from '@google-cloud/bigquery';
import { isRedisCluster } from 'bullmq';
import { backfillEndDate, backfillStartDate } from './fetchTiktokBasicAds.js';
const bigquery = new BigQuery();
let access_token = process.env.TIKTOK_MARKETING_ACCESS_TOKEN;

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

let storeIdAcc = {
    "eileengrace": {
        store_id: "7494055813163943155",
        table_name: "eileen_grace_pgmax"
    },
    "shrd": {
        store_id: "7494060372131481134",
        table_name: "shrd_pgmax"
    },
    "missdaisy": {
        store_id: "7494083757647759179",
        table_name: "miss_daisy_pgmax"
    },
    "polynia": {
        store_id: "7494718012797651378",
        table_name: "polynia_pgmax"
    },
    "cleviant": {
        store_id: "7495299579063405468",
        table_name: "cleviant_pgmax"
    },
    "mosseru": {
        store_id: "7495297011747293899",
        table_name: "mosseru_pgmax",
    },
    "mirae": {
        store_id: "7495819231306943483",
        table_name: "mirae_pgmax"
    },
    "mamaway": {
        store_id: "7494499456018189063",
        table_name: "mamaway_pgmax"
    },
    "chess": {
        store_id: "7494919612596259170",
        table_name: "chess_pgmax"
    }, 
    "nutribeyond": {
        store_id: "7496045913194138312",
        table_name: "nutri_beyond_pgmax"
    },
    "evoke": {
        store_id: "7495667268174318445",
        table_name: "evoke_pgmax"
    },
    "drjou": {
        store_id: "7495803189501659725",
        table_name: "dr_jou_pgmax"
    },
    "swissvita": {
        store_id: "7494835443584567449",
        table_name: "swissvita_pgmax"
    },
    "gbelle": {
        store_id: "7495908629104331053",
        table_name: "gbelle_pgmax"
    },
    "pastnine": {
        store_id: "7495997119882693518",
        table_name: "past_nine_pgmax"
    },
    "ivylily": {
        store_id: "7496045415576275429",
        table_name: "ivy_lily_pgmax"
    },
    "naruko": {
        store_id: "7496241553706617176",
        table_name: "naruko_pgmax"
    },
    "rocketindoshop": {
        store_id: "7495827950440450460",
        table_name: "rocketindo_shop_pgmax"
    }
}    

export async function fetchPGMVMaxBreakdown(brand, advertiser_id) {
    // Get campaign_ids on store's Product GMV Max campaigns
    // request parameter: advertiser_id, store_ids (per brand), gmv_max_promotion_types: ["PRODUCT_GMV_MAX"]
    // expected response: campaign_id, campaign_name

    await sleep(10000);

    let brandName = brand.toLowerCase().replace(/\s/g, "");


    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yyyy = yesterday.getFullYear();
    const mm = String(yesterday.getMonth() + 1).padStart(2, '0');
    const dd = String(yesterday.getDate()).padStart(2, '0');
    const yesterdayStr = `${yyyy}-${mm}-${dd}`;

    const hh = String(yesterday.getHours()).padStart(2, '0'); // Hours (00-23)
    const mi = String(yesterday.getMinutes()).padStart(2, '0'); // Minutes (00-59)
    const ss = String(yesterday.getSeconds()).padStart(2, '0'); // Seconds (00-59)

    // Combine all parts into the desired string format (e.g., YYYY-MM-DD HH:MI:SS)
    const yesterdayStrWithTime = `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;

    try {
        const params = {
            advertiser_id: advertiser_id,
            fields: JSON.stringify(["campaign_id", "campaign_name"]),
            filtering: JSON.stringify({ 
                gmv_max_promotion_types: ["PRODUCT_GMV_MAX"],
                store_ids: [storeIdAcc[brandName].store_id],
                // primary_status: "STATUS_DELIVERY_OK",
            }),
            // creation_filter_start_time: "2025-01-01 00:00:01",
            // creation_filter_end_time: yesterdayStrWithTime,
            page: 1, 
            page_size: 100
        }

        const url = "https://business-api.tiktok.com/open_api/v1.3/gmv_max/campaign/get/";
        const response = await axios.get(url, {
            headers: {
                'Access-Token': access_token
            },
            params
        });

        let campaignIdsAsParam = [];
        if(response && response.data && response.data.data && response.data.data.list) {
            // console.log(response?.data?.data?.list);

            let campaignIdList = response.data.data.list;
            campaignIdList.forEach((c) => {
                campaignIdsAsParam.push({
                    campaign_id: c.campaign_id,
                    campaign_name: c.campaign_name
                });
            })
        } else {
            console.log(`[PRODUCT-BREAKDOWN] response does not exist on ${brand}`);
        }

        
        if(campaignIdsAsParam.length > 0) {
            console.log(`🥰 Campaign Id & Name on brand ${brand}\n`);
            // console.log(campaignIdsAsParam);
            // console.log("\n");

            let breakdownCostList = [];
            for(const c of campaignIdsAsParam) {

                let success = false;
                let retries = 10;
                while(!success && retries > 0) {
                    try {
                        const params = {
                            advertiser_id: advertiser_id,
                            store_ids: JSON.stringify([storeIdAcc[brandName].store_id]),
                            metrics: JSON.stringify(["product_name", "item_group_id", "cost", "gross_revenue"]),
                            dimensions: JSON.stringify(["item_group_id", "stat_time_day"]),
                            filtering: JSON.stringify({
                                campaign_ids: [c.campaign_id],
                            }),
                            start_date: yesterdayStr,
                            end_date: yesterdayStr,
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

                        await sleep(3000);
                        
                        // console.log(`🔥 [PRODUCT-BREAKDOWN] raw response product-level metrics: `);
                        // console.log(response);
        
                        if(response && response.data && response.data.data && response.data.data.list) {

                            success = true;
                            
                            if(brand == "SHRD") {
                                console.log(`🔥 [PRODUCT-BREAKDOWN] product-level metrics response on ${brand} and Campaign Name: ${c.campaign_name} and ID: ${c.campaign_id}`);
                                // console.log(response.data.data.list);
                            }

                            // let sumCost = 0;
                            let productLevelList = response.data.data.list;
                            
                            productLevelList.forEach(p => {
                                if(p.metrics.cost !== "0") {
                                    let obj = {
                                        date: p.dimensions.stat_time_day.substring(0, 10),
                                        campaign_name: c.campaign_name,
                                        prod_id: p.metrics.item_group_id,
                                        prod_name: p.metrics.product_name,
                                        cost: parseInt(p.metrics.cost),
                                        gmv: parseInt(p.metrics.gross_revenue),
                                        process_dttm: new Date(Date.now() + 7 * 60 * 60 * 1000).toISOString().replace('T', ' ').substring(0, 19)
                                    }
                                    // sumCost += p.metrics.cost;
                                    breakdownCostList.push(obj);
                                }
                            });
                        } else {
                            retries -= 1;
                            console.log(`🥶 [PRODUCT-BREAKDOWN] Failed response on ${brand}. Retries left: ${retries}`);
                            // console.log(response?.data);

                            if(retries > 0) await sleep(25000);
                        }
                    } catch (e) {
                        console.log("🤯 [PRODUCT-BREAKDOWN] Error getting product-level metrics on: ", brand, "error: ", e);
                    }
                }
            }

            if(breakdownCostList.length > 0) {
                console.log(`🔥 [PRODUCT-BREAKDOWN] Breakdown Cost List on ${brand}`);
                // console.log(breakdownCostList);
                console.log("Breakdown Cost List length: ", breakdownCostList.length);

                // await mergeBreakdown(storeIdAcc[brandName].table_name, breakdownCostList);

                await preprocessData(advertiser_id, brandName, storeIdAcc[brandName].table_name, breakdownCostList, campaignIdsAsParam);
            }
        }

    } catch (e) {
        console.log("🤯 [PRODUCT-BREAKDOWN] Error getting campaign_id on: ", brand, "error: ", e);
    }
}

async function preprocessData(advertiser_id, brandName, tableName, dataReference, campaignIdList) {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yyyy = yesterday.getFullYear();
    const mm = String(yesterday.getMonth() + 1).padStart(2, '0');
    const dd = String(yesterday.getDate()).padStart(2, '0');
    const yesterdayStr = `${yyyy}-${mm}-${dd}`;
    
    let processedData = dataReference;
    console.log("PREPROCESS DATA: ", tableName);

    let campaignMetricsList = [];
    
    for(const c of campaignIdList) {

        try {
            const params = {
                advertiser_id: advertiser_id,
                store_ids: JSON.stringify([storeIdAcc[brandName].store_id]),
                metrics: JSON.stringify(["campaign_id", "campaign_name", "cost"]),
                dimensions: JSON.stringify(["campaign_id", "stat_time_day"]),
                filtering: JSON.stringify({
                    gmv_max_promotion_types: ["PRODUCT"],
                    campaign_ids: [c.campaign_id]
                }),
                start_date: yesterdayStr,
                end_date: yesterdayStr,
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
    
            if(response && response.data && response.data.data && response.data.data.list) {
                const refList = response.data.data.list;
                refList.forEach(r => {
                    let obj = {
                        date: r.dimensions.stat_time_day.substring(0, 10),
                        campaign_id: r.metrics.campaign_id,
                        campaign_name: r.metrics.campaign_name,
                        cost: parseInt(r.metrics.cost),
                    }
                    campaignMetricsList.push(obj);
                })
            }
        } catch (e) {
            console.log(`PREPROCESS ERROR on brand ${brandName}`);
            console.log(e);
        }
    }

    // console.log(`CAMPAIGN METRICS LIST ON ${tableName}\n`);
    // campaignMetricsList.forEach(c => console.log(c));
    // console.log("\n")
    
    // console.log(`PRODUCT METRICS LIST ON ${tableName}`);
    // dataReference.forEach(d => console.log(d));
    // console.log("\n");

    campaignMetricsList.forEach(camp => {
        const cName = camp.campaign_name;
        const cDate = camp.date; // Get the specific date for this campaign entry
        const campaignTotalCost = camp.cost;
        
        // --- UPDATED: Filter by Campaign Name AND Date ---
        let productsInCampaign = processedData.filter(p => p.campaign_name === cName && p.date === cDate);

        if(productsInCampaign.length > 0) {
            
            let productSumCost = productsInCampaign.reduce((sum, p) => sum + p.cost, 0);
            let diff = campaignTotalCost - productSumCost;

            if (Math.abs(diff) > 1000) { 
                console.warn(`⚠️ [MISMATCH] Date: ${cDate} | Campaign: "${cName}" | Camp Cost: ${campaignTotalCost} | Prod Sum: ${productSumCost} | Diff: ${diff}`);
                
                let highestCostProduct = productsInCampaign.reduce((prev, current) => {
                    return (prev.cost > current.cost) ? prev : current;
                });
                
                highestCostProduct.cost += diff;        
                console.log(`   > Adjusted "${highestCostProduct.prod_name}". New Cost: ${highestCostProduct.cost}\n`);
            
            } else {
                // console.log(`✅ [MATCH] Date: ${cDate} | Campaign: "${cName}" is balanced.`);
            }
        }
    });
    // 1. Get sum of all products' cost in a campaign on a given date.
    // const productCostMap = dataReference.reduce((acc, curr) => {
    //     const cName = curr.campaign_name;
    //     const pCost = parseInt(curr.cost) || 0;

    //     if (!acc[cName]) {
    //         acc[cName] = 0;
    //     }
    //     acc[cName] += pCost;
    //     return acc;
    // }, {});

    // campaignMetricsList.forEach(camp => {
    //     const cName = camp.campaign_name;
    //     const campaignTotalCost = camp.cost;
        
    //     let productSumCost = productCostMap[cName] || 0;

    //     const diff = Math.abs(campaignTotalCost - productSumCost);

    //     // We allow a very small epsilon for floating point differences, 
    //     // or check strict inequality if your data is perfectly clean.
    //     if (diff > 1000) { 
    //         console.warn(`⚠️ [MISMATCH DETECTED] Campaign: "${cName}"`);
    //         console.warn(`   > Campaign Level Cost: ${campaignTotalCost}`);
    //         console.warn(`   > Sum of Products Cost: ${productSumCost}`);
    //         console.warn(`   > Difference: ${diff.toFixed(4)}\n`);
    //     } else {
    //         console.log(`✅ [MATCH] Campaign: "${cName}" is balanced (Cost: ${campaignTotalCost})`);
    //     }
    // });

    await mergeBreakdown(tableName, processedData);
}

async function mergeBreakdown(tableName, data) {
    console.log("🥶 Merging to table: ", tableName);
    console.log("🥶 Data: \n");
    console.log(data.length);

    const datasetId = "tiktok_api_us";

    try {
        for(const d of data) {
            
            const checkQuery = `
                SELECT date 
                FROM \`${datasetId}.${tableName}\` 
                WHERE date = @date 
                AND campaign_name = @campaign_name
                AND prod_id = @prod_id
            `;
            
            const options = {
                query: checkQuery,
                params: {
                    date: d.date,
                    campaign_name: d.campaign_name,
                    prod_id: d.prod_id
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
                    date: d.date,
                    campaign_name: d.campaign_name, 
                    prod_id: d.prod_id,
                    prod_name: d.prod_name,
                    cost: parseInt(d.cost),
                    gmv: parseInt(d.gmv),
                    process_dttm: new Date(Date.now() + 7 * 60 * 60 * 1000).toISOString().replace('T', ' ').substring(0, 19)
                });
        }
        console.log(`[MERGE-BREAKDOWN] Successfully processed ${data.length} row(s) for ${tableName}`);
    } catch (e) {
        console.error(`🤯 Error merge breakdown data on ${tableName}`)
    }
}