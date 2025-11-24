import axios from 'axios';
import { BigQuery } from '@google-cloud/bigquery';
const bigquery = new BigQuery();

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export async function fetchPGMVMaxBreakdown(brand, advertiser_id) {
    // Get campaign_ids on store's Product GMV Max campaigns
    // request parameter: advertiser_id, store_ids (per brand), gmv_max_promotion_types: ["PRODUCT_GMV_MAX"]
    // expected response: campaign_id, campaign_name

    await sleep(7000);

    let access_token = process.env.TIKTOK_MARKETING_ACCESS_TOKEN;
    let brandName = brand.toLowerCase().replace(/\s/g, "");

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

    try {
        const params = {
            advertiser_id: advertiser_id,
            fields: JSON.stringify(["campaign_id", "campaign_name"]),
            filtering: JSON.stringify({ 
                gmv_max_promotion_types: ["PRODUCT_GMV_MAX"],
                store_ids: [storeIdAcc[brandName]],
                primary_status: "STATUS_DELIVERY_OK",
            }),
            creation_filter_start_time: "2025-07-01 00:00:01",
            creation_filter_end_time: "2025-11-23 23:59:59",
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
                campaignIdsAsParam.push(c.campaign_id);
            })
        } else {
            console.log(`[PRODUCT-BREAKDOWN] response does not exist on ${brand}`);
        }

        
        if(campaignIdsAsParam.length > 0) {
            let breakdownCostList = [];
            for(const campaignId of campaignIdsAsParam) {
                try {
                    const params = {
                        advertiser_id: advertiser_id,
                        store_ids: JSON.stringify([storeIdAcc[brandName]]),
                        metrics: JSON.stringify(["product_name", "cost"]),
                        dimensions: JSON.stringify(["item_group_id", "stat_time_day"]),
                        filtering: JSON.stringify({
                            campaign_ids: [campaignId],
                        }),
                        start_date: "2025-11-01",
                        end_date: "2025-11-22",
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
                    
                    // console.log(`🔥 [PRODUCT-BREAKDOWN] raw response product-level metrics: `);
                    // console.log(response);
    
                    if(response && response.data && response.data.data && response.data.data.list) {
                        // console.log(`🔥 [PRODUCT-BREAKDOWN] product-level metrics response on ${brand}: `);
                        // console.log(response.data.data.list);
                        let productLevelList = response.data.data.list;

                        productLevelList.forEach(p => {
                            if(p.metrics.cost !== "0") {
                                let obj = {
                                    date: p.dimensions.stat_time_day,
                                    product_name: p.metrics.product_name,
                                    cost: p.metrics.cost
                                }
                                breakdownCostList.push(obj);
                            }
                        });
                    } else {
                        console.log(`🤯 [PRODUCT-BREAKDOWN] product-level metrics on ${brand} does not exist`);
                    }
                } catch (e) {
                    console.log("🤯 [PRODUCT-BREAKDOWN] Error getting product-level metrics on: ", brand, "error: ", e);
                }
            }

            console.log(`🔥 [PRODUCT-BREAKDOWN] Breakdown Cost List on ${brand}`);
            console.log(breakdownCostList);
            console.log("Breakdown Cost List length: ", breakdownCostList.length);
        }

    } catch (e) {
        console.log("🤯 [PRODUCT-BREAKDOWN] Error getting campaign_id on: ", brand, "error: ", e);
    }
}