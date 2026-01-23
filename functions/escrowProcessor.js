import { BigQuery } from '@google-cloud/bigquery';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios from 'axios';
import crypto from 'crypto';

/*** 
TODO:
1. Should hit two endpoints: get_escrow_list and get_escrow_detail per order_sn
2. Transform the data with the required structure
***/
export async function mainDanaDilepas(brand, partner_id, partner_key, access_token, shop_id) {
    const escrowContainer = await fetchDanaDilepas(brand, partner_id, partner_key, access_token, shop_id);
    const twentyBatchContainer = await transformData(escrowContainer, brand);
    await breakdownEscrow(twentyBatchContainer, brand, partner_id, partner_key, access_token, shop_id);
}

export async function fetchDanaDilepas(brand, partner_id, partner_key, access_token, shop_id) {
    console.log("Fetch Dana Dilepas of brand: ", brand);

    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/payment/get_escrow_list";

    try {
    
        console.log("[SHOPEE-WITHDRAWAL] Raw response for brand: ", brand);
        const timestamp = Math.floor(Date.now() / 1000);
        const baseString = `${partner_id}${PATH}${timestamp}${access_token}${shop_id}`;
        const sign = crypto.createHmac('sha256', partner_key)
            .update(baseString)
            .digest('hex');
        
        let count = 0;
        let hasMore = true;
        let pageNumber = 1;
        let escrowContainer = [];

        while(hasMore) {

            const releaseTimeStart = Math.floor(new Date("2026-01-01") / 1000);
            const releaseTimeEnd = Math.floor(new Date("2026-01-21") / 1000);
            const params = new URLSearchParams({
                partner_id: partner_id,
                timestamp,
                access_token: access_token,
                shop_id: shop_id,
                sign,
                release_time_from: releaseTimeStart,
                release_time_to: releaseTimeEnd,
                page_size: 100,
                page_no: pageNumber,
            });

            const fullUrl = `${HOST}${PATH}?${params.toString()}`;
            console.log(`Hitting Dana Dilepas for ${brand}: `, fullUrl, " - page: ", pageNumber);

            const response = await axios.get(fullUrl, {
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            let escrowList = response.data.response.escrow_list;
            escrowContainer.push(...escrowList);

            count += escrowList.length;
            hasMore = response.data.response.more;
            pageNumber += 1;
        }

        console.log("[SHOPEE-WITHDRAWAL] Data count: ", count);
        return escrowContainer;
    } catch (e) {
        console.log("[SHOPEE-WITHDRAWAL] ERROR on fetching Dana Dilepas on brand: ", brand);
        console.log(e.response);
    }

    // console.log("All Dana Dilepas on brand: ", brand);
    // console.log("Count: ", danaDilepas.length);
}

async function transformData(data, brand) {
    console.log("Dana Dilepas on brand: ", brand)
    console.log("All Order_Sns on Data before Transform: \n");
    
    let twentyBatchContainer = [];
    let twentyBatch = [];
    data.forEach(d => {
        
        twentyBatch.push(d.order_sn);

        if(twentyBatch.length == 20) {
            twentyBatchContainer.push(twentyBatch);
            twentyBatch = [];
        }
    });

    if (twentyBatch.length > 0) {
        twentyBatchContainer.push(twentyBatch);
    }

    return twentyBatchContainer;
}

async function breakdownEscrow(data, brand, partner_id, partner_key, access_token, shop_id) {
    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/payment/get_escrow_detail_batch";

    try {
        for(let i=0; i<data.length; i++) {

            const timestamp = Math.floor(Date.now() / 1000);
            const baseString = `${partner_id}${PATH}${timestamp}${access_token}${shop_id}`;
            const sign = crypto.createHmac('sha256', partner_key)
                .update(baseString)
                .digest('hex');
            const params = new URLSearchParams({
                partner_id: partner_id,
                timestamp,
                access_token: access_token,
                shop_id: shop_id,
                sign,
                order_sn_list: d[i],
            });

            const fullUrl = `${HOST}${PATH}?${params.toString()}`;
            console.log("Hitting withdrawal URL: ", fullUrl, "on batch: ", i);

            const response = await axios.get(fullUrl, {
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            console.log("[SHOPEE-WITHDRAWAL] Raw response: ");
            let escrowDetailList = response.data.response;

            escrowDetailList.forEach(e => {
                console.log("Escrow order id: ", e.escrow_detail.order_sn);
                console.log("Escrow buyer: ", e.escrow_detail.buyer_user_name);
                console.log("\n");
            });

            if(i == 4) {
                break;
            }
        }
    } catch (e) {
        console.error("[SHOPEE-WITHDRAWAL] Error getting ESCROW DETAIL BATCH: ", brand);
        console.error(e);
    }
}

const brandTables = {
    "Chess": "chess_finance",
    "Cleviant": "cleviant_finance",
    "Dr.Jou": "dr_jou_finance",
    "Evoke": "evoke_finance",
    "G-Belle": "gbelle_finance",
    "Ivy & Lily": "ivy_lily_finance",
    "Naruko": "naruko_finance",
    "Miss Daisy": "miss_daisy_finance",
    "Mirae": "mirae_finance",
    "Mamaway": "mamaway_finance",
    "Mosseru": "mosseru_finance",
    "Nutri & Beyond": "nutri_beyond_finance",
    "Past Nine": "past_nine_finance",
    "Polynia": "polynia_finance",
    "SH-RD": "shrd_finance",
    "Swissvita": "swissvita_finance",
    "Eileen Grace": "eileen_grace_finance",
    "Relove": "relove_finance",
    "Joey & Roo": "joey_roo_finance",
    "Enchante": "enchante_finance",
    "Rocketindo Shop": "pinkrocket_finance",
}

// async function mergeData(data, brand) {
//     console.log("[SHOPEE-WITHDRAWAL] Start merging for brand: ", brand);
//     const tableName = brandTables[brand];
//     const bigquery = new BigQuery();
//     const datasetId = 'shopee_api';

//     try {

//     } catch (e) {
//         console.error("[SHOPEE-WITHDRAWAL] Error inserting FINANCE data on brand: ", brand);
//         console.error(e);
//     }
// }

