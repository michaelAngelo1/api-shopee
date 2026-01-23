import { BigQuery } from '@google-cloud/bigquery';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios from 'axios';
import crypto from 'crypto';

/*** 
TODO:
1. Should hit two endpoints: get_escrow_list and get_escrow_detail per order_sn
2. Transform the data with the required structure
***/

export async function fetchDanaDilepas(brand, partner_id, partner_key, access_token, shop_id) {
    console.log("Fetch Dana Dilepas of brand: ", brand);

    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/payment/get_escrow_list";

    try {
    
        console.log("[SHOPEE-WITHDRAWAL] Raw response for brand: ", brand);
        
        let danaDilepas = []
        let count = 0;
        let hasMore = true;
        let pageNumber = 1;

        while(hasMore) {
            const timestamp = Math.floor(Date.now() / 1000);
            const baseString = `${partner_id}${PATH}${timestamp}${access_token}${shop_id}`;
            const sign = crypto.createHmac('sha256', partner_key)
                .update(baseString)
                .digest('hex');

            const releaseTimeStart = Math.floor(new Date("2026-01-01") / 1000);
            const releaseTimeEnd = Math.floor(new Date("2026-01-03") / 1000);
            const params = new URLSearchParams({
                partner_id: partner_id,
                timestamp,
                access_token: access_token,
                shop_id: shop_id,
                sign,
                release_time_from: releaseTimeStart,
                release_time_to: releaseTimeEnd,
                page_size: 100,
                page_number: pageNumber,
            });

            const fullUrl = `${HOST}${PATH}?${params.toString()}`;
            console.log(`Hitting Dana Dilepas for ${brand}: `, fullUrl);

            const response = await axios.get(fullUrl, {
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            
            let escrowList = response.data.response.escrow_list;
            
            console.log("Escrow list first 3: ");
            console.log(escrowList.slice(0, 3));

            count += escrowList.length;

            hasMore = response.data.response.more;
            pageNumber += 1;
        }

        console.log("[SHOPEE-WITHDRAWAL] Data count: ", count);
    } catch (e) {
        console.log("[SHOPEE-WITHDRAWAL] ERROR on fetching Dana Dilepas on brand: ", brand);
        console.log(e.response);
    }

    console.log("All Dana Dilepas on brand: ", brand);
    console.log("Count: ", danaDilepas.length);
}

async function mergeDanaDilepas(data, brand) {
    console.log("Dana Dilepas on brand: ", brand)

    // Merge to BigQuery

    // console.log(data);
}