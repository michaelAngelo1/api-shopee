import { BigQuery } from '@google-cloud/bigquery';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios from 'axios';
import crypto from 'crypto';

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
            const releaseTimeEnd = Math.floor(new Date("2026-01-07") / 1000);
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
        let escrowBreakdown = [];
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
            });

            const fullUrl = `${HOST}${PATH}?${params.toString()}`;
            console.log("Hitting withdrawal URL: ", fullUrl, "on batch: ", i);

            console.log("Data[i]: ", data[i]);

            const response = await axios.post(fullUrl, {
                "order_sn_list": data[i]
            });

            let escrowDetailList = response.data.response;
            console.log("[SHOPEE-WITHDRAWAL] First raw escrow detail list: ");
            console.log(escrowDetailList.slice(0, 2));

            escrowDetailList.forEach(e => {
                let obj = {
                    "No_Pesanan": e.escrow_detail.order_sn,
                    "Harga_Asli_Produk": e.escrow_detail.order_income.order_original_price,
                    "Total_Diskon_Produk": e.escrow_detail.order_income.order_seller_discount,
                    "Diskon_Produk_Dari_Shopee": e.escrow_detail.order_income.shopee_discount,
                    "Diskon_Voucher_Ditanggung_Penjual": e.escrow_detail.order_income.voucher_from_seller,
                    "Biaya_Komisi_AMS": e.escrow_detail.order_income.order_ams_commission_fee,
                    "Biaya_Administrasi_with_PPN_11": e.escrow_detail.order_income.commission_fee,
                    "Biaya_Layanan": e.escrow_detail.order_income.service_fee,
                    "Biaya_Proses_Pesanan": e.escrow_detail.order_income.seller_order_processing_fee,
                    "Total_Penghasilan": e.escrow_detail.order_income.escrow_amount,
                    "process_dttm": new Date(Date.now() + 7 * 60 * 60 * 1000).toISOString().replace('T', ' ').substring(0, 19)
                }
                escrowBreakdown.push(obj);
            });

            // escrowBreakdown to merge to BigQuery.
        }
        await mergeData(escrowBreakdown, brand);
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

async function mergeData(data, brand) {
    console.log("[SHOPEE-WITHDRAWAL] Start merging for brand: ", brand);
    const tableName = brandTables[brand];
    const bigquery = new BigQuery();
    const datasetId = 'shopee_api';

    try {
        console.log("[SHOPEE-WITHDRAWAL] Data before merging. First two: ");
        console.log(data.slice(0, 2));

        const incomingOrderSNs = data.map(row => `'${row.No_Pesanan}'`).join(",");
        const query = `
            SELECT No_Pesanan 
            FROM \`${bigquery.projectId}.${datasetId}.${tableName}\`
            WHERE No_Pesanan IN (${incomingOrderSNs})
        `;
        const [existingRows] = await bigquery.query({ query });
        
        const existingIds = new Set(existingRows.map(row => row.No_Pesanan));
        console.log(`[SHOPEE-WITHDRAWAL] Found ${existingIds.size} duplicates in BigQuery.`);

        const recordsToInsert = data.filter(row => !existingIds.has(row.No_Pesanan));

        if (recordsToInsert.length === 0) {
            console.log("[SHOPEE-WITHDRAWAL] All data already exists. Skipping insert.");
            return;
        }

        console.log(`[SHOPEE-WITHDRAWAL] Inserting ${recordsToInsert.length} new rows`);
        await bigquery
            .dataset(datasetId)
            .table(tableName)
            .insert(recordsToInsert);

        console.log(`[SHOPEE-WITHDRAWAL] Successfully inserted rows for ${brand}.`);
    } catch (e) {
        console.error("[SHOPEE-WITHDRAWAL] Error inserting FINANCE data on brand: ", brand);
        console.error(e);
    }
}

/*** 
TODO:
1. Should hit two endpoints: get_escrow_list and get_escrow_detail per order_sn
2. Transform the data with the required structure
***/
export async function mainDanaDilepas(brand, partner_id, partner_key, access_token, shop_id) {
    const escrowContainer = await fetchDanaDilepas(brand, partner_id, partner_key, access_token, shop_id);
    const twentyBatchContainer = await transformData(escrowContainer, brand);

    if(twentyBatchContainer && twentyBatchContainer.length > 0) {
        await breakdownEscrow(twentyBatchContainer, brand, partner_id, partner_key, access_token, shop_id);
    }
}

