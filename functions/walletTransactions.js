import axios from 'axios';
import crypto from 'crypto';
import { BigQuery } from '@google-cloud/bigquery';

async function fetchWalletTransaction(brand, partner_id, partner_key, access_token, shop_id) {    
    console.log("Fetch Wallet Transaction of brand: ", brand);
    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/payment/get_wallet_transaction_list";

    try {
        const timestamp = Math.floor(Date.now() / 1000);
        const baseString = `${partner_id}${PATH}${timestamp}${access_token}${shop_id}`;
        const sign = crypto.createHmac('sha256', partner_key)
            .update(baseString)
            .digest('hex');
        
        let count = 0;
        let hasMore = true;
        let pageNumber = 0;
        let transactionContainer = [];

        while(hasMore) {
            const createTimeFrom = Math.floor(new Date("2025-12-29") / 1000);
            const createTimeTo = Math.floor(new Date("2026-01-03") / 1000);

            console.log("Create time from: ", createTimeFrom);
            console.log("Create time to: ", createTimeTo);

            const params = new URLSearchParams({
                partner_id: partner_id,
                timestamp,
                access_token: access_token,
                shop_id: shop_id,
                sign,
                page_size: 40,
                page_no: pageNumber,
                create_time_from: createTimeFrom,
                create_time_to: createTimeTo
            });
            const fullUrl = `${HOST}${PATH}?${params.toString()}`;
            console.log(`Hitting Wallet Trx for ${brand}: `, fullUrl, " - page: ", pageNumber);

            const response = await axios.get(fullUrl, {
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            let transactionList = response.data.response.transaction_list;
            transactionContainer.push(...transactionList);

            count += transactionList.length;
            hasMore = response.data.response.more;
            pageNumber += 1;
        }
        console.log("[WALLET-TRX] Data row count: ", count);
        return transactionContainer;
    } catch (e) {
        console.log("[WALLET-TRX] Error on fetching wallet transaction of brand: ", brand);
        console.log(e);
    }
}

async function transformData(data) {
    let transformed = [];
    data.forEach(d => {
        if(d.status !== "COMPLETED") {
            console.log("Non-completed data: ", d.status);
        }
        let obj = {
            'created_date': new Date(d.create_time * 1000).toISOString().replace('T', ' ').split('.')[0],
            'order_sn': d.order_sn,
            'description': d.description,
            'amount': d.amount,
            'money_flow': d.money_flow,
            'transaction_id': d.transaction_id,
            'status': d.status,
        }
        transformed.push(obj);
    });
    return transformed;
}

const brandTables = {
    "Chess": "chess_wallet_trx",
    "Cleviant": "cleviant_wallet_trx",
    "Dr.Jou": "dr_jou_wallet_trx",
    "Evoke": "evoke_wallet_trx",
    "G-Belle": "gbelle_wallet_trx",
    "Ivy & Lily": "ivy_lily_wallet_trx",
    "Naruko": "naruko_wallet_trx",
    "Miss Daisy": "miss_daisy_wallet_trx",
    "Mirae": "mirae_wallet_trx",
    "Mamaway": "mamaway_wallet_trx",
    "Mosseru": "mosseru_wallet_trx",
    "Nutri & Beyond": "nutri_beyond_wallet_trx",
    "Past Nine": "past_nine_wallet_trx",
    "Polynia": "polynia_wallet_trx",
    "SH-RD": "shrd_wallet_trx",
    "Swissvita": "swissvita_wallet_trx",
    "Eileen Grace": "eileen_grace_wallet_trx",
    "Relove": "relove_wallet_trx",
    "Joey & Roo": "joey_roo_wallet_trx",
    "Enchante": "enchante_wallet_trx",
    "Rocketindo Shop": "pinkrocket_wallet_trx",
}

async function mergeData(data, brand) {
    console.log("[WALLET-TRX] Start merging for brand: ", brand);
    const tableName = brandTables[brand];
    const bigquery = new BigQuery();
    const datasetId = 'shopee_api';

    // If no data to merge, exit early to save API calls
    if (!data || data.length === 0) {
        console.log("[WALLET-TRX] No data to merge for", brand);
        return;
    }
    const uniqueMap = new Map();
    data.forEach(item => {
        // We use the transaction_id as the key. 
        // If it already exists, this overwrites it with the latest version.
        uniqueMap.set(String(item.transaction_id), item);
    });
    const uniqueData = Array.from(uniqueMap.values());

    try {
        // SQL: MERGE Statement (The "Upsert" Logic)
        // We match rows based on 'transaction_id'.
        const query = `
            MERGE \`${datasetId}.${tableName}\` T
            USING UNNEST(@sourceData) S
            ON T.transaction_id = S.transaction_id
            
            -- 1. If ID exists: Update the status and refresh the process timestamp
            WHEN MATCHED THEN
                UPDATE SET 
                    status = S.status,
                    process_dttm = CURRENT_DATETIME()
            
            -- 2. If ID is new: Insert the full record
            WHEN NOT MATCHED THEN
                INSERT (
                    transaction_id, 
                    created_date, 
                    order_sn, 
                    description, 
                    amount, 
                    money_flow, 
                    status, 
                    process_dttm
                )
                VALUES (
                    S.transaction_id, 
                    S.created_date, 
                    S.order_sn, 
                    S.description, 
                    S.amount, 
                    S.money_flow, 
                    S.status, 
                    CURRENT_DATETIME()
                )
        `;

        // Map data to ensure clean types for BigQuery
        const sourceData = uniqueData.map(d => ({
            transaction_id: String(d.transaction_id), // Ensure ID is a string
            created_date: d.created_date,
            order_sn: d.order_sn,
            description: d.description,
            amount: parseFloat(d.amount),
            money_flow: d.money_flow,
            status: d.status
        }));

        const options = {
            query,
            params: {
                sourceData: sourceData
            }
        };

        // Run the query
        await bigquery.query(options);
        console.log(`[WALLET-TRX] Successfully merged (upserted) ${sourceData.length} rows for ${brand}`);

    } catch (e) {
        console.log("[WALLET-TRX] Error merging wallet trx on brand: ", brand);
        console.log(e);
    }   
}

export async function handleWalletTransactions(brand, partner_id, partner_key, access_token, shop_id) {
    const transactionContainer = await fetchWalletTransaction(brand, partner_id, partner_key, access_token, shop_id);
    const transformed = await transformData(transactionContainer);
    await mergeData(transformed, brand);
}

