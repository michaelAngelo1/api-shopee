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
            const createTimeFrom = Math.floor(new Date("2026-01-16") / 1000);
            const createTimeTo = Math.floor(new Date("2026-01-25") / 1000);
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
        console.log("[WALLET-TRX] Data count: ", count);
        return transactionContainer;
    } catch (e) {
        console.log("[WALLET-TRX] Error on fetching wallet transaction of brand: ", brand);
        console.log(e);
    }
}

async function transformData(data) {
    let transformed = [];
    data.forEach(d => {
        let obj = {
            'created_date': new Date(d.create_time * 1000).toISOString().replace('T', ' ').split('.')[0],
            'order_sn': d.order_sn,
            'description': d.description,
            'amount': d.amount,
            'money_flow': d.money_flow,
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

    try {
        // 2. Prepare the list of dates to check in bulk
        const incomingDates = data.map(d => d.created_date);

        // 3. Batch Query: Find all dates from this batch that ALREADY exist in BigQuery
        // We select 'created_date' specifically to compare against your input
        const query = `
            SELECT created_date
            FROM \`${datasetId}.${tableName}\`
            WHERE created_date IN UNNEST(@dates)
        `;

        const options = {
            query,
            params: {
                dates: incomingDates
            }
        };

        const [existingRows] = await bigquery.query(options);

        // 4. Create a Set for fast lookup of existing dates
        // Note: Ensure BQ timestamp format matches your input format (e.g. both are Unix integers or ISO strings)
        const existingDatesSet = new Set(existingRows.map(row => {
            // BigQuery might return an object for timestamps, ensure we get the primitive value
            return row.created_date.value ? row.created_date.value : row.created_date;
        }));

        // 5. Filter Data: Keep only records where created_date is NOT in the DB
        const rowsToInsert = data
            .filter(d => !existingDatesSet.has(d.created_date))
            .map(d => ({
                created_date: d.created_date,
                order_sn: d.order_sn, // Can be null/empty as per your requirement
                description: d.description,
                amount: d.amount,
                money_flow: d.money_flow,
                // Create timestamp for when this record was processed
                process_dttm: new Date().toISOString().replace('T', ' ').substring(0, 19)
            }));

        // 6. Batch Insert: Upload all new rows at once
        if (rowsToInsert.length > 0) {
            await bigquery
                .dataset(datasetId)
                .table(tableName)
                .insert(rowsToInsert);
                
            console.log(`[WALLET-TRX] Merged ${rowsToInsert.length} new rows to ${tableName}`);
        } else {
            console.log("[WALLET-TRX] All data already exists. No new rows inserted.");
        }
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

