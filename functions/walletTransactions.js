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
            const createTimeFrom = Math.floor(new Date("2026-01-01") / 1000);
            const createTimeTo = Math.floor(new Date("2026-01-15") / 1000);
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
    'Eileen Grace': "eileen_grace_wallet_trx",
}

async function mergeData(data, brand) {
    console.log("[WALLET-TRX] Start merging for brand: ", brand);
    const tableName = brandTables[brand];
    const bigquery = new BigQuery();
    const datasetId = 'shopee_api';

    try {
        console.log('[WALLET-TRX] Data before merge');
        for(const d of data) {
            console.log(d);
            // await bigquery
            //     .dataset(datasetId)
            //     .table(tableName)
            //     .insert({
            //         created_date: d.created_date,
            //         order_sn: d.order_sn,
            //         description: d.description,
            //         amount: d.amount,
            //         money_flow: d.money_flow,
            //         process_dttm: new Date(Date.now() + 7 * 60 * 60 * 1000).toISOString().replace('T', ' ').substring(0, 19)
            //     });
        }
        console.log("[WALLET-TRX] Merged to table: ", tableName);
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

