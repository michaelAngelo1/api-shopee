import axios from 'axios';
import crypto from 'crypto';

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
            'order_sn': d.order_sn,
            'description': d.description,
            'amount': d.amount,
            'money_flow': d.money_flow
        }
        transformed.push(obj);
    });
    return transformed;
}

async function mergeData(data, brand) {
    return;
}

export async function handleWalletTransactions(brand, partner_id, partner_key, access_token, shop_id) {
    const transactionContainer = await fetchWalletTransaction(brand, partner_id, partner_key, access_token, shop_id);
    const transformed = await transformData(transactionContainer);

    console.log("Transformed first 3: ");
    console.log(transformed.slice(0, 3));
}

