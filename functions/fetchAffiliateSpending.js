import axios from 'axios';
import crypto from 'crypto';

export async function fetchAffiliateSpending(brand, PARTNER_ID, PARTNER_KEY, ACCESS_TOKEN, SHOP_ID) {
    console.log("Fetch Affiliate Spending of brand: ", brand);

    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/payment/get_wallet_transaction_list";

    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${PATH}${timestamp}${ACCESS_TOKEN}${SHOP_ID}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    let create_time_from;
    let create_time_to;
    
    let now = new Date();
    let yesterday = new Date(now);
    yesterday.setDate(now.getDate() - 12);
    yesterday.setHours(0, 0, 0, 0);
    create_time_from = Math.floor(yesterday.getTime() / 1000);

    let todayEnd = new Date(now);
    todayEnd.setHours(23, 59, 59, 999);
    create_time_to = Math.floor(todayEnd.getTime() / 1000);

    console.log("create_time_from: ", create_time_from);
    console.log('create_time_to: ', create_time_to);

    const params = new URLSearchParams({
        partner_id: PARTNER_ID, 
        timestamp,
        access_token: ACCESS_TOKEN,
        shop_id: SHOP_ID,
        sign,
        create_time_from,
        create_time_to,
        transaction_type: "455"
    });

    const fullUrl = `${HOST}${PATH}?${params.toString()}`;
    console.log(`Hitting Affiliate Spending for ${brand}: ${fullUrl}`);

    try {
        const response = await axios.get(fullUrl, {
            headers: {
                'Content-Type': 'application/json'
            }
        })

        if(response && response.data.response) {
            console.log("res affiliate spending eg: ", response.data.response.transaction_list);
        }
    } catch (e) {
        console.log(`Error fetching affiliate spending for ${brand}: ${e}`);
    }
}