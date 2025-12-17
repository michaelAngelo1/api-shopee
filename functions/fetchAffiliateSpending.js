import axios from 'axios';
import crypto from 'crypto';

export async function fetchAffiliateSpending(brand, PARTNER_ID, PARTNER_KEY, ACCESS_TOKEN, SHOP_ID) {
    console.log("Fetch Affiliate Spending of brand: ", brand);

    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/ams/get_managed_affiliate_list";

    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${PATH}${timestamp}${ACCESS_TOKEN}${SHOP_ID}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');

    const params = new URLSearchParams({
        partner_id: PARTNER_ID, 
        timestamp,
        access_token: ACCESS_TOKEN,
        shop_id: SHOP_ID,
        sign,
        page_no: 1,
        page_size: 20
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
            console.log("[AFFILIATE] res affiliate spending eg: ", response.data.response.transaction_list);
        } else {
            console.error(response);
        }
    } catch (e) {
        console.error(`[AFFILIATE] Error fetching affiliate spending for ${brand}`);
        console.log(e);
    }
}