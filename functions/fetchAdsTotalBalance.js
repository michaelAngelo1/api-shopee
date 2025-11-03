import axios from 'axios';
import crypto from 'crypto';

export async function fetchAdsTotalBalance(brand, partner_id, partner_key, accessToken, shop_id) {
    console.log("Fetch Ads Total Balance of brand: ", brand);
    
    const HOST = "https://partner.shopeemobile.com"
    const PATH = "/api/v2/ads/get_all_cpc_ads_daily_performance";

    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${partner_id}${PATH}${timestamp}${accessToken}${shop_id}`;
    const sign = crypto.createHmac('sha256', partner_key)
        .update(baseString)
        .digest('hex');

    const yesterday = new Date(Date.now() - 86400000);
    const day = String(yesterday.getDate()).padStart(2, '0');
    const month = String(yesterday.getMonth() + 1).padStart(2, '0'); 
    const year = yesterday.getFullYear();
    const yesterdayString = `${day}-${month}-${year}`;
    
    const params = new URLSearchParams({
        partner_id: partner_id,
        timestamp,
        access_token: accessToken,
        shop_id: shop_id,
        sign,
        start_date: yesterdayString,
        end_date: yesterdayString
    });

    const fullUrl = `${HOST}${PATH}?${params.toString()}`;
    console.log(`Hitting Ads Total Balance for ${brand}: `, fullUrl);

    try {
        const response = await axios.get(fullUrl, {
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if(response && response.data.response) {
            console.log(`${brand} Ads Total Balance: ${response.data.response[0].expense} on ${response.data.response[0].date}`);
        }
    } catch (e) {
        console.log(`Error fetching total balance for ${brand}`);
    }
}