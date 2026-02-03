import 'dotenv/config';
import crypto from 'crypto';
import axios from 'axios';

// TODO:
// 1. Bikin function getAccessToken from Secret Manager
// 2. Bikin function refreshTokens, ambil refreshToken dari Secret Manager
// 3. Expiration: access token 7 hari, refresh token lumayan lama.
// *EG_TIKTOK_ACCESS_TOKEN remove dari env & secrets di staging-worker


async function getShopCipher(brand) {
    try {
        const appKey = process.env.EG_TIKTOK_PARTNER_APP_KEY;
        const appSecret = process.env.TIKTOK_PARTNER_APP_SECRET;
        
        const timestamp = Math.floor(Date.now() / 1000);
        const queryParams = "/app_key" + appKey + "timestamp" + timestamp;
        const path = "/authorization/202309/shops"
        const result = appSecret + "/" + path + queryParams;
        const sign = crypto.createHash('sha256').update(result).digest('hex');

        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?"
        const completeUrl = baseUrl + "app_key=" + appKey + "&" + "sign=" + sign + "&" + "timestamp=" + timestamp; 
        
        console.log("Hitting get shop cipher for brand: ", brand);
        console.log("Complete url: ", completeUrl);

        const headers = {
            'content-type': 'application/json',
            'x-tts-access-token': process.env.EG_TIKTOK_ACCESS_TOKEN,
        }
        const params = {
            app_key: process.env.TIKTOK_PARTNER_APP_KEY,
            sign: sign,
            timestamp: timestamp
        }

        const response = await axios.get(completeUrl, {
            headers: headers
        });
        console.log("[TIKTOK-FINANCE] Raw response: ", response?.data?.response);

    } catch (e) {
        console.log("Error get shop cipher on brand: ", brand)
        console.log(e);
    }
}

export async function handleFinance(brand) {
    await getShopCipher(brand);
    // const shopCipher = await getShopCipher(brand);
}