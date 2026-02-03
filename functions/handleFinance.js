import 'dotenv/config';
import crypto from 'crypto';
import axios from 'axios';

// TODO:
// 1. Bikin function getAccessToken from Secret Manager
// 2. Bikin function refreshTokens, ambil refreshToken dari Secret Manager
// 3. Expiration: access token 7 hari, refresh token lumayan lama.
// *EG_TIKTOK_ACCESS_TOKEN remove dari env & secrets di staging-worker

// curl -X GET "https://auth.tiktok-shops.com/api/v2/token/get?app_key=6ivpj08pq3t4s&app_secret=e9212b70ae318a3704c6ed8673b138ec6cb5723b&auth_code=ROW_tVrf5wAAAAD8IPyM8KSM-yD2plONL3kO-IMXvK3MG9MPLMMg9OGJq8dn4OPP6AZsagxf6CRaIVCvnt7TTe6m6YKF_Ki4fJm4a9wyqQ3j2mleooaCdDXXwbsHaq8NoQvZt8MmwO68DZdMN39CU_dx22kmTgMq4gqh&grant_type=authorized_code"

/*
{
    "accessToken": "ROW_srIgowAAAAAoYfgyHQFZ7-j_QZLXj-NIE7tVpLQ_4mgfh2_6zs-7CJ4Q1BEsf77UI2n3YNPBFU20ry4-pHZMscMWnkELuazsobxSPhr4-OW1me6jDyICFDKKISlLPz_HnlykJj1_hrfyqhhmvsEYgJZ6mQzOP97lLtGdlGsfht8IP4N2VAm_oQ",
    "refreshToken": "ROW_zKjNPwAAAAB3gdLG6OS-uLqmlp1aPMJFqq77Pi6EZ3aGXKS9_uMHfMgLKbKMfJq_oTY8zFyJ7BA"
}
*/

async function loadTokens() {

}

async function refreshTokens() {

}

async function getShopCipher(brand) {
    try {
        const appKey = process.env.TIKTOK_PARTNER_APP_KEY;
        const appSecret = process.env.TIKTOK_PARTNER_APP_SECRET;
        
        const timestamp = Math.floor(Date.now() / 1000);
        const queryParams = "app_key" + appKey + "timestamp" + timestamp;
        const path = "/authorization/202309/shops" // If fail, append "/"
        const result = appSecret + path + queryParams + appSecret;
        const sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');

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
        // console.log("[TIKTOK-FINANCE] Raw response: ", response.data.data);

        let authorizedShops = response.data.data.shops;
        let shopCipher = "";
        for(const shop of authorizedShops) {
            if(shop.name === brand) {
                shopCipher = shop.cipher;
            }
        }

        return shopCipher;

    } catch (e) {
        console.log("Error get shop cipher on brand: ", brand)
        console.log(e);
    }
}

export async function handleFinance(brand) {
    const shopCipher = await getShopCipher(brand);
    console.log("Shop cipher for brand: ", brand, ": ", shopCipher);
    // const shopCipher = await getShopCipher(brand);
}