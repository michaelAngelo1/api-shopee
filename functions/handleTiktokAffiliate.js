import 'dotenv/config';
import crypto from 'crypto';
import axios from 'axios';
import { loadTokens, refreshTokens, getShopCipher } from '../auth/tiktokAuth.js';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
const secretClient = new SecretManagerServiceClient();

export async function handleAffiliate(brand, shopCipher, accessToken) {
    try {   
        console.log("[TIKTOK-AFFILIATE] Fetching tiktok affiliate for brand: ", brand);
        const path = "/affiliate_seller/202410/orders/search";
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";

        let keepFetching = true;
        let currPageToken = "";

        let tiktokAppKey = "6j6u4kmpdda19"
        let tiktokAppSecret = "c4680b9ff6797160adb92104a77e2e1aa085c733"
        
        const appKey = tiktokAppKey;
        const appSecret = tiktokAppSecret;
        const createTimeFrom = Math.floor(new Date("2026-01-01T00:00:00+07:00").getTime() / 1000);
        const createTimeTo = Math.floor(new Date("2026-01-31T00:00:00+07:00").getTime() / 1000);
        
        while(keepFetching) {
            const requestBody = {
                create_time_ge: createTimeFrom,
                create_time_lt: createTimeTo
            }

            const timestamp = Math.floor(Date.now() / 1000);
            const queryParams = {   
                app_key: appKey,
                page_size: 100,
                timestamp: timestamp,
                shop_cipher: shopCipher
            };  
            if(currPageToken) {
                queryParams.page_token = currPageToken;
            }
            const sortedKeys = Object.keys(queryParams).sort();

            let result = appSecret + path;
            for(const key of sortedKeys) {
                result += key + queryParams[key];
            }
            result += JSON.stringify(requestBody);
            result += appSecret;

            const sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');
            queryParams.sign = sign;
            const querySearchParams = new URLSearchParams(queryParams);

            const completeUrl = baseUrl + querySearchParams.toString();
            
            const response = await axios.post(completeUrl, 
                requestBody,
                {
                    headers: {
                        'content-type': 'application/json',
                        'x-tts-access-token': accessToken,
                    },
                }
            );

            console.log("[TIKTOK-AFFILIATE] Affiliate raw response orders: ", response.data.data.orders);

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        }

    } catch (e) {
        console.log("[TIKTOK-AFFILIATE] Error get affiliate info: ", e.response.data.message);
    }
}

export async function handleTiktokAffiliate(brand) {
    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;

    await refreshTokens(brand, refreshToken);

    const shopCipher = await getShopCipher(brand, accessToken);
    console.log("Shop cipher: ", shopCipher);

    await handleAffiliate(brand, shopCipher, accessToken);
}

await handleTiktokAffiliate("Eileen Grace")