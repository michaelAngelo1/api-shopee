import 'dotenv/config';
import crypto from 'crypto';
import axios from 'axios';
import { loadTokens, refreshTokens, getShopCipher } from '../auth/tiktokAuthAffiliate.js';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
const secretClient = new SecretManagerServiceClient();
let appKey = process.env.TIKTOK_AFFILIATE_APP_KEY;
let appSecret = process.env.TIKTOK_AFFILIATE_APP_SECRET;

export async function handleAffiliate(brand, shopCipher, accessToken) {
    try {   
        console.log("[TIKTOK-AFFILIATE] Fetching tiktok affiliate for brand: ", brand);
        const path = "/affiliate_seller/202410/orders/search";
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";

        let keepFetching = true;
        let currPageToken = "";
        const createTimeFrom = Math.floor(new Date("2026-01-01T00:00:00+07:00").getTime() / 1000);
        const createTimeTo = Math.floor(new Date("2026-01-31T23:59:59+07:00").getTime() / 1000);

        let rawAffiliateOrders = [];
        let rawAffiliateOrdersLength = 0;
        
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

            // console.log("[TIKTOK-AFFILIATE] Affiliate raw response orders: ", response.data.data.orders);
            rawAffiliateOrders.push(...response.data.data.orders);
            rawAffiliateOrdersLength += response.data.data.orders.length;   

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        }

        console.log("Affiliate orders qty: ", rawAffiliateOrdersLength);
        console.log("Affiliate Orders. First: ");
        console.log(rawAffiliateOrders[0]);

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