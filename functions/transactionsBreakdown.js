import crypto from 'crypto';
import axios from 'axios';
import { getShopCipher, loadTokens, refreshTokens } from "../auth/tiktokAuthTransaction.js";

const secondTransactionBrands = [
    "Mirae",
    "Swissvita",
    "G-Belle",
    "Past Nine",
    "Nutri & Beyond",
    "Ivy & Lily",
    "Naruko",
    "Relove",
    "Joey & Roo",
    "Rocketindo Shop"
]

async function getOrderIdList(brand, shopCipher, accessToken) {
    try {
        let appKey, appSecret;
        if(!secondTransactionBrands.includes(brand)) {
            appKey = "6jalpvras8n00"
            appSecret = "608e7a9c85afc968d0baa47f6f93258d3ab51949"
        } else {
            appKey = "6jbdera7i5q9b";
            appSecret = "7955bd57fa190f1d6ea27514f45b884192e474f0";
        }

        const path = "/order/202309/orders/search";
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";

        let keepFetching = true;
        let currPageToken = "";

        const createTimeFrom = Math.floor(new Date("2026-01-01T00:00:00+07:00").getTime() / 1000);
        const createTimeTo = Math.floor(new Date("2026-01-31T23:59:59+07:00").getTime() / 1000);

        let rawOrderIds = [];

        while(keepFetching) {
            const timestamp = Math.floor(Date.now() / 1000);
            const queryParams = {
                app_key: appKey, 
                timestamp: timestamp,
                page_size: 100,
                shop_cipher: shopCipher
            }
            if(currPageToken) {
                queryParams.page_token = currPageToken;
            }
            const sortedKeys = Object.keys(queryParams).sort();

            let result = appSecret + path;
            for(const key of sortedKeys) {
                result += key + queryParams[key];
            }
            result += appSecret;
            const sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');
            queryParams.sign = sign;
            const querySearchParams = new URLSearchParams(queryParams);
            const completeUrl = baseUrl + querySearchParams.toString();

            const requestBody = {
                create_time_ge: createTimeFrom,
                create_time_lt: createTimeTo,
            }

            const response = await axios.post(completeUrl, 
                requestBody,
                {
                    headers: {
                        'content-type': 'application/json',
                        'x-tts-access-token': accessToken
                    }
                }
            );

            console.log("[TIKTOK-REALTIME] Raw response order list: ", response.data.data.orders);
            rawOrderIds.push(...response.data.data.orders.map(o => o.id));

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        }

        return rawOrderIds;

    } catch (e) {
        console.log("[TIKTOK-REALTIME] Error getting realtime tiktok data on brand: ", brand);
        console.log(e);
    }
}

async function fetchTransactionsBreakdown(brand, shopCipher, accessToken) {
    try {
        let appKey, appSecret;
        if(!secondTransactionBrands.includes(brand)) {
            appKey = "6jalpvras8n00"
            appSecret = "608e7a9c85afc968d0baa47f6f93258d3ab51949"
        } else {
            appKey = "6jbdera7i5q9b";
            appSecret = "7955bd57fa190f1d6ea27514f45b884192e474f0";
        }

        console.log("[TIKTOK-TRANSACTION] Fetch transactions breakdown on brand: ", brand);

    } catch (e) {   
        console.log("[TIKTOK-TRANSACTION] Error transaction tiktok on brand: ", brand);
        console.log(e.response.data.message);
    }
}

async function handleTransactionsBreakdown(brand) {
    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;

    await refreshTokens(brand, refreshToken);

    const shopCipher = await getShopCipher(brand, accessToken);
    console.log("Shop cipher: ", shopCipher);

    const rawOrderIds = await getOrderIdList(brand, shopCipher, accessToken);
    console.log("FIRST 10 ORDER IDs");
    console.log(rawOrderIds.slice(0, 10));

    // Call fetchTransactionsBreakdown here
}

async function mainTransactionsBreakdown() {
    await handleTransactionsBreakdown("Eileen Grace");
}

mainTransactionsBreakdown();