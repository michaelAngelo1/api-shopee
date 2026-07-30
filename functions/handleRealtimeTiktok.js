import { getShopCipher, loadTokens, refreshTokens } from "../auth/tiktokAuth.js";
import crypto from 'crypto';
import { handleMergeRealtime, loadCredentials } from "./handleMergeRealtime.js";
import { requestWithRetry } from "./httpRetry.js";

const internalAppBrands = {
    "Eileen Grace": 1,
    "Mamaway": 1, 
    "SHRD": 1,
    "CHESS": 1,
    "Miss Daisy": 1,
    "Polynia": 1,
    "CHESS": 1,
    "Cléviant": 1,
    "Mossèru": 1,
    "Evoke": 1,
    "Dr Jou": 1,
    "Mirae": 2,
    "Swissvita": 2,
    "G-Belle": 2,
    "Past Nine": 2,
    "Nutri & Beyond": 2,
    "Ivy & Lily": 2,
    "Naruko": 2,
    "Relove": 2,
    "Joey & Roo": 2, 
    "Rocketindo Shop": 2,
    "M2": 3,
    "PAZZO": 3
}

async function getOrderList(brand, shopCipher, accessToken) {

    try {
        let tiktokAppKey;
        let tiktokAppSecret;

        if(internalAppBrands[brand] == 1) {
            tiktokAppKey = "6j6u4kmpdda19"
            tiktokAppSecret = "c4680b9ff6797160adb92104a77e2e1aa085c733"
        } else if(internalAppBrands[brand] == 2) {
            tiktokAppKey = "6j7inu4s9dkfq"
            tiktokAppSecret = "3493907831adc26d58c74262f709b48a2205a2d0"
        } else {
            tiktokAppKey = "6jbrll2ed26dp";
            tiktokAppSecret = "04679ae180556cdc79b11a3e7cbd8da33f0d6e92";
        }

        // let tiktokAppKey = process.env.TIKTOK_PARTNER_APP_KEY;
        // let tiktokAppSecret = process.env.TIKTOK_PARTNER_APP_SECRET;

        const path = "/order/202309/orders/search";
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";

        let keepFetching = true;
        let currPageToken = "";
        
        const nowSeconds = Math.floor(Date.now() / 1000);
        const jakartaOffset = 25200;
        const secondsPassedToday = (nowSeconds + jakartaOffset) % 86400;
        const JAKARTA_MIDNIGHT_TS = nowSeconds - secondsPassedToday;

        const createTimeFrom = JAKARTA_MIDNIGHT_TS;
        const createTimeTo = nowSeconds;
        let orderTotal = 0;
        let orders = [];

        while(keepFetching) {
            const requestBody = {
                create_time_ge: createTimeFrom,
                create_time_lt: createTimeTo,
            }

            const timestamp = Math.floor(Date.now() / 1000);
            const queryParams = {
                app_key: tiktokAppKey, 
                timestamp: timestamp,
                page_size: 100,
                shop_cipher: shopCipher
            }
            if(currPageToken) {
                queryParams.page_token = currPageToken;
            }
            const sortedKeys = Object.keys(queryParams).sort();

            let result = tiktokAppSecret + path;
            for(const key of sortedKeys) {
                result += key + queryParams[key];
            }
            result += JSON.stringify(requestBody);
            result += tiktokAppSecret;

            const sign = crypto.createHmac('sha256', tiktokAppSecret).update(result).digest('hex');
            queryParams.sign = sign;
            const querySearchParams = new URLSearchParams(queryParams);
            const completeUrl = baseUrl + querySearchParams.toString();


            const response = await requestWithRetry({
                method: 'post',
                url: completeUrl,
                data: requestBody,
                headers: {
                    'content-type': 'application/json',
                    'x-tts-access-token': accessToken
                }
            }, { label: `tiktok-orders ${brand}` });

            // console.log("[TIKTOK-REALTIME] Raw response order list: ", response);

            if(response.data.data && response.data.data.orders) {
                orders.push(...response.data.data.orders);
                orderTotal += response.data.data.orders.length;
            }

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        }

        // This number may be inflated due to: unspecified order status (should be other than UNPAID)
        // Next step should account for order status
        // If is_cod = true, then pay_time can be empty
        // If is_cod = false, then pay_time can not be empty.
        console.log("Order total on brand: ", brand, " length: ", orderTotal);

        await processOrdersGMV(brand, orders, "TIKTOK_SHOP");
        await processOrdersGMV(brand, orders, "TOKOPEDIA");
    } catch (e) {
        console.log("[TIKTOK-REALTIME] Error getting realtime tiktok data on brand: ", brand);
        console.log(e.response?.data?.message || e.message || e);
    }
}

async function processOrdersGMV(brand, orders, commerce) {
    const tiktokOnlyOrders = orders.filter(o => o.commerce_platform === commerce);
    
    let cleanedOrdersNonCOD = tiktokOnlyOrders.filter(o => {
        return o.is_cod === false && o.paid_time > 0;
    });

    let cleanedOrdersCOD = tiktokOnlyOrders.filter(o => {
        return o.is_cod === true && o.status !== "UNPAID";
    });

    let totalCleanedOrders = cleanedOrdersNonCOD.concat(cleanedOrdersCOD);

    let totalAmount = 0;
    totalCleanedOrders.forEach(o => {
        totalAmount += parseFloat(o.payment.sub_total) + parseFloat(o.payment.platform_discount);
    });
    console.log("Total amount GMV: ", totalAmount, "on commerce: ", commerce, " brand: ", brand);
    
    let marketplace = commerce == "TIKTOK_SHOP" ? "TikTok" : "Tokopedia";
    await handleMergeRealtime(brand, marketplace, totalAmount, totalCleanedOrders.length)
}

async function mainRealtimeTiktok(brand) {
    console.log("Main Realtime tiktok: ", brand);
    
    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;
    await refreshTokens(brand, refreshToken);

    const shopCipher = await getShopCipher(brand, accessToken);

    await getOrderList(brand, shopCipher, accessToken);
}

export async function parentRealtimeTiktok() {
    const brands = [
        "Eileen Grace", "Mamaway", "SHRD", "Miss Daisy", "Polynia", "CHESS",
        "Cléviant", "Mossèru", "Evoke", "Dr Jou", "Mirae", "Swissvita",
        "G-Belle", "Past Nine", "Nutri & Beyond", "Ivy & Lily", "Naruko",
        "Relove", "Joey & Roo", "Rocketindo Shop", "M2", 
        "PAZZO"
    ];

    for (const brand of brands) {
        try {
            await mainRealtimeTiktok(brand);
        } catch (e) {
            console.log(`[TIKTOK-REALTIME] Brand failed, skipping: ${brand}`);
            console.log(e.response?.data?.message || e.message || e);
        }
    }
}

// Uncomment for production.
// await parentRealtimeTiktok();