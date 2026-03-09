import { getShopCipher, loadTokens, refreshTokens } from "../auth/tiktokAuth";
import { handleMergeRealtime } from "./handleMergeRealtime";

async function getOrderList(brand, shopCipher, accessToken) {

    try {
        let tiktokAppKey;
        let tiktokAppSecret;

        if(!secondInternalAppBrands.includes(brand)) {
            tiktokAppKey = "6j6u4kmpdda19"
            tiktokAppSecret = "c4680b9ff6797160adb92104a77e2e1aa085c733"
        } else {
            tiktokAppKey = "6j7inu4s9dkfq"
            tiktokAppSecret = "3493907831adc26d58c74262f709b48a2205a2d0"
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


            const response = await axios.post(completeUrl, 
                requestBody,
                {
                    headers: {
                        'content-type': 'application/json',
                        'x-tts-access-token': accessToken
                    }
                }
            );

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
        console.log(e.response.data.message);
    }
}

export async function mainRealtimeTiktok(brand) {
    console.log("Main Realtime tiktok: ", brand);

    await refreshTokens(brand, refreshToken);
    
    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;

    let brand = "Eileen Grace";
    const shopCipher = await getShopCipher(brand, accessToken);

    await getOrderList(brand, shopCipher, accessToken);

    // let totalSalesBrand = 0;
    // let marketplace = "Tiktok";
    // await handleMergeRealtime(brand, marketplace, totalSalesBrand);
}