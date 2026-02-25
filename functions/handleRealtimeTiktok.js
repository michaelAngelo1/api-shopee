import { getShopCipher, loadTokens, refreshTokens } from "../auth/tiktokAuth";
import { handleMergeRealtime } from "./handleMergeRealtime";

async function getOrderList(brand, shopCipher, accessToken) {

    try {
        let tiktokAppKey = "6j6u4kmpdda19"
        let tiktokAppSecret = "c4680b9ff6797160adb92104a77e2e1aa085c733"

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
    
        while(keepFetching) {
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
            result += tiktokAppSecret;
            const sign = crypto.createHmac('sha256', tiktokAppSecret).update(result).digest('hex');
            queryParams.sign = sign;
            const querySearchParams = new URLSearchParams(queryParams);
            const completeUrl = baseUrl + querySearchParams.toString();

            const requestBody = {
                create_time_ge: createTimeFrom,
                create_time_lt: createTimeTo,
            }

            const response = await axios.get(completeUrl, 
                requestBody,
                {
                    headers: {
                        'content-type': 'application/json',
                        'x-tts-access-token': accessToken
                    }
                }
            );

            console.log("[TIKTOK-REALTIME] Raw response order list: ", response.data.data.orders);

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        }

    } catch (e) {
        console.log("[TIKTOK-REALTIME] Error getting realtime tiktok data on brand: ", brand);
        console.log(e);
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