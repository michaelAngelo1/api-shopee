import { getShopCipher, loadTokens, refreshTokens } from "../auth/tiktokAuth.js";
import { getOrderList } from "./handleRealtimeTiktok.js";

async function fetchOrderDetailTiktok() {
    try {

    } catch (e) {
        console.log('Error fetch order detail tiktok: ', e);
    }
}

async function mainRealtimeTiktok(brand) {
    console.log("Main Realtime tiktok: ", brand);
    
    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;
    await refreshTokens(brand, refreshToken);

    const shopCipher = await getShopCipher(brand, accessToken);

    const orders = await getOrderList(brand, shopCipher, accessToken);
    console.log("Orders: ", JSON.stringify(orders.slice(0, 3), 0, 2));
}

await mainRealtimeTiktok("Eileen Grace");