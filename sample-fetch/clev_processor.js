import { SecretManagerServiceClient } from "@google-cloud/secret-manager";
import axios from 'axios';
import crypto from 'crypto';
import { getEndOfPreviousMonthTimestampWIB, getEndOfYesterdayTimestampWIB, getStartOfMonthTimestampWIB, getStartOfPreviousMonthTimestampWIB } from '../processor.js';
import { getOrderDetailCLEV } from '../api/cleviant/getOrderDetailCLEV.js';
import { getEscrowDetailCLEV } from '../api/cleviant/getEscrowDetailCLEV.js';
import { handleOrdersCLEV } from '../api/cleviant/handleOrdersCLEV.js';
import { getReturnDetailCLEV, getReturnListCLEV } from '../api/cleviant/getReturnsCLEV.js';
import { handleReturnsCLEV } from '../api/cleviant/handleReturnsCLEV.js';
import { fetchAdsTotalBalance } from "../functions/fetchAdsTotalBalance.js";
import { fetchGMVMaxSpending } from "../functions/fetchGMVMaxSpending.js";
import { fetchTiktokBasicAds } from "../functions/fetchTiktokBasicAds.js";
import { fetchProductGMVMax } from "../functions/fetchProductGMVMax.js";
import { fetchLiveGMVMax } from "../functions/fetchLiveGMVMax.js";
import { handleTiktokAdsData } from "../functions/handleTiktokAdsData.js";

const secretClient = new SecretManagerServiceClient();

export const PARTNER_ID = parseInt(process.env.CLEVIANT_PARTNER_ID);
export const PARTNER_KEY = process.env.CLEVIANT_PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.CLEVIANT_SHOP_ID);
const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";

export const HOST = "https://partner.shopeemobile.com";
const PATH = "/api/v2/order/get_order_list";

export let CLEV_ACCESS_TOKEN;
let CLEV_REFRESH_TOKEN;

async function refreshToken() {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: CLEV_REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: SHOP_ID
    }

    console.log("Hitting Refresh Token endpoint CLEV: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    })

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        CLEV_ACCESS_TOKEN = newAccessToken;
        CLEV_REFRESH_TOKEN = newRefreshToken;

        saveTokensToSecret({
            accessToken: CLEV_ACCESS_TOKEN,
            refreshToken: CLEV_REFRESH_TOKEN
        });
    } else {
        console.log("[CLEV] token refresh not found :(")
        throw new Error("Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/clev-shopee-tokens';
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'utf-8');

    try {
        await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });
        
        console.log("[CLEV] Successfully saved tokens to CLEV Secret Manager: ", parent);
    } catch (e) {
        console.error("[CLEV] Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    const secretName = 'projects/231801348950/secrets/clev-shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("[CLEV] Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.error("[CLEV] Error loading tokens from Secret Manager: ", e);
    }
}

async function fetchReturnsByTimeframe(timeFrom, timeTo, accessToken) {
    let intervals = [];
    let start = timeFrom;
    while(start < timeTo) {
        let end = Math.min(start + 15 * 24 * 60 * 60 - 1, timeTo);
        intervals.push({ from: start, to: end});
        start = end + 1;
    }

    let allReturnList = [];
    let allReturnDetails = [];
    for(const interval of intervals) {
        const newReturnList = await getReturnListCLEV(interval.from, interval.to, accessToken);
        if(newReturnList && newReturnList.length > 0) {
            allReturnList = allReturnList.concat(newReturnList);
        } else {
            console.log("CLEV: No returns found for interval: ", interval);
        }
    }
    

    if(allReturnList && allReturnList.length > 0) {
        allReturnDetails = await getReturnDetailCLEV(allReturnList);
        await handleReturnsCLEV(allReturnDetails);
    } else {
        console.log("CLEV: No return details to process.");
    }
}

async function fetchByTimeframe(timeFrom, timeTo, accessToken) {
    // Divide the timeframe into 15-day intervals
    let intervals = [];
    let start = timeFrom;
    while(start < timeTo) {
        let end = Math.min(start + 15 * 24 * 60 * 60 - 1, timeTo);
        intervals.push({ from: start, to: end});
        start = end + 1;
    }

    for(const interval of intervals) {
        console.log(`[CLEV] Interval: ${interval.from} to ${interval.to}`);
        
        let hasMore = true;
        let cursor = "";
        
        while(hasMore) {

            const timestamp = Math.floor(Date.now() / 1000);
            const baseString = `${PARTNER_ID}${PATH}${timestamp}${accessToken}${SHOP_ID}`;
            const sign = crypto.createHmac('sha256', PARTNER_KEY)
                .update(baseString)
                .digest('hex');
            
            const params = new URLSearchParams({
                partner_id: PARTNER_ID,
                timestamp,
                access_token: accessToken,
                shop_id: SHOP_ID,
                sign,
                time_range_field: "create_time",
                time_from: interval.from,
                time_to: interval.to,
                page_size: 100,
            });

            if(cursor) params.append('cursor', cursor);

            const fullUrl = `${HOST}${PATH}?${params.toString()}`;
            console.log("Hitting Get Order List CLEV endpoint:", fullUrl);

            try {
                const response = await axios.get(fullUrl, {
                    headers: {
                        'Content-Type': 'application/json'
                    }
                });

                if(response.data && response.data.response && Array.isArray(response.data.response.order_list)) {
                    const onePageOfOrders = response.data.response.order_list;
                    console.log("Fetched one-page of CLEV orders monthly");

                    if(onePageOfOrders.length > 0) {
                        
                        const onePageWithDetail = await getOrderDetailCLEV(onePageOfOrders);
                        const onePageWithEscrow = await getEscrowDetailCLEV(onePageOfOrders);

                        await handleOrdersCLEV(onePageWithDetail, onePageWithEscrow);
                    }

                    hasMore = response.data.response.more;
                    cursor = response.data.response.next_cursor || "";
                } else {
                    hasMore = false;
                }
            } catch (e) {
                console.log("Error fetching CLEV orders: ", e);
                hasMore = false;
            }
        }
    }
}

export async function fetchAndProcessOrdersCLEV() {
    console.log("[CLEV] Start fetching ads total balance. Calling the function.");
    let brand = "Cleviant";

    const loadedTokens = await loadTokensFromSecret();
    CLEV_ACCESS_TOKEN = loadedTokens.accessToken;
    CLEV_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, CLEV_ACCESS_TOKEN, SHOP_ID);

    let advIdClev = "7553576714043965448";

    const basicAdsData = await fetchTiktokBasicAds(brand, advIdClev);
    const pgmvMaxData = await fetchProductGMVMax(brand, advIdClev);
    const lgmvMaxData = await fetchLiveGMVMax(brand, advIdClev);
    
    console.log("[CLEV] All data on: ", brand);
    console.log(basicAdsData);
    console.log(pgmvMaxData);
    console.log(lgmvMaxData);
    console.log("\n");

    await handleTiktokAdsData(basicAdsData, pgmvMaxData, lgmvMaxData, brand);
}
