import { SecretManagerServiceClient } from "@google-cloud/secret-manager";
import { getEndOfPreviousMonthTimestampWIB, getEndOfYesterdayTimestampWIB, getStartOfMonthTimestampWIB, getStartOfPreviousMonthTimestampWIB } from '../processor.js';
import axios from 'axios';
import crypto from "crypto";
import { getOrderDetailSHRD } from '../api/shrd/getOrderDetailSHRD.js';
import { getEscrowDetailSHRD } from '../api/shrd/getEscrowDetailSHRD.js';
import { handleOrdersSHRD } from '../api/shrd/handleOrdersSHRD.js';
import { getReturnDetailSHRD, getReturnListSHRD } from '../api/shrd/getReturnsSHRD.js';
import { handleReturnsSHRD } from '../api/shrd/handleReturnsSHRD.js';
import { fetchAdsTotalBalance } from "../functions/fetchAdsTotalBalance.js";
import { fetchGMVMaxSpending } from "../functions/fetchGMVMaxSpending.js";
import { fetchTiktokBasicAds } from "../functions/fetchTiktokBasicAds.js";
import { fetchProductGMVMax } from "../functions/fetchProductGMVMax.js";
import { fetchLiveGMVMax } from "../functions/fetchLiveGMVMax.js";
import { handleTiktokAdsData } from "../functions/handleTiktokAdsData.js";
import { fetchPGMVMaxBreakdown } from "../functions/fetchPGMVMaxBreakdown.js";

const secretClient = new SecretManagerServiceClient();

export const PARTNER_ID = parseInt(process.env.SHRD_PARTNER_ID);
export const PARTNER_KEY = process.env.SHRD_PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.SHRD_SHOP_ID);
export const SHRD_INITIAL_ACCESS_TOKEN = process.env.SHRD_INITIAL_ACCESS_TOKEN;
export const SHRD_INITIAL_REFRESH_TOKEN = process.env.SHRD_INITIAL_REFRESH_TOKEN;
const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";

export const HOST = "https://partner.shopeemobile.com";
const PATH = "/api/v2/order/get_order_list";

export let SHRD_ACCESS_TOKEN;
let SHRD_REFRESH_TOKEN;

async function refreshToken() {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: SHRD_REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: SHOP_ID
    }

    console.log("Hitting Refresh Token endpoint SHRD: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    })

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        SHRD_ACCESS_TOKEN = newAccessToken;
        SHRD_REFRESH_TOKEN = newRefreshToken;

        saveTokensToSecret({
            accessToken: SHRD_ACCESS_TOKEN,
            refreshToken: SHRD_REFRESH_TOKEN
        });
    } else {
        console.log("[SHRD] token refresh not found :(")
        throw new Error("Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/shrd-shopee-tokens';
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'utf-8');

    try {
        await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });
        console.log("[SHRD] Saved tokens to SHRD Secret Manager");
    } catch (e) {
        console.log("[SHRD] Error saving tokens to Secret Manager", )
    }
}

async function loadTokensFromSecret() {
    const secretName = 'projects/231801348950/secrets/shrd-shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("[SHRD] Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.log("[SHRD] Error loading tokens from Secret Manager: ", e);
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
        const newReturnList = await getReturnListSHRD(interval.from, interval.to, accessToken);
        if(newReturnList && newReturnList.length > 0) {
            allReturnList = allReturnList.concat(newReturnList);
        } else {
            console.log("SHRD: No returns found for interval: ", interval);
        }
    }
    

    if(allReturnList && allReturnList.length > 0) {
        allReturnDetails = await getReturnDetailSHRD(allReturnList);
        await handleReturnsSHRD(allReturnDetails);
    } else {
        console.log("SHRD: No return details to process.");
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
        console.log(`Interval: ${interval.from} to ${interval.to}`);
        
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
            console.log("Hitting Get Order List SHRD endpoint:", fullUrl);

            try {
                const response = await axios.get(fullUrl, {
                    headers: {
                        'Content-Type': 'application/json'
                    }
                });

                if(response.data && response.data.response && Array.isArray(response.data.response.order_list)) {
                    const onePageOfOrders = response.data.response.order_list;
                    console.log("Fetched one-page of SHRD orders monthly");

                    if(onePageOfOrders.length > 0) {
                        
                        const onePageWithDetail = await getOrderDetailSHRD(onePageOfOrders);
                        const onePageWithEscrow = await getEscrowDetailSHRD(onePageOfOrders);

                        await handleOrdersSHRD(onePageWithDetail, onePageWithEscrow);
                    }

                    hasMore = response.data.response.more;
                    cursor = response.data.response.next_cursor || "";
                } else {
                    hasMore = false;
                }
            } catch (e) {
                console.log("Error fetching SHRD orders: ", e);
                hasMore = false;
            }
        }
    }
}

export async function fetchAndProcessOrdersSHRD() {
    console.log("[SH-RD] Start fetching ads total balance. Calling the function.");
    let brand = "SH-RD";
    let brandTT = "SHRD";

    const loadedTokens = await loadTokensFromSecret();
    SHRD_ACCESS_TOKEN = loadedTokens.accessToken;
    SHRD_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, SHRD_ACCESS_TOKEN, SHOP_ID);
    let adsIdSHRD = "7377330420947632145";
    const basicAdsData = await fetchTiktokBasicAds(brandTT, adsIdSHRD);
    const pgmvMaxData = await fetchProductGMVMax(brandTT, adsIdSHRD);
    const lgmvMaxData = await fetchLiveGMVMax(brandTT, adsIdSHRD);
    
    console.log("[SHRD] All data on: ", brand);
    console.log(basicAdsData);
    console.log(pgmvMaxData);
    console.log(lgmvMaxData);
    console.log("\n");

    await handleTiktokAdsData(basicAdsData, pgmvMaxData, lgmvMaxData, brandTT);

    await fetchPGMVMaxBreakdown(brandTT, adsIdSHRD);
}
