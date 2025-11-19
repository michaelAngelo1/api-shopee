import { getReturnDetail, getReturnList } from './api/eileen_grace/getReturns.js';
import { getEscrowDetail } from './api/eileen_grace/getEscrowDetail.js';
import { getOrderDetail } from './api/eileen_grace/getOrderDetail.js';
import { handleReturns } from './api/eileen_grace/handleReturns.js';
import { handleOrders } from './api/eileen_grace/handleOrders.js';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios from 'axios';
import crypto from 'crypto';
import 'dotenv/config';
import { fetchAdsTotalBalance } from './functions/fetchAdsTotalBalance.js';
import { fetchAffiliateSpending } from './functions/fetchAffiliateSpending.js';
import { fetchGMVMaxSpending } from './functions/fetchGMVMaxSpending.js';
import { fetchTiktokBasicAds } from './functions/fetchTiktokBasicAds.js';
// import fs from 'fs';
// import path from 'path';
// import { fileURLToPath } from 'url';

const port = 3000
let secretClient;

export const HOST = "https://partner.shopeemobile.com";
const PATH = "/api/v2/order/get_order_list";

export const PARTNER_ID = parseInt(process.env.PARTNER_ID);
export const PARTNER_KEY = process.env.PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.SHOP_ID);

const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";

export let ACCESS_TOKEN;
let REFRESH_TOKEN;

async function refreshToken() {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');

    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: SHOP_ID
    }

    console.log("Hitting Refresh Token endpoint: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    });

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        ACCESS_TOKEN = newAccessToken;
        REFRESH_TOKEN = newRefreshToken;

        // saveTokensToFile({ accessToken: ACCESS_TOKEN, refreshToken: REFRESH_TOKEN });

        saveTokensToSecret({ accessToken: ACCESS_TOKEN, refreshToken: REFRESH_TOKEN });
    } else {
        console.log("[EG] Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/shopee-tokens';
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'UTF-8');
    secretClient = new SecretManagerServiceClient();

    try {
        await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });

        console.log("Saved Shopee tokens to Secret Manager");
    } catch (e) {
        console.log("Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    secretClient = new SecretManagerServiceClient();
    const secretName = 'projects/231801348950/secrets/shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.log("Error loading tokens from Secret Manager: ", e);
    }
}

const jakartaOffset = 7 * 60 * 60;

function getJakartaTimestampTimeFrom(year, month, day, hour, minute, second) {
    const date = new Date(Date.UTC(year, month, day, hour, minute, second));
    return Math.floor(date.getTime() / 1000) - jakartaOffset;
}

function getJakartaTimestampTimeTo(year, month, day, hour, minute, second) {
    const date = new Date(Date.UTC(year, month, day, hour, minute, second));
    return Math.floor(date.getTime() / 1000) - jakartaOffset;
}

export function getStartOfMonthTimestampWIB() {
    const now = new Date(); 
    const year = now.getFullYear();
    const month = now.getMonth();
    const startOfMonthUTC = new Date(Date.UTC(year, month, 0, 0, 0, 0)); 
    const startOfMonthWIB = new Date(startOfMonthUTC.getTime() - (7 * 60 * 60 * 1000));
    return Math.floor(startOfMonthWIB.getTime() / 1000);
}

export function getEndOfYesterdayTimestampWIB() {
    const now = new Date(); 
    const startOfTodayUTC = new Date(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0));
    const startOfTodayWIB = new Date(startOfTodayUTC.getTime() - (7 * 60 * 60 * 1000));
    const endOfYesterdayWIB = new Date(startOfTodayWIB.getTime() - 1000); // Subtract 1 second
    return Math.floor(endOfYesterdayWIB.getTime() / 1000);
}

// Function to get UTC Unix timestamp for the START of the PREVIOUS month in WIB
export function getStartOfPreviousMonthTimestampWIB() {
    const now = new Date();
    // Calculate the year and month of the previous month
    let prevMonthYear = now.getFullYear();
    let prevMonthMonth = now.getMonth() - 1; // Month is 0-indexed (Jan=0)
    if (prevMonthMonth < 0) {
        prevMonthMonth = 11; // December
        prevMonthYear--;
    }
    // Create date for the 1st of the PREVIOUS month IN UTC, then adjust back 7 hours for WIB
    const startOfPrevMonthUTC = new Date(Date.UTC(prevMonthYear, prevMonthMonth, 1, 0, 0, 0));
    const startOfPrevMonthWIB = new Date(startOfPrevMonthUTC.getTime() - (7 * 60 * 60 * 1000));
    return Math.floor(startOfPrevMonthWIB.getTime() / 1000);
}

// Function to get UTC Unix timestamp for the END of the PREVIOUS month in WIB
export function getEndOfPreviousMonthTimestampWIB() {
    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth(); // Current month
    // Create date for the 1st of the CURRENT month IN UTC, adjust back 7 hours for WIB midnight
    const startOfMonthUTC = new Date(Date.UTC(year, month, 1, 0, 0, 0));
    const startOfMonthWIB = new Date(startOfMonthUTC.getTime() - (7 * 60 * 60 * 1000));
    // Subtract 1 second to get the end of the PREVIOUS month WIB
    const endOfPrevMonthWIB = new Date(startOfMonthWIB.getTime() - 1000);
    return Math.floor(endOfPrevMonthWIB.getTime() / 1000);
}

export async function fetchAndProcessOrders() {
    console.log("[EG] Start fetching ads total balance. Calling the function.");
    let brand = "Eileen Grace";

    const loadedTokens = await loadTokensFromSecret();
    ACCESS_TOKEN = loadedTokens.accessToken;
    REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, ACCESS_TOKEN, SHOP_ID);

    let advIdEG = "6899326735087566850";
    await fetchGMVMaxSpending(brand, advIdEG);

    await fetchTiktokBasicAds(brand, advIdEG);
}