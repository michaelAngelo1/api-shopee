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
import { fetchProductGMVMax } from './functions/fetchProductGMVMax.js';
import { fetchLiveGMVMax } from './functions/fetchLiveGMVMax.js';
import { handleTiktokAdsData } from './functions/handleTiktokAdsData.js';
import { fetchPGMVMaxBreakdown } from './functions/fetchPGMVMaxBreakdown.js';
import { fetchAdsProductLevel } from './functions/fetchAdsProductLevel.js';
import { fetchAffiliateData } from './functions/amsProcessor.js';
import { mainDanaDilepas } from './functions/escrowProcessor.js';
import { handleWalletTransactions } from './functions/walletTransactions.js';
import { handleFinance } from './functions/handleFinance.js';
// import fs from 'fs';
// import path from 'path';
// import { fileURLToPath } from 'url';

const port = 3000
const secretClient = new SecretManagerServiceClient();

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

        await saveTokensToSecret({ accessToken: ACCESS_TOKEN, refreshToken: REFRESH_TOKEN });
    } else {
        console.log("[EG] Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/shopee-tokens';
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'UTF-8');

    try {
        const [newVersion] = await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });

        console.log("Saved Shopee tokens to Secret Manager");

        // Destroying previous token version
        const [versions] = await secretClient.listSecretVersions({
            parent: parent
        });

        for (const version of versions) {
            if (version.name !== newVersion.name && version.state !== 'DESTROYED') {
                try {
                    await secretClient.destroySecretVersion({
                        name: version.name
                    });
                    console.log(`Destroyed old token version: ${version.name}`);
                } catch (destroyError) {
                    console.error(`Failed to destroy version ${version.name}:`, destroyError);
                }
            }
        }
    } catch (e) {
        console.log("Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
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

    // await handleFinance(brand);

    // await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, ACCESS_TOKEN, SHOP_ID);
    // await mainDanaDilepas(brand, PARTNER_ID, PARTNER_KEY, ACCESS_TOKEN, SHOP_ID);
    
    await handleWalletTransactions(brand, PARTNER_ID, PARTNER_KEY, ACCESS_TOKEN, SHOP_ID);
    
    // await fetchAdsProductLevel(brand, PARTNER_ID, PARTNER_KEY, ACCESS_TOKEN, SHOP_ID);


    // await fetchAffiliateData(brand, SHOP_ID, 1000);

    // let advIdEG = "6899326735087566850";
    // const basicAdsData = await fetchTiktokBasicAds(brand, advIdEG);
    // const pgmvMaxData = await fetchProductGMVMax(brand, advIdEG);
    // const lgmvMaxData = await fetchLiveGMVMax(brand, advIdEG);
    
    // console.log("[EG] All data on: ", brand);
    // console.log(basicAdsData);
    // console.log(pgmvMaxData);
    // console.log(lgmvMaxData);
    // console.log("\n");

    // await handleTiktokAdsData(basicAdsData, pgmvMaxData, lgmvMaxData, brand);

    // await fetchPGMVMaxBreakdown(brand, advIdEG);

    // Rocketindo Shop
    // let advIdRshop = "7581835025746771976";
    // let brandRshop = "Rocketindo Shop"
    
    // const basicAdsDataRshop = await fetchTiktokBasicAds(brandRshop, advIdRshop, 50000);
    // const pgmvMaxDataRshop = await fetchProductGMVMax(brandRshop, advIdRshop, 52000);
    // const lgmvMaxDataRshop = await fetchLiveGMVMax(brandRshop, advIdRshop, 54000);

    // await handleTiktokAdsData(basicAdsDataRshop, pgmvMaxDataRshop, lgmvMaxDataRshop, brandRshop);

    // await fetchPGMVMaxBreakdown(brandRshop, advIdRshop)

    // Naruko, Relove, JR, Enchante
    await handleNaruko();
    await handleRelove();
    await handleJR();
    await handleEnchante();
    await handleRocketindoShop();
}

export const DRJOU_PARTNER_ID = parseInt(process.env.DRJOU_PARTNER_ID);
export const DRJOU_PARTNER_KEY = process.env.DRJOU_PARTNER_KEY;
const NEW_BRANDS_REFRESH_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";
let NEW_BRANDS_ACCESS_TOKEN, NEW_BRANDS_REFRESH_TOKEN;

async function refreshTokenNewBrands(brand, shop_id) {
    console.log("Refreshing token for brand: ", brand);

    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${DRJOU_PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', DRJOU_PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${NEW_BRANDS_REFRESH_URL}?partner_id=${DRJOU_PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: NEW_BRANDS_REFRESH_TOKEN,
        partner_id: DRJOU_PARTNER_ID,
        shop_id: shop_id
    }

    console.log("Hitting Refresh Token endpoint New Brands: ", fullUrl);

    try {
        const response = await axios.post(fullUrl, body, {
            headers: {
                'Content-Type': 'application/json'
            }
        })
    
        const newAccessToken = response.data.access_token;
        const newRefreshToken = response.data.refresh_token;
    
        if(newAccessToken && newRefreshToken) {
            NEW_BRANDS_ACCESS_TOKEN = newAccessToken;
            NEW_BRANDS_REFRESH_TOKEN = newRefreshToken;
    
            await saveTokensNewBrands(brand, {
                accessToken: NEW_BRANDS_ACCESS_TOKEN,
                refreshToken: NEW_BRANDS_REFRESH_TOKEN
            });
        } else {
            console.log("[NEW-BRANDS] token refresh not found :(")
            throw new Error("NEW BRANDS Tokens dont exist");
        }
    } catch (e) {
        console.log("[NEW-BRANDS] Error refreshing new brands token: ", e);
    }
}

let brandSecret = {
    "Naruko": "projects/231801348950/secrets/naruko-shopee-tokens",
    "Relove": "projects/231801348950/secrets/relove-shopee-tokens",
    "Joey & Roo": "projects/231801348950/secrets/joey-roo-shopee-tokens",
    "Enchante": "projects/231801348950/secrets/enchante-shopee-tokens",
    "Rocketindo Shop": "projects/231801348950/secrets/rocketindoshop-shopee-tokens",
}

async function saveTokensNewBrands(brand, tokens) {
    let parent = brandSecret[brand];
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'utf-8');
    try {
        const [newVersion] = await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });

        console.log("[NEW-BRANDS] Saved Shopee tokens to Secret Manager");

        // Destroying previous token version
        const [versions] = await secretClient.listSecretVersions({
            parent: parent
        });

        for (const version of versions) {
            if (version.name !== newVersion.name && version.state !== 'DESTROYED') {
                try {
                    await secretClient.destroySecretVersion({
                        name: version.name
                    });
                    console.log(`Destroyed old token version: ${version.name}`);
                } catch (destroyError) {
                    console.error(`Failed to destroy version ${version.name}:`, destroyError);
                }
            }
        }
        console.log("[NEW-BRANDS] Successfully saved tokens to New Brands Secret Manager: ", parent);
    } catch (e) {
        console.error("[NEW-BRANDS] Error saving tokens to Secret Manager: ", e);
    }
}


async function loadTokensNewBrands(brand) {
    let brandSecretName = {
        "Naruko": "projects/231801348950/secrets/naruko-shopee-tokens/versions/latest",
        "Relove": "projects/231801348950/secrets/relove-shopee-tokens/versions/latest",
        "Joey & Roo": "projects/231801348950/secrets/joey-roo-shopee-tokens/versions/latest",
        "Enchante": "projects/231801348950/secrets/enchante-shopee-tokens/versions/latest",
        "Rocketindo Shop": "projects/231801348950/secrets/rocketindoshop-shopee-tokens/versions/latest",
    }
    const secretName = brandSecretName[brand];
    console.log("SECRET NAME: ", secretName);
    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log(brand, " Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.error("[NEW-BRANDS] Error loading tokens from Secret Manager: ", e);
    }
}

async function handleNaruko() {
    let brand = "Naruko";
    let shopId = 1638001566;

    const loadedTokens = await loadTokensNewBrands(brand);
    NEW_BRANDS_ACCESS_TOKEN = loadedTokens.accessToken;
    NEW_BRANDS_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshTokenNewBrands(brand, shopId)

    await handleWalletTransactions(brand, DRJOU_PARTNER_ID, DRJOU_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId);
}

async function handleRelove() {
    let advId = "7374006579160612865";
    let brand = "Relove";
    let shopId = 1684312913;

    // const basicAds = await fetchTiktokBasicAds(brand, advId, 56000);
    // const pgmvMax = await fetchProductGMVMax(brand, advId, 58000);
    // const lgmvMax = await fetchLiveGMVMax(brand, advId, 60000);

    // await handleTiktokAdsData(basicAds, pgmvMax, lgmvMax, brand);

    // await fetchPGMVMaxBreakdown(brand, advId);
    
    const loadedTokens = await loadTokensNewBrands(brand);
    NEW_BRANDS_ACCESS_TOKEN = loadedTokens.accessToken;
    NEW_BRANDS_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshTokenNewBrands(brand, shopId);

    await handleWalletTransactions(brand, DRJOU_PARTNER_ID, DRJOU_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId);
}

async function handleJR() {
    let advId = "7431433066935091201"
    let brand = "Joey & Roo"
    let brandTT = "Joey Roo"
    let shopId = 1682176843

    // const basicAds = await fetchTiktokBasicAds(brandTT, advId, 62000);
    // const pgmvMax = await fetchProductGMVMax(brandTT, advId, 64000);
    // const lgmvMax = await fetchLiveGMVMax(brandTT, advId, 66000);

    // await handleTiktokAdsData(basicAds, pgmvMax, lgmvMax, brand);

    // await fetchPGMVMaxBreakdown(brandTT, advId);
    const loadedTokens = await loadTokensNewBrands(brand);
    NEW_BRANDS_ACCESS_TOKEN = loadedTokens.accessToken;
    NEW_BRANDS_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshTokenNewBrands(brand, shopId);

    await handleWalletTransactions(brand, DRJOU_PARTNER_ID, DRJOU_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId);
}

async function handleEnchante() {
    let advId = "7579206207240765448"
    let brand = "Enchante"
    let shopId = 1684342027

    // const basicAds = await fetchTiktokBasicAds(brand, advId, 68000);
    // const pgmvMax = await fetchProductGMVMax(brand, advId, 70000);
    // const lgmvMax = await fetchLiveGMVMax(brand, advId, 72000);

    // await handleTiktokAdsData(basicAds, pgmvMax, lgmvMax, brand);

    // await fetchPGMVMaxBreakdown(brand, advId);
    
    const loadedTokens = await loadTokensNewBrands(brand);
    NEW_BRANDS_ACCESS_TOKEN = loadedTokens.accessToken;
    NEW_BRANDS_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshTokenNewBrands(brand, shopId);

    await handleWalletTransactions(brand, DRJOU_PARTNER_ID, DRJOU_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId);
}

async function handleRocketindoShop() {
    // same thing like new brands
    console.log("Rocketindo Shop Wallet Trx");
}