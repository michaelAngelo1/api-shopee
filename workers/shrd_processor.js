import { SecretManagerServiceClient } from "@google-cloud/secret-manager";
import { getEndOfPreviousMonthTimestampWIB, getEndOfYesterdayTimestampWIB, getStartOfMonthTimestampWIB, getStartOfPreviousMonthTimestampWIB } from '../processor.js';
import axios from 'axios';
import crypto from "crypto";
import { fetchAdsTotalBalance } from "../functions/fetchAdsTotalBalance.js";
import { fetchGMVMaxSpending } from "../functions/fetchGMVMaxSpending.js";
import { fetchTiktokBasicAds } from "../functions/fetchTiktokBasicAds.js";
import { fetchProductGMVMax } from "../functions/fetchProductGMVMax.js";
import { fetchLiveGMVMax } from "../functions/fetchLiveGMVMax.js";
import { handleTiktokAdsData } from "../functions/handleTiktokAdsData.js";
import { fetchPGMVMaxBreakdown } from "../functions/fetchPGMVMaxBreakdown.js";
import { fetchAffiliateData } from '../functions/amsProcessor.js';
import { handleWalletTransactions } from "../functions/walletTransactions.js";
import { handleFinance } from "../functions/handleFinance.js";
import { mainDanaDilepas } from "../functions/escrowProcessor.js";

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

export async function fetchAndProcessOrdersSHRD() {
    console.log("[SH-RD] Start fetching ads total balance. Calling the function.");
    let brand = "SH-RD";
    let brandTT = "SHRD";

    const loadedTokens = await loadTokensFromSecret();
    SHRD_ACCESS_TOKEN = loadedTokens.accessToken;
    SHRD_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await mainDanaDilepas(brand, PARTNER_ID, PARTNER_KEY, SHRD_ACCESS_TOKEN, SHOP_ID);
    // await handleFinance(brandTT);
    // await handleWalletTransactions(brand, PARTNER_ID, PARTNER_KEY, SHRD_ACCESS_TOKEN, SHOP_ID)

    // await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, SHRD_ACCESS_TOKEN, SHOP_ID);

    // await fetchAffiliateData(brand, SHOP_ID, 2000);

    // let adsIdSHRD = "7377330420947632145";
    // const basicAdsData = await fetchTiktokBasicAds(brandTT, adsIdSHRD);
    // const pgmvMaxData = await fetchProductGMVMax(brandTT, adsIdSHRD);
    // const lgmvMaxData = await fetchLiveGMVMax(brandTT, adsIdSHRD);
    
    // console.log("[SHRD] All data on: ", brand);
    // console.log(basicAdsData);
    // console.log(pgmvMaxData);
    // console.log(lgmvMaxData);
    // console.log("\n");

    // await handleTiktokAdsData(basicAdsData, pgmvMaxData, lgmvMaxData, brandTT);

    // await fetchPGMVMaxBreakdown(brandTT, adsIdSHRD);
}
