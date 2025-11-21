import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios, { all } from 'axios';
import crypto from 'crypto';
import { fetchAdsTotalBalance } from '../functions/fetchAdsTotalBalance.js';
import { fetchGMVMaxSpending } from '../functions/fetchGMVMaxSpending.js';
import { fetchTiktokBasicAds } from '../functions/fetchTiktokBasicAds.js';
import { fetchProductGMVMax } from '../functions/fetchProductGMVMax.js';
import { fetchLiveGMVMax } from '../functions/fetchLiveGMVMax.js';
import { handleTiktokAdsData } from '../functions/handleTiktokAdsData.js';

const secretClient = new SecretManagerServiceClient();

export const PARTNER_ID = parseInt(process.env.SHRD_PARTNER_ID);
export const PARTNER_KEY = process.env.SHRD_PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.EVOKE_SHOP_ID);
const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";
export const HOST = "https://partner.shopeemobile.com";

export let EVOKE_ACCESS_TOKEN;
let EVOKE_REFRESH_TOKEN;

async function refreshToken() {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: EVOKE_REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: SHOP_ID
    }

    console.log("Hitting Refresh Token endpoint EVOKE: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    })

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        EVOKE_ACCESS_TOKEN = newAccessToken;
        EVOKE_REFRESH_TOKEN = newRefreshToken;

        saveTokensToSecret({
            accessToken: EVOKE_ACCESS_TOKEN,
            refreshToken: EVOKE_REFRESH_TOKEN
        });
    } else {
        console.log("[EVOKE] token refresh not found :(")
        throw new Error("Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/evoke-shopee-tokens';
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'utf-8');

    try {
        await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });
        console.log("[EVOKE] Successfully saved tokens to EVOKE Secret Manager: ", parent);
    } catch (e) {
        console.error("[EVOKE] Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    const secretName = 'projects/231801348950/secrets/evoke-shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.error("[EVOKE] Error loading tokens from Secret Manager: ", e);
    }
}

export async function fetchAndProcessOrdersEVOKE() {
    console.log("Starting fetch orders EVOKE");
    let brand = "Evoke";

    const loadedTokens = await loadTokensFromSecret();
    EVOKE_ACCESS_TOKEN = loadedTokens.accessToken;
    EVOKE_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, EVOKE_ACCESS_TOKEN, SHOP_ID);

    let advIdEvokeDrJouSwiss = "7374337917889953808"
    const basicAdsData = await fetchTiktokBasicAds(brand, advIdEvokeDrJouSwiss, 10000);
    const pgmvMaxData = await fetchProductGMVMax(brand, advIdEvokeDrJouSwiss, 11000);
    const lgmvMaxData = await fetchLiveGMVMax(brand, advIdEvokeDrJouSwiss, 12000);
    
    console.log("[EVOKE] All data on: ", brand);
    console.log(basicAdsData);
    console.log(pgmvMaxData);
    console.log(lgmvMaxData);
    console.log("\n");

    await handleTiktokAdsData(basicAdsData, pgmvMaxData, lgmvMaxData, brand);
}

