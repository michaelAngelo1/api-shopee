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

export const PARTNER_ID = parseInt(process.env.PARTNER_ID);
export const PARTNER_KEY = process.env.PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.IL_SHOP_ID);
const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";
export const HOST = "https://partner.shopeemobile.com";

export let IL_ACCESS_TOKEN;
let IL_REFRESH_TOKEN;

async function refreshToken() {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: IL_REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: SHOP_ID
    }

    console.log("Hitting Refresh Token endpoint IL: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    })

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        IL_ACCESS_TOKEN = newAccessToken;
        IL_REFRESH_TOKEN = newRefreshToken;

        saveTokensToSecret({
            accessToken: IL_ACCESS_TOKEN,
            refreshToken: IL_REFRESH_TOKEN
        });
    } else {
        console.log("[IL] token refresh not found :(")
        throw new Error("Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/il-shopee-tokens';
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'utf-8');

    try {
        await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });

        console.log("[IL] Successfully saved tokens to IL Secret Manager: ", parent);
    } catch (e) {
        console.error("[IL] Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    const secretName = 'projects/231801348950/secrets/il-shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.error("[IL] Error loading tokens from Secret Manager: ", e);
    }
}

export async function fetchAndProcessOrdersIL() {
    console.log("Starting fetch orders IL");
    let brand = "Ivy & Lily";
    let brandTT = "Ivy Lily";
    let brandNaruko = "Naruko";

    const loadedTokens = await loadTokensFromSecret();
    IL_ACCESS_TOKEN = loadedTokens.accessToken;
    IL_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, IL_ACCESS_TOKEN, SHOP_ID);

    let advIdGbellePastnineIvyLilyNaruko = "7329483707528691714";
    const basicAdsData = await fetchTiktokBasicAds(brandTT, advIdGbellePastnineIvyLilyNaruko);
    const pgmvMaxData = await fetchProductGMVMax(brandTT, advIdGbellePastnineIvyLilyNaruko);
    const lgmvMaxData = await fetchLiveGMVMax(brandTT, advIdGbellePastnineIvyLilyNaruko);
    
    console.log("[IVYLILY] All data on: ", brand);
    console.log(basicAdsData);
    console.log(pgmvMaxData);
    console.log(lgmvMaxData);
    console.log("\n");

    const basicAdsDataNaruko = await fetchTiktokBasicAds(brandNaruko, advIdGbellePastnineIvyLilyNaruko, 19000);
    const pgmvMaxDataNaruko = await fetchProductGMVMax(brandNaruko, advIdGbellePastnineIvyLilyNaruko, 20000);
    const lgmvMaxDataNaruko = await fetchLiveGMVMax(brandNaruko, advIdGbellePastnineIvyLilyNaruko, 21000);
    
    console.log("[NARUKO] All data on: ", brandNaruko);
    console.log(basicAdsDataNaruko);
    console.log(pgmvMaxDataNaruko);
    console.log(lgmvMaxDataNaruko);
    console.log("\n");

    await handleTiktokAdsData(basicAdsData, pgmvMaxData, lgmvMaxData, brand);

    await handleTiktokAdsData(basicAdsDataNaruko, pgmvMaxDataNaruko, lgmvMaxDataNaruko, brandNaruko);
}
