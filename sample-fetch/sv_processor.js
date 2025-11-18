import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios, { all } from 'axios';
import crypto from 'crypto';
import { fetchAdsTotalBalance } from '../functions/fetchAdsTotalBalance.js';
import { fetchGMVMaxSpending } from '../functions/fetchGMVMaxSpending.js';
import { fetchTiktokBasicAds } from '../functions/fetchTiktokBasicAds.js';

const secretClient = new SecretManagerServiceClient();

export const PARTNER_ID = parseInt(process.env.SV_PARTNER_ID);
export const PARTNER_KEY = process.env.SV_PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.SV_SHOP_ID);
const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";
export const HOST = "https://partner.shopeemobile.com";

export let SV_ACCESS_TOKEN;
let SV_REFRESH_TOKEN;

async function refreshToken() {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: SV_REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: SHOP_ID
    }

    console.log("Hitting Refresh Token endpoint SV: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    })

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        SV_ACCESS_TOKEN = newAccessToken;
        SV_REFRESH_TOKEN = newRefreshToken;

        saveTokensToSecret({
            accessToken: SV_ACCESS_TOKEN,
            refreshToken: SV_REFRESH_TOKEN
        });
    } else {
        console.log("[SV] Token refresh not found :(")
        throw new Error("Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/sv-shopee-tokens';
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'utf-8');

    try {
        await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });
        console.log("[SV] Successfully saved tokens to SV Secret Manager: ", parent);
    } catch (e) {
        console.error("[SV] Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    const secretName = 'projects/231801348950/secrets/sv-shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.error("[SV] Error loading tokens from Secret Manager: ", e);
    }
}

export async function fetchAndProcessOrdersSV() {
    console.log("Starting fetch orders SV");
    let brand = "Swissvita";

    const loadedTokens = await loadTokensFromSecret();
    SV_ACCESS_TOKEN = loadedTokens.accessToken;
    SV_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, SV_ACCESS_TOKEN, SHOP_ID);

    let advIdEvokeDrJouSwiss = "7374337917889953808"
    await fetchGMVMaxSpending(brand, advIdEvokeDrJouSwiss);

    await fetchTiktokBasicAds(brand, advIdEvokeDrJouSwiss);
}
