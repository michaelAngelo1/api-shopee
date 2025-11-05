import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios, { all } from 'axios';
import crypto from 'crypto';
import { fetchAdsTotalBalance } from '../functions/fetchAdsTotalBalance.js';

const secretClient = new SecretManagerServiceClient();

export const PARTNER_ID = parseInt(process.env.MOSS_PARTNER_ID);
export const PARTNER_KEY = process.env.MOSS_PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.MMW_SHOP_ID);
const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";
export const HOST = "https://partner.shopeemobile.com";

export let MMW_ACCESS_TOKEN;
let MMW_REFRESH_TOKEN;

async function refreshToken() {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: MMW_REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: SHOP_ID
    }

    console.log("Hitting Refresh Token endpoint MMW: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    })

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        MMW_ACCESS_TOKEN = newAccessToken;
        MMW_REFRESH_TOKEN = newRefreshToken;

        saveTokensToSecret({
            accessToken: MMW_ACCESS_TOKEN,
            refreshToken: MMW_REFRESH_TOKEN
        });
    } else {
        console.log("token refresh not found :(")
        throw new Error("Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/mmw-shopee-tokens';
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'utf-8');

    try {
        await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });
        console.log("[MMW] Successfully saved tokens to MMW Secret Manager: ", parent);
    } catch (e) {
        console.error("[MMW] Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    const secretName = 'projects/231801348950/secrets/mmw-shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.error("[MMW] Error loading tokens from Secret Manager: ", e);
        throw e;
    }
}

export async function fetchAndProcessOrdersMMW() {
    console.log("Starting fetch orders MMW");
    let brand = "Mamaway";

    const loadedTokens = await loadTokensFromSecret();
    MMW_ACCESS_TOKEN = loadedTokens.accessToken;
    MMW_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, MMW_ACCESS_TOKEN, SHOP_ID);
}
