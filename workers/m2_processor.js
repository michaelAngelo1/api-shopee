import axios from 'axios';
import crypto from 'crypto';
import 'dotenv/config';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import { mainRealtime } from '../functions/handleRealtime.js';
import { mainDanaDilepas } from '../functions/escrowProcessor.js';

const secretClient = new SecretManagerServiceClient();
export const PARTNER_ID = parseInt(process.env.PARTNER_ID);
export const PARTNER_KEY = process.env.PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.M2_SHOP_ID);

export let ACCESS_TOKEN;
let REFRESH_TOKEN;
export const HOST = "https://partner.shopeemobile.com";
const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";

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
        console.log("[M2] Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/m2-shopee-tokens';
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
    const secretName = 'projects/231801348950/secrets/m2-shopee-tokens/versions/latest';

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

export async function mainM2() {
    let brand = "M2";

    const loadedTokens = await loadTokensFromSecret();
    ACCESS_TOKEN = loadedTokens.accessToken;
    REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    // await mainRealtime(brand, PARTNER_ID, PARTNER_KEY, ACCESS_TOKEN, SHOP_ID);
    mainDanaDilepas(brand, PARTNER_ID, PARTNER_KEY, ACCESS_TOKEN, SHOP_ID);
}