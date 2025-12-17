import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios from 'axios';
import crypto from 'crypto';

export const PARTNER_ID = parseInt(process.env.AMS_PARTNER_ID);
export const PARTNER_KEY = process.env.AMS_PARTNER_KEY;

const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";
export const HOST = "https://partner.shopeemobile.com";
const secretClient = new SecretManagerServiceClient();
let AMS_ACCESS_TOKEN, AMS_REFRESH_TOKEN;

async function refreshToken(shop_id) {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: AMS_REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: shop_id
    }

    console.log("Hitting Refresh Token endpoint AMS: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    })

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        AMS_ACCESS_TOKEN = newAccessToken;
        AMS_REFRESH_TOKEN = newRefreshToken;

        saveTokensToSecret({
            accessToken: AMS_ACCESS_TOKEN,
            refreshToken: AMS_REFRESH_TOKEN
        });
    } else {
        console.log("[AMS] token refresh not found :(")
        throw new Error("AMS Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/ams-shopee-tokens';
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'utf-8');

    try {
        const [newVersion] = await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });

        console.log("[AMS] Saved Shopee tokens to Secret Manager");

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
        console.log("[AMS] Successfully saved tokens to AMS Secret Manager: ", parent);
    } catch (e) {
        console.error("[AMS] Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    const secretName = 'projects/231801348950/secrets/ams-shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.error("[NB] Error loading tokens from Secret Manager: ", e);
    }
}

export async function fetchAffiliateData(brand, shop_id) {
    
    console.log('Fetch affiliate data on brand: ', brand);

    const loadedTokens = await loadTokensFromSecret();
    AMS_ACCESS_TOKEN = loadedTokens.accessToken;
    AMS_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken(shop_id);

    // Fetch affiliate data per shop_id
    
}