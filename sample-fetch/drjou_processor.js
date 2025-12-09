import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios, { all } from 'axios';
import crypto from 'crypto';
import { fetchAdsTotalBalance } from '../functions/fetchAdsTotalBalance.js';
import { fetchGMVMaxSpending } from '../functions/fetchGMVMaxSpending.js';
import { fetchTiktokBasicAds } from '../functions/fetchTiktokBasicAds.js';
import { fetchProductGMVMax } from '../functions/fetchProductGMVMax.js';
import { fetchLiveGMVMax } from '../functions/fetchLiveGMVMax.js';
import { handleTiktokAdsData } from '../functions/handleTiktokAdsData.js';
import { fetchPGMVMaxBreakdown } from '../functions/fetchPGMVMaxBreakdown.js';

const secretClient = new SecretManagerServiceClient();

export const PARTNER_ID = parseInt(process.env.DRJOU_PARTNER_ID);
export const PARTNER_KEY = process.env.DRJOU_PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.DRJOU_SHOP_ID);
const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";
export const HOST = "https://partner.shopeemobile.com";

export let DRJOU_ACCESS_TOKEN;
let DRJOU_REFRESH_TOKEN;

async function refreshToken() {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: DRJOU_REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: SHOP_ID
    }

    console.log("Hitting Refresh Token endpoint DRJOU: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    })

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        DRJOU_ACCESS_TOKEN = newAccessToken;
        DRJOU_REFRESH_TOKEN = newRefreshToken;

        saveTokensToSecret({
            accessToken: DRJOU_ACCESS_TOKEN,
            refreshToken: DRJOU_REFRESH_TOKEN
        });
    } else {
        console.log("[DRJOU] token refresh not found :(")
        throw new Error("Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/drjou-shopee-tokens';
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
        console.log("[DRJOU] Successfully saved tokens to DRJOU Secret Manager: ", parent);
    } catch (e) {
        console.error("[DRJOU] Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    const secretName = 'projects/231801348950/secrets/drjou-shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.error("[DRJOU] Error loading tokens from Secret Manager: ", e);
    }
}

export async function fetchAndProcessOrdersDRJOU() {
    console.log("Starting fetch orders DRJOU");
    let brand = "Dr.Jou";
    let brandTT = "Dr Jou";

    const loadedTokens = await loadTokensFromSecret();
    DRJOU_ACCESS_TOKEN = loadedTokens.accessToken;
    DRJOU_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, DRJOU_ACCESS_TOKEN, SHOP_ID);

    let advIdDrJou = "7431385339190820880"
    
    // For backfilling
    let advIdEvoke = "7374337917889953808"
    
    const basicAdsData = await fetchTiktokBasicAds(brandTT, advIdEvoke);
    const pgmvMaxData = await fetchProductGMVMax(brandTT, advIdEvoke);
    const lgmvMaxData = await fetchLiveGMVMax(brandTT, advIdEvoke);
    
    console.log("[DRJOU] All data on: ", brand);
    console.log(basicAdsData);
    console.log(pgmvMaxData);
    console.log(lgmvMaxData);
    console.log("\n");

    await handleTiktokAdsData(basicAdsData, pgmvMaxData, lgmvMaxData, brand);

    // For backfilling
    await fetchPGMVMaxBreakdown(brandTT, advIdEvoke);
}
