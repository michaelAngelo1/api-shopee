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
import { fetchAffiliateData } from '../functions/amsProcessor.js';

const secretClient = new SecretManagerServiceClient();

export const PARTNER_ID = parseInt(process.env.PN_PARTNER_ID);
export const PARTNER_KEY = process.env.PN_PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.NB_SHOP_ID);
const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";
export const HOST = "https://partner.shopeemobile.com";

export let NB_ACCESS_TOKEN;
let NB_REFRESH_TOKEN;

async function refreshToken() {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: NB_REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: SHOP_ID
    }

    console.log("Hitting Refresh Token endpoint NB: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    })

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        NB_ACCESS_TOKEN = newAccessToken;
        NB_REFRESH_TOKEN = newRefreshToken;

        saveTokensToSecret({
            accessToken: NB_ACCESS_TOKEN,
            refreshToken: NB_REFRESH_TOKEN
        });
    } else {
        console.log("[NB] token refresh not found :(")
        throw new Error("Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/nb-shopee-tokens';
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
        console.log("[NB] Successfully saved tokens to NB Secret Manager: ", parent);
    } catch (e) {
        console.error("[NB] Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    const secretName = 'projects/231801348950/secrets/nb-shopee-tokens/versions/latest';

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

export async function fetchAndProcessOrdersNB() {
    console.log("Starting fetch orders NB");
    let brand = "Nutri & Beyond";
    let brandTT = "Nutri Beyond";

    const loadedTokens = await loadTokensFromSecret();
    NB_ACCESS_TOKEN = loadedTokens.accessToken;
    NB_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, NB_ACCESS_TOKEN, SHOP_ID);

    await fetchAffiliateData(brand, SHOP_ID, 8500);

    let advIdNutriBeyond = "7457040121955729425";
    // For backfilling
    let advIdMamaway = "7306800699382251521";

    let advertiserId = advIdNutriBeyond;

    const basicAdsData = await fetchTiktokBasicAds(brandTT, advertiserId);
    const pgmvMaxData = await fetchProductGMVMax(brandTT, advertiserId);
    const lgmvMaxData = await fetchLiveGMVMax(brandTT, advertiserId);
    
    console.log("[NB] All data on: ", brand);
    console.log(basicAdsData);
    console.log(pgmvMaxData);
    console.log(lgmvMaxData);
    console.log("\n");

    await handleTiktokAdsData(basicAdsData, pgmvMaxData, lgmvMaxData, brand);

    // For backfilling
    await fetchPGMVMaxBreakdown(brandTT, advertiserId);
}
