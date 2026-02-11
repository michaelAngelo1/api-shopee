import { SecretManagerServiceClient } from "@google-cloud/secret-manager";
import axios from 'axios';
import crypto from 'crypto';
import { fetchAdsTotalBalance } from "../functions/fetchAdsTotalBalance.js";
import { fetchTiktokBasicAds } from "../functions/fetchTiktokBasicAds.js";
import { fetchProductGMVMax } from "../functions/fetchProductGMVMax.js";
import { fetchLiveGMVMax } from "../functions/fetchLiveGMVMax.js";
import { handleTiktokAdsData } from "../functions/handleTiktokAdsData.js";
import { fetchPGMVMaxBreakdown } from "../functions/fetchPGMVMaxBreakdown.js";
import { fetchAffiliateData } from '../functions/amsProcessor.js';
import { handleWalletTransactions } from "../functions/walletTransactions.js";
import { mainDanaDilepas } from "../functions/escrowProcessor.js";

const secretClient = new SecretManagerServiceClient();

export const PARTNER_ID = parseInt(process.env.CLEVIANT_PARTNER_ID);
export const PARTNER_KEY = process.env.CLEVIANT_PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.CLEVIANT_SHOP_ID);
const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";

export const HOST = "https://partner.shopeemobile.com";
const PATH = "/api/v2/order/get_order_list";

export let CLEV_ACCESS_TOKEN;
let CLEV_REFRESH_TOKEN;

async function refreshToken() {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: CLEV_REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: SHOP_ID
    }

    console.log("Hitting Refresh Token endpoint CLEV: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    })

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        CLEV_ACCESS_TOKEN = newAccessToken;
        CLEV_REFRESH_TOKEN = newRefreshToken;

        saveTokensToSecret({
            accessToken: CLEV_ACCESS_TOKEN,
            refreshToken: CLEV_REFRESH_TOKEN
        });
    } else {
        console.log("[CLEV] token refresh not found :(")
        throw new Error("Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/clev-shopee-tokens';
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
        
        console.log("[CLEV] Successfully saved tokens to CLEV Secret Manager: ", parent);
    } catch (e) {
        console.error("[CLEV] Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    const secretName = 'projects/231801348950/secrets/clev-shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("[CLEV] Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.error("[CLEV] Error loading tokens from Secret Manager: ", e);
    }
}

export async function fetchAndProcessOrdersCLEV() {
    console.log("[CLEV] Start fetching ads total balance. Calling the function.");
    let brand = "Cleviant";

    const loadedTokens = await loadTokensFromSecret();
    CLEV_ACCESS_TOKEN = loadedTokens.accessToken;
    CLEV_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await mainDanaDilepas(brand, PARTNER_ID, PARTNER_KEY, CLEV_ACCESS_TOKEN, SHOP_ID);
    await handleWalletTransactions(brand, PARTNER_ID, PARTNER_KEY, CLEV_ACCESS_TOKEN, SHOP_ID);
    await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, CLEV_ACCESS_TOKEN, SHOP_ID);

    await fetchAffiliateData(brand, SHOP_ID, 4000);

    let advIdClev = "7553576714043965448";
    
    // For backfill
    let advIdMirae = "7306798768821387265";

    let advertiserId = advIdClev;

    const basicAdsData = await fetchTiktokBasicAds(brand, advertiserId);
    const pgmvMaxData = await fetchProductGMVMax(brand, advertiserId);
    const lgmvMaxData = await fetchLiveGMVMax(brand, advertiserId);
    
    console.log("[CLEV] All data on: ", brand);
    console.log(basicAdsData);
    console.log(pgmvMaxData);
    console.log(lgmvMaxData);
    console.log("\n");

    await handleTiktokAdsData(basicAdsData, pgmvMaxData, lgmvMaxData, brand);

    await fetchPGMVMaxBreakdown(brand, advertiserId);
}
