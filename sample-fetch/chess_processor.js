import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios, { all } from 'axios';
import crypto from 'crypto';
import { fetchAdsTotalBalance } from '../functions/fetchAdsTotalBalance.js';
import { fetchGMVMaxSpending } from '../functions/fetchGMVMaxSpending.js';
import { fetchTiktokBasicAds } from '../functions/fetchTiktokBasicAds.js';
import { fetchProductGMVMax } from '../functions/fetchProductGMVMax.js';
import { fetchLiveGMVMax } from '../functions/fetchLiveGMVMax.js';

const secretClient = new SecretManagerServiceClient();

export const PARTNER_ID = parseInt(process.env.CLEVIANT_PARTNER_ID);
export const PARTNER_KEY = process.env.CLEVIANT_PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.CHESS_SHOP_ID);
const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";
export const HOST = "https://partner.shopeemobile.com";

export let CHESS_ACCESS_TOKEN;
let CHESS_REFRESH_TOKEN;

async function refreshToken() {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: CHESS_REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: SHOP_ID
    }

    console.log("Hitting Refresh Token endpoint CHESS: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    })

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        CHESS_ACCESS_TOKEN = newAccessToken;
        CHESS_REFRESH_TOKEN = newRefreshToken;

        saveTokensToSecret({
            accessToken: CHESS_ACCESS_TOKEN,
            refreshToken: CHESS_REFRESH_TOKEN
        });
    } else {
        console.log("[CHESS] token refresh not found :(")
        throw new Error("Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/chess-shopee-tokens';
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'utf-8');

    try {
        await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });

        // const [allVersions] = await secretClient.listSecretVersions({
        //     parent: parent,
        // });

        // // Disable all past versions
        // for (const version of allVersions) {
        //     if (version.name !== newVersion.name && version.state === 'ENABLED') {
        //         try {
        //             await secretClient.disableSecretVersion({
        //                 name: version.name,
        //             });
        //             console.log(`Successfully disabled old version: ${version.name}`);
        //         } catch (disableError) {
        //             console.error(`Error disabling version ${version.name}:`, disableError);
        //         }
        //     }
        // }
        console.log("[CHESS] Successfully saved tokens to CHESS Secret Manager: ", parent);
    } catch (e) {
        console.error("[CHESS] Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    const secretName = 'projects/231801348950/secrets/chess-shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.error("[CHESS] Error loading tokens from Secret Manager: ", e);
    }
}

export async function fetchAndProcessOrdersCHESS() {
    console.log("Starting fetch orders CHESS");
    let brand = "Chess";

    const loadedTokens = await loadTokensFromSecret();
    CHESS_ACCESS_TOKEN = loadedTokens.accessToken;
    CHESS_REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await fetchAdsTotalBalance(brand, PARTNER_ID, PARTNER_KEY, CHESS_ACCESS_TOKEN, SHOP_ID);

    let advIdMMWCHESSNB = "7306800699382251521";

    const basicAdsData = await fetchTiktokBasicAds(brand, advIdMMWCHESSNB);

    const pgmvMaxData = await fetchProductGMVMax(brand, advIdMMWCHESSNB);

    const lgmvMaxData = await fetchLiveGMVMax(brand, advIdMMWCHESSNB);

    console.log("[CHESS] All data on: ", brand);
    console.log(basicAdsData);
    console.log(pgmvMaxData);
    console.log(lgmvMaxData);
    console.log("\n");
}
