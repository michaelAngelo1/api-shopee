import axios from 'axios';
import crypto from 'crypto';
import 'dotenv/config';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import { mainDanaDilepas } from '../functions/escrowProcessor.js';
import { fetchAdsTotalBalance } from '../functions/fetchAdsTotalBalance.js';
import { handleWalletTransactions } from '../functions/walletTransactions.js';
import { fetchTiktokBasicAds } from '../functions/fetchTiktokBasicAds.js';
import { fetchProductGMVMax } from '../functions/fetchProductGMVMax.js';
import { fetchLiveGMVMax } from '../functions/fetchLiveGMVMax.js';
import { handleTiktokAdsData } from '../functions/handleTiktokAdsData.js';
import { fetchPGMVMaxBreakdown } from '../functions/fetchPGMVMaxBreakdown.js';
import { fetchAffiliateData } from '../functions/amsProcessor.js';

const secretClient = new SecretManagerServiceClient();
export const DRJOU_PARTNER_ID = parseInt(process.env.DRJOU_PARTNER_ID);
export const DRJOU_PARTNER_KEY = process.env.DRJOU_PARTNER_KEY;
const NEW_BRANDS_REFRESH_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";
let NEW_BRANDS_ACCESS_TOKEN, NEW_BRANDS_REFRESH_TOKEN;

const MOSS_PARTNER_ID = parseInt(process.env.MOSS_PARTNER_ID);
const MOSS_PARTNER_KEY = process.env.MOSS_PARTNER_KEY;
const SHRD_PARTNER_ID = parseInt(process.env.SHRD_PARTNER_ID);
const SHRD_PARTNER_KEY = process.env.SHRD_PARTNER_KEY;

async function refreshTokenNewBrands(brand, shop_id) {
    console.log("Refreshing token for brand: ", brand);

    let partnerId = DRJOU_PARTNER_ID;
    let partnerKey = DRJOU_PARTNER_KEY;
    
    if(brand == "Naruko") {
        partnerId = MOSS_PARTNER_ID;
        partnerKey = MOSS_PARTNER_KEY;
    }
    if(brand == "Relove") {
        partnerId = SHRD_PARTNER_ID;
        partnerKey = SHRD_PARTNER_KEY;
    }

    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${partnerId}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', partnerKey)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${NEW_BRANDS_REFRESH_URL}?partner_id=${partnerId}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: NEW_BRANDS_REFRESH_TOKEN,
        partner_id: partnerId,
        shop_id: shop_id
    }

    console.log("Hitting Refresh Token endpoint New Brands: ", fullUrl);

    try {
        const response = await axios.post(fullUrl, body, {
            headers: {
                'Content-Type': 'application/json'
            }
        })
    
        const newAccessToken = response.data.access_token;
        const newRefreshToken = response.data.refresh_token;
    
        if(newAccessToken && newRefreshToken) {
            NEW_BRANDS_ACCESS_TOKEN = newAccessToken;
            NEW_BRANDS_REFRESH_TOKEN = newRefreshToken;
    
            await saveTokensNewBrands(brand, {
                accessToken: NEW_BRANDS_ACCESS_TOKEN,
                refreshToken: NEW_BRANDS_REFRESH_TOKEN
            });
        } else {
            console.log("[NEW-BRANDS] token refresh not found :(")
            throw new Error("NEW BRANDS Tokens dont exist");
        }
    } catch (e) {
        console.log("[NEW-BRANDS] Error refreshing new brands token: ", e);
    }
}

let brandSecret = {
    "Naruko": "projects/231801348950/secrets/naruko-shopee-tokens",
    "Relove": "projects/231801348950/secrets/relove-shopee-tokens",
    "Joey & Roo": "projects/231801348950/secrets/joey-roo-shopee-tokens",
    "Enchante": "projects/231801348950/secrets/enchante-shopee-tokens",
    "Rocketindo Shop": "projects/231801348950/secrets/rocketindoshop-shopee-tokens",
}

async function saveTokensNewBrands(brand, tokens) {
    let parent = brandSecret[brand];
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'utf-8');
    try {
        const [newVersion] = await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });

        console.log("[NEW-BRANDS] Saved Shopee tokens to Secret Manager");

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
        console.log("[NEW-BRANDS] Successfully saved tokens to New Brands Secret Manager: ", parent);
    } catch (e) {
        console.error("[NEW-BRANDS] Error saving tokens to Secret Manager: ", e);
    }
}


async function loadTokensNewBrands(brand) {
    let brandSecretName = {
        "Naruko": "projects/231801348950/secrets/naruko-shopee-tokens/versions/latest",
        "Relove": "projects/231801348950/secrets/relove-shopee-tokens/versions/latest",
        "Joey & Roo": "projects/231801348950/secrets/joey-roo-shopee-tokens/versions/latest",
        "Enchante": "projects/231801348950/secrets/enchante-shopee-tokens/versions/latest",
        "Rocketindo Shop": "projects/231801348950/secrets/rocketindoshop-shopee-tokens/versions/latest",
    }
    const secretName = brandSecretName[brand];
    console.log("SECRET NAME: ", secretName);
    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log(brand, " Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.error("[NEW-BRANDS] Error loading tokens from Secret Manager: ", e);
    }
}

async function handleNaruko() {
    let brandNaruko = "Naruko";
    let brand = "Naruko";
    
    let shopId = 1638001566;

    let advIdNaruko = "7392579089489608720"
    const basicAdsDataNaruko = await fetchTiktokBasicAds(brandNaruko, advIdNaruko, 19000);
    const pgmvMaxDataNaruko = await fetchProductGMVMax(brandNaruko, advIdNaruko, 20000);
    const lgmvMaxDataNaruko = await fetchLiveGMVMax(brandNaruko, advIdNaruko, 21000);
    
    console.log("[NARUKO] All data on: ", brandNaruko);
    console.log(basicAdsDataNaruko);
    console.log(pgmvMaxDataNaruko);
    console.log(lgmvMaxDataNaruko);
    console.log("\n");

    await handleTiktokAdsData(basicAdsDataNaruko, pgmvMaxDataNaruko, lgmvMaxDataNaruko, brandNaruko);
    await fetchPGMVMaxBreakdown(brandNaruko, advIdNaruko);

    const loadedTokens = await loadTokensNewBrands(brand);
    NEW_BRANDS_ACCESS_TOKEN = loadedTokens.accessToken;
    NEW_BRANDS_REFRESH_TOKEN = loadedTokens.refreshToken;

    // await refreshTokenNewBrands(brand, shopId)

    await fetchAffiliateData(brand, shopId); // ams processor
    await mainDanaDilepas(brand, MOSS_PARTNER_ID, MOSS_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId);
    await fetchAdsTotalBalance(brand, MOSS_PARTNER_ID, MOSS_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId)
    await handleWalletTransactions(brand, MOSS_PARTNER_ID, MOSS_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId);
}

async function handleRelove() {
    let advId = "7374006579160612865";
    let brand = "Relove";
    let shopId = 1684312913;

    const basicAds = await fetchTiktokBasicAds(brand, advId, 56000);
    const pgmvMax = await fetchProductGMVMax(brand, advId, 58000);
    const lgmvMax = await fetchLiveGMVMax(brand, advId, 60000);

    await handleTiktokAdsData(basicAds, pgmvMax, lgmvMax, brand);
    await fetchPGMVMaxBreakdown(brand, advId);
    
    const loadedTokens = await loadTokensNewBrands(brand);
    NEW_BRANDS_ACCESS_TOKEN = loadedTokens.accessToken;
    NEW_BRANDS_REFRESH_TOKEN = loadedTokens.refreshToken;

    // await refreshTokenNewBrands(brand, shopId);

    await fetchAffiliateData(brand, shopId);
    await mainDanaDilepas(brand, SHRD_PARTNER_ID, SHRD_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId);
    await fetchAdsTotalBalance(brand, SHRD_PARTNER_ID, SHRD_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId)
    await handleWalletTransactions(brand, SHRD_PARTNER_ID, SHRD_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId);
}

async function handleJR() {
    let advId = "7431433066935091201"
    let brand = "Joey & Roo"
    let brandTT = "Joey Roo"
    let shopId = 1682176843

    const basicAds = await fetchTiktokBasicAds(brandTT, advId, 62000);
    const pgmvMax = await fetchProductGMVMax(brandTT, advId, 64000);
    const lgmvMax = await fetchLiveGMVMax(brandTT, advId, 66000);

    await handleTiktokAdsData(basicAds, pgmvMax, lgmvMax, brand);
    await fetchPGMVMaxBreakdown(brandTT, advId);
    
    const loadedTokens = await loadTokensNewBrands(brand);
    NEW_BRANDS_ACCESS_TOKEN = loadedTokens.accessToken;
    NEW_BRANDS_REFRESH_TOKEN = loadedTokens.refreshToken;
    
    // await refreshTokenNewBrands(brand, shopId);
    
    await fetchAffiliateData(brand, shopId);
    await mainDanaDilepas(brand, DRJOU_PARTNER_ID, DRJOU_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId);
    await fetchAdsTotalBalance(brand, DRJOU_PARTNER_ID, DRJOU_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId)
    await handleWalletTransactions(brand, DRJOU_PARTNER_ID, DRJOU_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId);
}

async function handleRocketindoShop() {
    let brand = "Rocketindo Shop";
    let shopId = 375791385;

    // Rocketindo Shop
    let advIdRshop = "7581835025746771976";
    let brandRshop = "Rocketindo Shop"
    
    const basicAdsDataRshop = await fetchTiktokBasicAds(brandRshop, advIdRshop, 50000);
    const pgmvMaxDataRshop = await fetchProductGMVMax(brandRshop, advIdRshop, 52000);
    const lgmvMaxDataRshop = await fetchLiveGMVMax(brandRshop, advIdRshop, 54000);

    await handleTiktokAdsData(basicAdsDataRshop, pgmvMaxDataRshop, lgmvMaxDataRshop, brandRshop);
    await fetchPGMVMaxBreakdown(brandRshop, advIdRshop)

    const loadedTokens = await loadTokensNewBrands(brand);
    NEW_BRANDS_ACCESS_TOKEN = loadedTokens.accessToken;
    NEW_BRANDS_REFRESH_TOKEN = loadedTokens.refreshToken;

    // await refreshTokenNewBrands(brand, shopId)

    await mainDanaDilepas(brand, DRJOU_PARTNER_ID, DRJOU_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId);
    await fetchAdsTotalBalance(brand, DRJOU_PARTNER_ID, DRJOU_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId)
    await handleWalletTransactions(brand, DRJOU_PARTNER_ID, DRJOU_PARTNER_KEY, NEW_BRANDS_ACCESS_TOKEN, shopId);
}

export async function fetchNewBrands() {
    console.log("Start handling new brands");
    await handleNaruko();
    await handleRelove();
    await handleJR();
    await handleRocketindoShop();
    console.log("End handling new brands");
}