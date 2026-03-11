import 'dotenv/config';
import crypto from 'crypto';
import axios from 'axios';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
const secretClient = new SecretManagerServiceClient(); 

const affiliateAppBrands = {
    "Eileen Grace": 1,
    "Mamaway": 1,
    "SHRD": 1,
    "Miss Daisy": 1,
    "CHESS": 1,
    "Polynia": 1,
    "CHESS": 1,
    "Cléviant": 1,
    "Mossèru": 1,
    "Evoke": 1,
    "Dr Jou": 1,
    "Mirae": 2,
    "Swissvita": 2,
    "G-Belle": 2,
    "Past Nine": 2,
    "Nutri & Beyond": 2,
    "Ivy & Lily": 2,
    "Naruko": 2,
    "Relove": 2,
    "Joey & Roo": 2, 
    "Rocketindo Shop": 2,
    "M2": 3,
}

const tiktokAffiliateSecrets = {
    "Eileen Grace": "projects/231801348950/secrets/eg-tiktok-affiliate-tokens",
    "Mamaway": "projects/231801348950/secrets/mamaway-tiktok-affiliate-tokens",
    "SHRD": "projects/231801348950/secrets/shrd-tiktok-affiliate-tokens",
    "Miss Daisy": "projects/231801348950/secrets/md-tiktok-affiliate-tokens",
    "Polynia": "projects/231801348950/secrets/poly-tiktok-affiliate-tokens",
    "CHESS": "projects/231801348950/secrets/chess-tiktok-affiliate-tokens",
    "Cléviant": "projects/231801348950/secrets/cleviant-tiktok-affiliate-tokens",
    "Mossèru": "projects/231801348950/secrets/moss-tiktok-affiliate-tokens",
    "Evoke": "projects/231801348950/secrets/evoke-tiktok-affiliate-tokens",
    "Dr Jou": "projects/231801348950/secrets/drjou-tiktok-affiliate-tokens",
    "Mirae": "projects/231801348950/secrets/mirae-tiktok-affiliate-tokens",
    "Swissvita": "projects/231801348950/secrets/sv-tiktok-affiliate-tokens",
    "G-Belle": "projects/231801348950/secrets/gb-affiliate-tiktok-tokens",
    "Past Nine": "projects/231801348950/secrets/pn-tiktok-affiliate-tokens",
    "Nutri & Beyond": "projects/231801348950/secrets/nb-tiktok-affiliate-tokens",
    "Ivy & Lily": "projects/231801348950/secrets/il-tiktok-affiliate-tokens",
    "Naruko": "projects/231801348950/secrets/naruko-tiktok-affiliate-tokens",
    "Relove": "projects/231801348950/secrets/relove-tiktok-affiliate-tokens",
    "Joey & Roo": "projects/231801348950/secrets/jr-tiktok-affiliate-tokens",
    "Rocketindo Shop": "projects/231801348950/secrets/rshop-tiktok-affiliate-tokens",
    "M2": "projects/231801348950/secrets/m2-tiktok-tokens"
}

export async function loadTokens(brand) {
    const secretName = tiktokAffiliateSecrets[brand] + "/versions/latest";
    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("[TIKTOK-SECRETS] Tokens loaded: ", tokens);
        return tokens;
    } catch (e) {
        console.log("[TIKTOK-SECRETS] Error loading tokens for brand: ", brand);
        console.log(e);
    }
}

export async function saveTokens(brand, tokens) {
    const parent = tiktokAffiliateSecrets[brand];
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'UTF-8');

    try {
        const [newTokens] = await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });

        console.log("[TIKTOK-AFFILIATE] Saved Tiktok Tokens to Secret Manager on brand: ", brand);

        const [prevTokens] = await secretClient.listSecretVersions({
            parent: parent
        });
        
        for(const prevToken of prevTokens) {
            if(prevToken.name !== newTokens.name && prevToken.state !== 'DESTROYED') {
                try {
                    await secretClient.destroySecretVersion({
                        name: prevToken.name
                    })
                } catch (destroyError) {
                    console.error(`[TIKTOK-SECRETS] Failed to destroy version ${version.name}:`, destroyError);
                }
            }
        }
    } catch (e) {
        console.log("[TIKTOK-SECRETS] Error saving tokens to Secret Manager: ", e);
    }
}

// Tokens are exclusive per shop
// Refresh token itself contains identity of the corresponding shop
// Such is why it does not need shop_cipher or any other parameters. 

export async function refreshTokens(brand, refreshToken) {
    let tiktokAppKey;
    let tiktokAppSecret;

    if(affiliateAppBrands[brand] == 1) {
        tiktokAppKey = "6j7bl3bsi59jh"
        tiktokAppSecret = "8e7cc952feb703b4ef22fce29c85721c4e98d443"
    } else if(affiliateAppBrands[brand] == 2) {
        tiktokAppKey = "6j7q24v1la9la"
        tiktokAppSecret = "8ea3e5fe98de48c2f83d6e20321010e7a79fd15a"
    } else {
        tiktokAppKey = "6jbrll2ed26dp";
        tiktokAppSecret = "04679ae180556cdc79b11a3e7cbd8da33f0d6e92";
    }

    let appKey = tiktokAppKey;
    let appSecret = tiktokAppSecret;

    const refreshUrl = "https://auth.tiktok-shops.com/api/v2/token/refresh";
    const queryParams = "?" + "app_key=" + appKey + "&" + "app_secret=" + appSecret + "&" + "refresh_token=" + refreshToken + "&" + "grant_type=refresh_token";
    const completeUrl = refreshUrl + queryParams;

    console.log("[TIKTOK-SECRETS] DEBUG url: ", completeUrl);

    try {   
        const response = await axios.get(completeUrl);

        let newAccessToken = response?.data?.data?.access_token;
        let newRefreshToken = response?.data?.data?.refresh_token;

        if(newAccessToken && newRefreshToken) {
            await saveTokens(brand, {
                accessToken: newAccessToken, 
                refreshToken: newRefreshToken
            });
        } else {
            console.log("[TIKTOK-SECRETS] New tokens dont exist");
        }
    } catch (e) {
        console.log("[TIKTOK-SECRETS] Error refreshing tokens: ", e);
    }
}

export async function getShopCipher(brand, accessToken) {
    try {
        let tiktokAppKey;
        let tiktokAppSecret;

        if(affiliateAppBrands[brand] == 1) {
            tiktokAppKey = "6j7bl3bsi59jh"
            tiktokAppSecret = "8e7cc952feb703b4ef22fce29c85721c4e98d443"
        } else if(affiliateAppBrands[brand] == 2) {
            tiktokAppKey = "6j7q24v1la9la"
            tiktokAppSecret = "8ea3e5fe98de48c2f83d6e20321010e7a79fd15a"
        } else {
            tiktokAppKey = "6jbrll2ed26dp";
            tiktokAppSecret = "04679ae180556cdc79b11a3e7cbd8da33f0d6e92";
        }

        let appKey = tiktokAppKey;
        let appSecret = tiktokAppSecret;

        const timestamp = Math.floor(Date.now() / 1000);
        const queryParams = "app_key" + appKey + "timestamp" + timestamp;
        const path = "/authorization/202309/shops" // If fail, append "/"
        const result = appSecret + path + queryParams + appSecret;
        const sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');

        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?"
        const completeUrl = baseUrl + "app_key=" + appKey + "&" + "sign=" + sign + "&" + "timestamp=" + timestamp; 
        
        console.log("[TIKTOK-AFFILIATE] Hitting get shop cipher for brand: ", brand);
        console.log("Complete url: ", completeUrl);

        const headers = {
            'content-type': 'application/json',
            'x-tts-access-token': accessToken,
        }
        const params = {
            app_key: appKey,
            sign: sign,
            timestamp: timestamp
        }

        const response = await axios.get(completeUrl, {
            headers: headers
        });
        // console.log("[TIKTOK-FINANCE] Raw response: ", response.data.data);

        let authorizedShops = response.data.data.shops;
        let shopCipher = "";
        for(const shop of authorizedShops) {
            console.log("Shop name: ", shop.name);
            if(shop.name.toLowerCase().includes(brand.toLowerCase())) {
                shopCipher = shop.cipher;
            } 
        }

        return shopCipher;

    } catch (e) {
        console.log("Error get shop cipher on brand: ", brand)
        console.log(e);
    }
}