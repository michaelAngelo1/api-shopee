import 'dotenv/config';
import crypto from 'crypto';
import axios from 'axios';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
const secretClient = new SecretManagerServiceClient();

const secondTransactionBrands = [
    "Mirae",
    "Swissvita",
    "G-Belle",
    "Past Nine",
    "Nutri & Beyond",
    "Ivy & Lily",
    "Naruko",
    "Relove",
    "Joey & Roo",
    "Rocketindo Shop"
]

const tiktokSecrets = {
    "Eileen Grace": "projects/231801348950/secrets/eg-tiktok-transaction",
    "Mamaway": "projects/231801348950/secrets/mamaway-tiktok-transaction",
    "SHRD": "projects/231801348950/secrets/shrd-tiktok-transaction",
    "Miss Daisy": "projects/231801348950/secrets/md-tiktok-transaction",
    "Polynia": "projects/231801348950/secrets/polynia-tiktok-transaction",
    "CHESS": "projects/231801348950/secrets/chess-tiktok-transaction",
    "Cléviant": "projects/231801348950/secrets/cleviant-tiktok-transaction",
    "Mossèru": "projects/231801348950/secrets/mosseru-tiktok-transaction",
    "Evoke": "projects/231801348950/secrets/evoke-tiktok-transaction",
    "Dr Jou": "projects/231801348950/secrets/drjou-tiktok-transaction",
    "Mirae": "projects/231801348950/secrets/mirae-tiktok-transaction",
    "Swissvita": "projects/231801348950/secrets/swissvita-tiktok-transaction",
    "G-Belle": "projects/231801348950/secrets/gbelle-tiktok-transaction",
    "Past Nine": "projects/231801348950/secrets/pn-tiktok-transaction",
    "Nutri & Beyond": "projects/231801348950/secrets/nb-tiktok-transaction",
    "Ivy & Lily": "projects/231801348950/secrets/il-tiktok-transaction",
    "Naruko": "projects/231801348950/secrets/naruko-tiktok-transaction",
    "Relove": "projects/231801348950/secrets/relove-tiktok-transaction",
    "Joey & Roo": "projects/231801348950/secrets/joey-roo-tiktok-transaction",
    "Rocketindo Shop": "projects/231801348950/secrets/rocketindo-shop-tiktok-transaction"
}

export async function loadTokens(brand) {
    const secretName = tiktokSecrets[brand] + "/versions/latest";
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
    const parent = tiktokSecrets[brand];
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'UTF-8');

    try {
        const [newTokens] = await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });

        console.log("Saved Tiktok Tokens to Secret Manager on brand: ", brand);

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
    let appKey;
    let appSecret;

    if(!secondTransactionBrands.includes(brand)) {
        appKey = "6jalpvras8n00"
        appSecret = "608e7a9c85afc968d0baa47f6f93258d3ab51949"
    } else {
        appKey = "6jbdera7i5q9b";
        appSecret = "7955bd57fa190f1d6ea27514f45b884192e474f0";
    }

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
        let appKey;
        let appSecret;

        if(!secondTransactionBrands.includes(brand)) {
            appKey = "6jalpvras8n00"
            appSecret = "608e7a9c85afc968d0baa47f6f93258d3ab51949"
        } else {
            appKey = "6jbdera7i5q9b";
            appSecret = "7955bd57fa190f1d6ea27514f45b884192e474f0";
        }
        
        const timestamp = Math.floor(Date.now() / 1000);
        const queryParams = "app_key" + appKey + "timestamp" + timestamp;
        const path = "/authorization/202309/shops" // If fail, append "/"
        const result = appSecret + path + queryParams + appSecret;
        const sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');

        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?"
        const completeUrl = baseUrl + "app_key=" + appKey + "&" + "sign=" + sign + "&" + "timestamp=" + timestamp; 
        
        console.log("Hitting get shop cipher for brand: ", brand);
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