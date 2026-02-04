import 'dotenv/config';
import crypto from 'crypto';
import axios from 'axios';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
const secretClient = new SecretManagerServiceClient();
// TODO:
// 1. Bikin function getAccessToken from Secret Manager
// 2. Bikin function refreshTokens, ambil refreshToken dari Secret Manager
// 3. Expiration: access token 7 hari, refresh token lumayan lama.
// *EG_TIKTOK_ACCESS_TOKEN remove dari env & secrets di staging-worker

// curl -X GET "https://auth.tiktok-shops.com/api/v2/token/get?app_key=6ivpj08pq3t4s&app_secret=e9212b70ae318a3704c6ed8673b138ec6cb5723b&auth_code=ROW_tVrf5wAAAAD8IPyM8KSM-yD2plONL3kO-IMXvK3MG9MPLMMg9OGJq8dn4OPP6AZsagxf6CRaIVCvnt7TTe6m6YKF_Ki4fJm4a9wyqQ3j2mleooaCdDXXwbsHaq8NoQvZt8MmwO68DZdMN39CU_dx22kmTgMq4gqh&grant_type=authorized_code"

/*
{
    "accessToken": "ROW_srIgowAAAAAoYfgyHQFZ7-j_QZLXj-NIE7tVpLQ_4mgfh2_6zs-7CJ4Q1BEsf77UI2n3YNPBFU20ry4-pHZMscMWnkELuazsobxSPhr4-OW1me6jDyICFDKKISlLPz_HnlykJj1_hrfyqhhmvsEYgJZ6mQzOP97lLtGdlGsfht8IP4N2VAm_oQ",
    "refreshToken": "ROW_zKjNPwAAAAB3gdLG6OS-uLqmlp1aPMJFqq77Pi6EZ3aGXKS9_uMHfMgLKbKMfJq_oTY8zFyJ7BA"
}
*/

const tiktokSecrets = {
    "Eileen Grace": "projects/231801348950/secrets/eg-tiktok-tokens"
}

async function loadTokens(brand) {
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

async function saveTokens(brand, tokens) {
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

async function refreshTokens(brand, refreshToken) {

    const tokens = await loadTokens(brand);
    const appKey = process.env.TIKTOK_PARTNER_APP_KEY;
    const appSecret = process.env.TIKTOK_PARTNER_APP_SECRET;

    const refreshUrl = "auth.tiktok-shops.com/api/v2/token/refresh";
    const queryParams = "?" + "app_key=" + appKey + "&" + "app_secret=" + appSecret + "&" + "refresh_token=" + refreshToken + "&" + "grant_type=refresh_token";
    const completeUrl = refreshUrl + queryParams;

    try {   
        const response = await axios.get(completeUrl);

        let newAccessToken = response.data.data.access_token;
        let newRefreshToken = response.data.data.refresh_token;

        await saveTokens({
            accessToken: newAccessToken, 
            refreshToken: newRefreshToken
        });
    } catch (e) {
        console.log("[TIKTOK-SECRETS] Error refreshing tokens: ", e);
    }
}

async function getShopCipher(brand, accessToken) {
    try {
        const appKey = process.env.TIKTOK_PARTNER_APP_KEY;
        const appSecret = process.env.TIKTOK_PARTNER_APP_SECRET;
        
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
            app_key: process.env.TIKTOK_PARTNER_APP_KEY,
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
            if(shop.name === brand) {
                shopCipher = shop.cipher;
            }
        }

        return shopCipher;

    } catch (e) {
        console.log("Error get shop cipher on brand: ", brand)
        console.log(e);
    }
}

export async function handleFinance(brand) {

    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;

    await refreshTokens(brand, refreshToken);

    const shopCipher = await getShopCipher(brand, accessToken);
    console.log("Shop cipher for brand: ", brand, ": ", shopCipher);
    // const shopCipher = await getShopCipher(brand);
}