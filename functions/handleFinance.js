import 'dotenv/config';
import crypto from 'crypto';
import axios from 'axios';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
const secretClient = new SecretManagerServiceClient();

const tiktokSecrets = {
    "Eileen Grace": "projects/231801348950/secrets/eg-tiktok-tokens",
    "Mamaway": "projects/231801348950/secrets/mamaway-tiktok-tokens",
    "SHRD": "projects/231801348950/secrets/shrd-tiktok-tokens",
    "Miss Daisy": "projects/231801348950/secrets/md-tiktok-tokens",
    "Polynia": "projects/231801348950/secrets/polynia-tiktok-tokens",
    "Chess": ""
}

// Should check for syntax error
const brandSecrets = {
    "Eileen Grace": {
        appKey: process.env.TIKTOK_PARTNER_APP_KEY,
        appSecret: process.env.TIKTOK_PARTNER_APP_SECRET
    },
    "Mamaway": {
        appKey: process.env.MAMAWAY_APP_KEY,
        appSecret: process.env.MAMAWAY_APP_SECRET
    },
    "SHRD": {
        appKey: process.env.SHRD_APP_KEY,
        appSecret: process.env.SHRD_APP_SECRET
    },
    "Miss Daisy": {
        appKey: process.env.MD_APP_KEY,
        appSecret: process.env.MD_APP_SECRET
    }, 
    "Polynia": {
        appKey: process.env.POLY_APP_KEY,
        appSecret: process.env.POLY_APP_SECRET
    }
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
    const appKey = brandSecrets[brand].appKey;
    const appSecret = brandSecrets[brand].appSecret;

    const refreshUrl = "https://auth.tiktok-shops.com/api/v2/token/refresh";
    const queryParams = "?" + "app_key=" + appKey + "&" + "app_secret=" + appSecret + "&" + "refresh_token=" + refreshToken + "&" + "grant_type=refresh_token";
    const completeUrl = refreshUrl + queryParams;

    console.log("[TIKTOK-SECRETS] DEBUG url: ", completeUrl);

    try {   
        const response = await axios.get(completeUrl);

        let newAccessToken = response?.data?.data?.access_token;
        let newRefreshToken = response?.data?.data?.refresh_token;

        await saveTokens(brand, {
            accessToken: newAccessToken, 
            refreshToken: newRefreshToken
        });
    } catch (e) {
        console.log("[TIKTOK-SECRETS] Error refreshing tokens: ", e);
    }
}

async function getShopCipher(brand, accessToken) {
    try {
        const appKey = brandSecrets[brand].appKey;
        const appSecret = brandSecrets[brand].appSecret;
        
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

async function getWithdrawals(brand, shopCipher, accessToken) {
    try {
        const appKey = brandSecrets[brand].appKey;
        const appSecret = brandSecrets[brand].appSecret;
        
        const path = "/finance/202309/withdrawals";
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";
        const createTimeFrom = Math.floor(new Date("2026-01-01T00:00:00+07:00").getTime() / 1000);
        const createTimeTo = Math.floor(new Date("2026-01-31T23:59:59+07:00").getTime() / 1000);
        
        let keepFetching = true;
        let currPageToken = "";
        
        while(keepFetching) {
            
            const timestamp = Math.floor(Date.now() / 1000);
            const queryParams = {   
                app_key: appKey,
                create_time_ge: createTimeFrom,
                create_time_lt: createTimeTo,
                types: ["WITHDRAW", "SETTLE", "TRANSFER", "REVERSE"].join(','),
                page_size: 100,
                timestamp: timestamp,
                shop_cipher: shopCipher
            };  
            if(currPageToken) {
                queryParams.page_token = currPageToken;
            }
            const sortedKeys = Object.keys(queryParams).sort();

            let result = appSecret + path;
            for(const key of sortedKeys) {
                result += key + queryParams[key];
            }
            result += appSecret;

            const sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');
            queryParams.sign = sign;
            const querySearchParams = new URLSearchParams(queryParams);

            const completeUrl = baseUrl + querySearchParams.toString();
            const response = await axios.get(completeUrl, {
                headers: {
                    'content-type': 'application/json',
                    'x-tts-access-token': accessToken,
                }
            });

            console.log("[TIKTOK-FINANCE] Raw response: ", response.data.data);

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        }
    } catch (e) {
        console.log("[TIKTOK-FINANCE] Error getting withdrawals on brand: ", brand);
        console.log(e);
    }
}

async function getTransactionsByStatement(brand, shopCipher, accessToken) {
    try {
        const appKey = brandSecrets[brand].appKey;
        const appSecret = brandSecrets[brand].appSecret;
        
        const statementId = "7599840168392115976";
        const path = `/finance/202501/statements/${statementId}/statement_transactions`;
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";
        
        let keepFetching = true;
        let currPageToken = "";
        
        while(keepFetching) {
            
            const timestamp = Math.floor(Date.now() / 1000);
            const queryParams = {   
                app_key: appKey,
                sort_field: "order_create_time",
                sort_order: "DESC",
                page_size: 100,
                timestamp: timestamp,
                shop_cipher: shopCipher
            };  
            if(currPageToken) {
                queryParams.page_token = currPageToken;
            }
            const sortedKeys = Object.keys(queryParams).sort();

            let result = appSecret + path;
            for(const key of sortedKeys) {
                result += key + queryParams[key];
            }
            result += appSecret;

            const sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');
            queryParams.sign = sign;
            const querySearchParams = new URLSearchParams(queryParams);

            const completeUrl = baseUrl + querySearchParams.toString();
            const response = await axios.get(completeUrl, {
                headers: {
                    'content-type': 'application/json',
                    'x-tts-access-token': accessToken,
                }
            });

            console.log("[TIKTOK-FINANCE] TRX by statement response: ", response.data.data);

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        }
    } catch (e) {
        console.log("[TIKTOK-FINANCE] Error getting trx by statement on brand: ", brand);
        console.log(e);
    }
}

async function getStatements(brand, shopCipher, accessToken) {
    try {
        const appKey = brandSecrets[brand].appKey;
        const appSecret = brandSecrets[brand].appSecret;
        
        const path = "/finance/202309/statements";
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";
        const statementTimeFrom = Math.floor(new Date("2026-01-01T00:00:00+07:00").getTime() / 1000);
        const statementTimeTo = Math.floor(new Date("2026-02-02T00:00:00+07:00").getTime() / 1000);
        
        let keepFetching = true;
        let currPageToken = "";
        
        while(keepFetching) {
            
            const timestamp = Math.floor(Date.now() / 1000);
            const queryParams = {   
                app_key: appKey,
                statement_time_ge: statementTimeFrom,
                statement_time_lt: statementTimeTo,
                sort_field: "statement_time",
                sort_order: "DESC",
                page_size: 100,
                timestamp: timestamp,
                shop_cipher: shopCipher
            };  
            if(currPageToken) {
                queryParams.page_token = currPageToken;
            }
            const sortedKeys = Object.keys(queryParams).sort();

            let result = appSecret + path;
            for(const key of sortedKeys) {
                result += key + queryParams[key];
            }
            result += appSecret;

            const sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');
            queryParams.sign = sign;
            const querySearchParams = new URLSearchParams(queryParams);

            const completeUrl = baseUrl + querySearchParams.toString();
            const response = await axios.get(completeUrl, {
                headers: {
                    'content-type': 'application/json',
                    'x-tts-access-token': accessToken,
                }
            });

            console.log("[TIKTOK-FINANCE] Statements raw response: ", response.data.data);

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        }
    } catch (e) {
        console.log("[TIKTOK-FINANCE] Error getting statements on brand: ", brand);
        console.log(e);
    }
}

export async function handleFinance(brand) {

    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;

    await refreshTokens(brand, refreshToken);

    const shopCipher = await getShopCipher(brand, accessToken);

    // await getWithdrawals(brand, shopCipher, accessToken);
    // await getTransactionsByStatement(brand, shopCipher, accessToken);
    await getStatements(brand, shopCipher, accessToken);
}