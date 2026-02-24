import 'dotenv/config';
import crypto from 'crypto';
import axios from 'axios';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import { loadTokens, refreshTokens, getShopCipher } from '../auth/tiktokAuth';
const secretClient = new SecretManagerServiceClient();
let tiktokAppKey = process.env.TIKTOK_PARTNER_APP_KEY;
let tiktokAppSecret = process.env.TIKTOK_PARTNER_APP_SECRET;

async function getWithdrawals(brand, shopCipher, accessToken) {
    try {
        const appKey = tiktokAppKey
        const appSecret = tiktokAppSecret
        
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
        const appKey = tiktokAppKey
        const appSecret = tiktokAppSecret
        
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
        const appKey = tiktokAppKey
        const appSecret = tiktokAppSecret
        
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
    console.log("Shop cipher: ", shopCipher);

    // await getWithdrawals(brand, shopCipher, accessToken);
    // await getTransactionsByStatement(brand, shopCipher, accessToken);
    // await getStatements(brand, shopCipher, accessToken);
}