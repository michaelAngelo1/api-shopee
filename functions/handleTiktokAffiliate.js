import 'dotenv/config';
import crypto from 'crypto';
import axios from 'axios';
import { BigQuery } from '@google-cloud/bigquery';
import { loadTokens, refreshTokens, getShopCipher } from '../auth/tiktokAuthAffiliate.js';
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

export async function handleAffiliate(brand, shopCipher, accessToken) {
    try {   
        let tiktokAppKey;
        let tiktokAppSecret;

        if(affiliateAppBrands[brand] == 1) {
            tiktokAppKey = "6j7bl3bsi59jh"
            tiktokAppSecret = "8e7cc952feb703b4ef22fce29c85721c4e98d443"
        } else if(internalAppBrands[brand] == 2) {
            tiktokAppKey = "6j7q24v1la9la"
            tiktokAppSecret = "8ea3e5fe98de48c2f83d6e20321010e7a79fd15a"
        } else {
            tiktokAppKey = "6jbrll2ed26dp";
            tiktokAppSecret = "04679ae180556cdc79b11a3e7cbd8da33f0d6e92";
        }

        let appKey = tiktokAppKey;
        let appSecret = tiktokAppSecret;

        console.log("[TIKTOK-AFFILIATE] Fetching tiktok affiliate for brand: ", brand);
        const path = "/affiliate_seller/202410/orders/search";
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";

        let keepFetching = true;
        let currPageToken = "";
        const createTimeFrom = Math.floor(new Date("2026-03-01T00:00:00+07:00").getTime() / 1000);
        // const createTimeFrom = 1767200458;
        const createTimeTo = Math.floor(new Date("2026-03-02T23:59:59+07:00").getTime() / 1000);

        let rawAffiliateOrders = [];
        let rawAffiliateOrdersLength = 0;
        
        while(keepFetching) {
            const requestBody = {
                create_time_ge: createTimeFrom,
                create_time_lt: createTimeTo
            }

            const timestamp = Math.floor(Date.now() / 1000);
            const queryParams = {   
                app_key: appKey,
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
            result += JSON.stringify(requestBody);
            result += appSecret;

            const sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');
            queryParams.sign = sign;
            const querySearchParams = new URLSearchParams(queryParams);

            const completeUrl = baseUrl + querySearchParams.toString();
            
            const response = await axios.post(completeUrl, 
                requestBody,
                {
                    headers: {
                        'content-type': 'application/json',
                        'x-tts-access-token': accessToken,
                    },
                }
            );

            // console.log("[TIKTOK-AFFILIATE] Affiliate raw response orders: ", response.data.data.orders);
            rawAffiliateOrders.push(...response.data.data.orders);
            rawAffiliateOrdersLength += response.data.data.orders.length;   

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        }

        console.log("Affiliate orders qty: ", rawAffiliateOrdersLength);
        // console.log("Affiliate Orders. First: ");
        // console.log(rawAffiliateOrders[0]);

        // for(const order of rawAffiliateOrders) {
        //     if(order.id === "582125212408513896") {
        //         const date = new Date(order.create_time * 1000);
        //         const utc7Date = new Date(date.getTime() + (7 * 60 * 60 * 1000)); 
        //         const isoString = utc7Date.toISOString();
        //         const result = isoString.replace('T', ' ').substring(0, 19);
        //         order.skus.forEach(sku => {
        //             console.log("Order ID: ", order.id);
        //             console.log("Order created time: ", result);
        //             console.log("SKU GMV: ", sku.price.amount);
        //             console.log("SKU Est. Paid Commission: ", sku.estimated_paid_commission.amount);
        //         });
        //     }
        // }

        return rawAffiliateOrders;

    } catch (e) {
        console.log("[TIKTOK-AFFILIATE] Error get affiliate info: ", e);
    }
}

function convertTimestamp(orderCreatedTime) {
    const date = new Date(orderCreatedTime * 1000);
    const utc7Date = new Date(date.getTime() + (7 * 60 * 60 * 1000)); 
    const isoString = utc7Date.toISOString();
    const result = isoString.replace('T', ' ').substring(0, 19);
    return result;
}

const brandAffiliateTables = {
    "Eileen Grace": "eileen_grace_tt_affiliate",
    "Mamaway": "mamaway_tt_affiliate",
    "SHRD": "shrd_tt_affiliate",
    "Miss Daisy": "miss_daisy_tt_affiliate",
    "Polynia": "polynia_tt_affiliate",
    "CHESS": "chess_tt_affiliate",
    "Cléviant": "cleviant_tt_affiliate",
    "Mossèru": "mosseru_tt_affiliate",
    "Evoke": "evoke_tt_affiliate",
    "Dr Jou": "dr_jou_tt_affiliate",
    "Mirae": "mirae_tt_affiliate",
    "Swissvita": "swissvita_tt_affiliate",
    "G-Belle": "gbelle_tt_affiliate",
    "Past Nine": "past_nine_tt_affiliate",
    "Nutri & Beyond": "nutri_beyond_tt_affiliate",
    "Ivy & Lily": "ivy_lily_tt_affiliate",
    "Naruko": "naruko_tt_affiliate",
    "Relove": "relove_tt_affiliate",
    "Joey & Roo": "joey_roo_tt_affiliate",
    "Rocketindo Shop": "pinkrocket_tt_affiliate"
}

async function mergeTiktokAffiliate(orders, brand) {
    try {
        console.log("Merging tiktok affiliate orders on brand: ", brand);
        const datasetId = "tiktok_api_us";
        const bigquery = new BigQuery();
        const tableName = brandAffiliateTables[brand];

        let batchSize = 1000;
        for(let i=0; i<orders.length; i+=batchSize) {
            const batchData = orders.slice(i, i+batchSize);

            const incomingOrderIds = batchData.map(row => `'${row.id}'`).join(",");

            if(!incomingOrderIds) continue;

            const query = `
                SELECT Order_ID
                FROM \`${bigquery.projectId}.${datasetId}.${tableName}\`
                WHERE Order_ID IN (${incomingOrderIds})
            `
            const [existingRows] = await bigquery.query(query);
            const existingIds = new Set(existingRows.map(row => row.Order_ID));
            console.log("[TIKTOK-AFFILIATE] Found: ", existingIds.size, " duplicates in table: ", brandAffiliateTables[brand]);

            const dataToInsert = batchData.filter(row => !existingIds.has(row.id));

            if(dataToInsert.length === 0) {
                console.log("[TIKTOK-AFFILIATE] All data already exists. Skipping inserts.");
                continue;
            }

            console.log("[TIKTOK-AFFILIATE] Inserting ", dataToInsert.length, " new rows");

            const formattedData = dataToInsert.map(d => {
                let obj = {};
                obj.Time_Created = convertTimestamp(d.create_time);
                obj.Order_ID = d.id;
                obj.Price = parseInt(d.skus[0].price.amount);
                obj.Est_Commission_Payment = parseInt(d.skus[0].estimated_paid_commission.amount);
                return obj;
            });

            await bigquery
                .dataset(datasetId)
                .table(tableName)
                .insert(formattedData);
            
            console.log("[TIKTOK-AFFILIATE] Successfully inserted rows on: ", brandAffiliateTables[brand]);
        }
    } catch (e) {
        console.log("[TIKTOK-AFFILIATE] Error merging tiktok affiliate orders on brand: ", brand);
        console.log(e);
    }
} 

export async function handleTiktokAffiliate(brand) {
    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;

    await refreshTokens(brand, refreshToken);

    const shopCipher = await getShopCipher(brand, accessToken);
    console.log("Shop cipher: ", shopCipher);

    const affiliateOrders = await handleAffiliate(brand, shopCipher, accessToken);
    affiliateOrders.sort((a, b) => a.create_time - b.create_time);

    // await mergeTiktokAffiliate(affiliateOrders, brand);
}

export async function mainTiktokAffiliate() {
    // await handleTiktokAffiliate("Eileen Grace")
    // await handleTiktokAffiliate("Mamaway");
    // await handleTiktokAffiliate("SHRD");
    // await handleTiktokAffiliate("Miss Daisy");
    // await handleTiktokAffiliate("Polynia");
    // await handleTiktokAffiliate("CHESS");
    await handleTiktokAffiliate("Cléviant");
    await handleTiktokAffiliate("Mossèru");
    await handleTiktokAffiliate("Evoke");
    await handleTiktokAffiliate("Dr Jou");
    // await handleTiktokAffiliate("Mirae")
    // await handleTiktokAffiliate("Swissvita");
    // await handleTiktokAffiliate("G-Belle");
    // await handleTiktokAffiliate("Past Nine");
    // await handleTiktokAffiliate("Nutri & Beyond");
    // await handleTiktokAffiliate("Ivy & Lily");
    // await handleTiktokAffiliate("Naruko");
    // await handleTiktokAffiliate("Relove");
    // await handleTiktokAffiliate("Joey & Roo");
    // await handleTiktokAffiliate("Rocketindo Shop");
    // await handleTiktokAffiliate("M2");
}

// Comment out in deployment. 
await mainTiktokAffiliate();