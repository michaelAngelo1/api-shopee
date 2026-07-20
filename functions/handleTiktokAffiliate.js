import 'dotenv/config';
import crypto, { randomBytes } from 'crypto';
import axios from 'axios';
import { BigQuery } from '@google-cloud/bigquery';
import { Storage } from '@google-cloud/storage';  
import { loadTokens, refreshTokens, getShopCipher } from '../auth/tiktokAuthAffiliate.js';

const affiliateSchema = {
    fields: [
        { name: 'id', type: 'STRING' },
        { name: 'delivery_time', type: 'STRING' },
        { name: 'create_time', type: 'STRING' },
        { name: 'status', type: 'STRING' },
        { name: 'skus', type: 'RECORD', mode: 'REPEATED', fields: [
            { name: 'sku_id', type: 'STRING' },
            { name: 'settlement_status', type: 'STRING' },
            { name: 'open_collaboration_id', type: 'STRING' },
            { name: 'target_collaboration_id', type: 'STRING' },
            { name: 'campaign_id', type: 'STRING' },
            { name: 'creator_username', type: 'STRING' },
            { name: 'price', type: 'RECORD', fields: [
                { name: 'amount', type: 'NUMERIC' },
                { name: 'currency', type: 'STRING' },
            ]},
            { name: 'quantity', type: 'INTEGER' },
            { name: 'content_type', type: 'STRING' },
            { name: 'content_id', type: 'STRING' },
            { name: 'product_id', type: 'STRING' },
            { name: 'commission_model', type: 'STRING' },
            { name: 'commission_tier_setting', type: 'STRING' },
            { name: 'commission_rate', type: 'NUMERIC' },
            { name: 'partner_commission_rate', type: 'NUMERIC' },
            { name: 'shop_ads_commission_rate', type: 'NUMERIC' },
            { name: 'estimated_commission_base', type: 'RECORD', fields: [
                { name: 'amount', type: 'NUMERIC' },
                { name: 'currency', type: 'STRING' },
            ]},
            { name: 'estimated_paid_shop_ads_commission', type: 'RECORD', fields: [
                { name: 'amount', type: 'NUMERIC' },
                { name: 'currency', type: 'STRING' },
            ]},
            { name: 'estimated_paid_commission', type: 'RECORD', fields: [
                { name: 'amount', type: 'NUMERIC' },
                { name: 'currency', type: 'STRING' },
            ]},
            { name: 'estimated_paid_partner_commission', type: 'RECORD', fields: [
                { name: 'amount', type: 'NUMERIC' },
                { name: 'currency', type: 'STRING' },
            ]},
            { name: 'actual_commission_base', type: 'RECORD', fields: [
                { name: 'amount', type: 'NUMERIC' },
                { name: 'currency', type: 'STRING' },
            ]},
            { name: 'actual_paid_commission', type: 'RECORD', fields: [
                { name: 'amount', type: 'NUMERIC' },
                { name: 'currency', type: 'STRING' },
            ]},
            { name: 'actual_paid_partner_commission', type: 'RECORD', fields: [
                { name: 'amount', type: 'NUMERIC' },
                { name: 'currency', type: 'STRING' },
            ]},
            { name: 'actual_paid_shop_ads_commission', type: 'RECORD', fields: [
                { name: 'amount', type: 'NUMERIC' },
                { name: 'currency', type: 'STRING' },
            ]},
            { name: 'estimated_cofunded_creator_bonus_amount', type: 'RECORD', fields: [
                { name: 'amount', type: 'NUMERIC' },
                { name: 'currency', type: 'STRING' },
            ]},
            { name: 'actual_cofunded_creator_bonus_amount', type: 'RECORD', fields: [
                { name: 'amount', type: 'NUMERIC' },
                { name: 'currency', type: 'STRING' },
            ]},
            { name: 'refunded_quantity', type: 'INTEGER' },
            { name: 'returned_quantity', type: 'INTEGER' },
            { name: 'fully_return', type: 'STRING' },
        ]},
    ]
};

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
        } else if(affiliateAppBrands[brand] == 2) {
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
        // const createTimeFrom = Math.floor(new Date("2026-03-01T00:00:00+07:00").getTime() / 1000);
        // // const createTimeFrom = 1767200458;
        // const createTimeTo = Math.floor(new Date("2026-03-02T23:59:59+07:00").getTime() / 1000);

        const yesterdayDate = new Date(Date.now() - 86400000)
            .toLocaleDateString('sv-SE', { timeZone: 'Asia/Bangkok' });

        // Production
        const startTime = Math.floor(new Date(`${yesterdayDate}T00:00:00+07:00`).getTime() / 1000) - (29 * 86400);
        const endTime = Math.floor(new Date(`${yesterdayDate}T23:59:59+07:00`).getTime() / 1000);

        // Testing
        // const startTime = Math.floor(new Date(`2026-04-01T00:00:00+07:00`).getTime() / 1000);
        // const endTime = Math.floor(new Date(`2026-04-30T23:59:59+07:00`).getTime() / 1000);

        let rawAffiliateOrders = [];
        let rawAffiliateOrdersLength = 0;
        
        while(keepFetching) {
            const requestBody = {
                create_time_ge: startTime,
                create_time_lt: endTime
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
            if(response.data.data && response.data.data.orders) {
                rawAffiliateOrders.push(...response.data.data.orders);
                rawAffiliateOrdersLength += response.data.data.orders.length;   

                const nextPageToken = response.data.data.next_page_token;
    
                if(nextPageToken && nextPageToken.length > 0) {
                    currPageToken = nextPageToken;
                } else {
                    keepFetching = false;
                }
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

        return {
            rawAffiliateOrders,
            startTime,
            endTime
        };

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
    "Rocketindo Shop": "pinkrocket_tt_affiliate",
    "M2": "m2_tt_affiliate"
}

async function mergeTiktokAffiliate(orders, brand) {
    try {
        const datasetId = "tiktok_api_us";
        const bigquery = new BigQuery();
        const storage = new Storage();
        const tableName = brandAffiliateTables[brand];
        const projectId = bigquery.projectId;
        const bucketName = "donotdelete-tiktokaffiliate"; 
        const fileName = `tiktok_affiliate_temp/${brand}_${Date.now()}.ndjson`;

        const formattedData = orders.map(d => JSON.stringify({
            ...d,
            create_time: convertTimestamp(d.create_time),
            delivery_time: convertTimestamp(d.delivery_time),
        })).join('\n');

        const bucket = storage.bucket(bucketName);
        const file = bucket.file(fileName);
        await file.save(formattedData, { contentType: 'application/json' });
        console.log(`[TIKTOK-AFFILIATE] Uploaded to GCS: ${fileName}`);

        try {
            const [job] = await bigquery
                .dataset(datasetId)
                .table(tableName)
                .load(file, {
                    sourceFormat: 'NEWLINE_DELIMITED_JSON',
                    writeDisposition: 'WRITE_APPEND',
                    autodetect: false, 
                    schema: affiliateSchema,
                });
            const errors = job.status?.errors;
            if (errors && errors.length > 0) {
                throw new Error(`Load job failed: ${JSON.stringify(errors)}`);
            }
            console.log(`[TIKTOK-AFFILIATE] Load job ${job.id} completed`);
    
            const dedupeQuery = `
                CREATE OR REPLACE TABLE \`${projectId}.${datasetId}.${tableName}\` AS
                SELECT * EXCEPT(row_num) FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY create_time DESC) AS row_num
                    FROM \`${projectId}.${datasetId}.${tableName}\`
                )
                WHERE row_num = 1
            `;
            await bigquery.query(dedupeQuery);
            console.log(`[TIKTOK-AFFILIATE] Deduplicated ${tableName}`);
    
            await file.delete();
            console.log(`[TIKTOK-AFFILIATE] Cleaned up GCS temp file`);
        } catch (e) {
            console.log("[TIKTOK-AFFILIATE] Error deduping and cleaning: ", e);
            await file.delete().catch(e => console.log('GCS cleanup failed:', e));
        }

    } catch (e) {
        console.log("[TIKTOK-AFFILIATE] Error merging tiktok affiliate: ", e);
    }
}

export async function handleTiktokAffiliate(brand) {
    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;

    await refreshTokens(brand, refreshToken);

    const shopCipher = await getShopCipher(brand, accessToken);
    console.log("Shop cipher: ", shopCipher);

    const { rawAffiliateOrders, startTime, endTime } = await handleAffiliate(brand, shopCipher, accessToken);

    if(rawAffiliateOrders && rawAffiliateOrders.length > 0) {
        rawAffiliateOrders.sort((a, b) => a.create_time - b.create_time);
    
        let startTimeOnData = convertTimestamp(rawAffiliateOrders[0].create_time);
        let endTimeOnData = convertTimestamp(rawAffiliateOrders[rawAffiliateOrders.length - 1].create_time);
    
        // Validate against bigquery
        console.log("Start time on data: ", startTimeOnData);
        console.log("End time on data: ", endTimeOnData);

        await mergeTiktokAffiliate(rawAffiliateOrders, brand);
    
        // setTimeout(() => {}, 3000);
    }
}

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

export async function mainTiktokAffiliate() {
    await handleTiktokAffiliate("Eileen Grace");
    await delay(3000); 
    
    await handleTiktokAffiliate("Mamaway");
    await delay(3000); 
    
    // Partial failure error
    await handleTiktokAffiliate("SHRD");
    await delay(3000); 
    
    await handleTiktokAffiliate("Miss Daisy");
    await delay(3000); 
    
    await handleTiktokAffiliate("Polynia");
    await delay(3000); 
    
    await handleTiktokAffiliate("CHESS");
    await delay(3000); 
    
    await handleTiktokAffiliate("Cléviant");
    await delay(3000); 
    
    await handleTiktokAffiliate("Mossèru");
    await delay(3000); 
    
    await handleTiktokAffiliate("Evoke");
    await delay(3000); 
    
    await handleTiktokAffiliate("Dr Jou");
    await delay(3000); 
    
    // Partial failure error
    await handleTiktokAffiliate("Mirae");
    await delay(3000); 
    
    await handleTiktokAffiliate("Swissvita");
    await delay(3000); 
    
    await handleTiktokAffiliate("G-Belle");
    await delay(3000); 
    
    await handleTiktokAffiliate("Past Nine");
    await delay(3000); 
    
    await handleTiktokAffiliate("Nutri & Beyond");
    await delay(3000); 
    
    await handleTiktokAffiliate("Ivy & Lily");
    await delay(3000); 
    
    await handleTiktokAffiliate("Naruko");
    await delay(3000); 
    
    await handleTiktokAffiliate("Relove");
    await delay(3000); 
    
    await handleTiktokAffiliate("Joey & Roo");
    await delay(3000); 
    
    await handleTiktokAffiliate("Rocketindo Shop");
    await delay(3000); 
    
    await handleTiktokAffiliate("M2");
}

export async function testAffiliateM2() {
    await handleTiktokAffiliate("M2");
}

export async function testAffiliateMosseruJune30() {
    const brand = "Mossèru";

    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;

    await refreshTokens(brand, refreshToken);

    const shopCipher = await getShopCipher(brand, accessToken);
    console.log("[TEST-MOSSERU] Shop cipher: ", shopCipher);

    const tiktokAppKey = "6j7bl3bsi59jh";
    const tiktokAppSecret = "8e7cc952feb703b4ef22fce29c85721c4e98d443";

    const path = "/affiliate_seller/202410/orders/search";
    const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";

    const startTime = Math.floor(new Date(`2026-06-27T00:00:00+07:00`).getTime() / 1000);
    const endTime = Math.floor(new Date(`2026-06-30T23:59:59+07:00`).getTime() / 1000);

    let keepFetching = true;
    let currPageToken = "";
    let rawAffiliateOrders = [];

    while(keepFetching) {
        const requestBody = {
            create_time_ge: startTime,
            create_time_lt: endTime
        };

        const timestamp = Math.floor(Date.now() / 1000);
        const queryParams = {
            app_key: tiktokAppKey,
            page_size: 100,
            timestamp: timestamp,
            shop_cipher: shopCipher
        };
        if(currPageToken) {
            queryParams.page_token = currPageToken;
        }
        const sortedKeys = Object.keys(queryParams).sort();

        let result = tiktokAppSecret + path;
        for(const key of sortedKeys) {
            result += key + queryParams[key];
        }
        result += JSON.stringify(requestBody);
        result += tiktokAppSecret;

        const sign = crypto.createHmac('sha256', tiktokAppSecret).update(result).digest('hex');
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

        if(response.data.data && response.data.data.orders) {
            rawAffiliateOrders.push(...response.data.data.orders);

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        } else {
            keepFetching = false;
        }
    }

    const TARGET_CREATOR = "tomtomtomttttt";
    const TARGET_CONTENT_ID = "7656011289190468370";

    const matchingOrders = rawAffiliateOrders
        .map(order => ({
            ...order,
            skus: (order.skus || []).filter(sku =>
                sku.creator_username === TARGET_CREATOR && sku.content_id === TARGET_CONTENT_ID
            ),
        }))
        .filter(order => order.skus.length > 0);

    console.log(`[TEST-MOSSERU] Orders for creator_username="${TARGET_CREATOR}" content_id="${TARGET_CONTENT_ID}" (qty: ${matchingOrders.length}):`);
    console.log(JSON.stringify(matchingOrders, null, 2));

    return matchingOrders;
}

await testAffiliateMosseruJune30();
// Comment out in deployment.
// await mainTiktokAffiliate();
