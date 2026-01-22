import { BigQuery } from '@google-cloud/bigquery';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios from 'axios';
import crypto from 'crypto';

export const PARTNER_ID = parseInt(process.env.AMS_PARTNER_ID);
export const PARTNER_KEY = process.env.AMS_PARTNER_KEY;

const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";
export const HOST = "https://partner.shopeemobile.com";
const secretClient = new SecretManagerServiceClient();
let AMS_ACCESS_TOKEN, AMS_REFRESH_TOKEN;

async function refreshToken(brand, shop_id) {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');
    
    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: AMS_REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: shop_id
    }

    console.log("Hitting Refresh Token endpoint AMS: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    })

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        AMS_ACCESS_TOKEN = newAccessToken;
        AMS_REFRESH_TOKEN = newRefreshToken;

        saveTokensToSecret(brand, {
            accessToken: AMS_ACCESS_TOKEN,
            refreshToken: AMS_REFRESH_TOKEN
        });
    } else {
        console.log("[AMS] token refresh not found :(")
        throw new Error("AMS Tokens dont exist");
    }
}

let brandSecret = {
    "Eileen Grace": "projects/231801348950/secrets/ams-shopee-tokens",
    "Mamaway": "projects/231801348950/secrets/mamaway-ams-shopee-tokens",
    "SH-RD": "projects/231801348950/secrets/shrd-ams-shopee-tokens",
    "Miss Daisy": "projects/231801348950/secrets/md-ams-shopee-tokens",
    "Polynia": "projects/231801348950/secrets/poly-ams-shopee-tokens",
    "Chess": "projects/231801348950/secrets/chess-ams-shopee-tokens",
    "Cleviant": "projects/231801348950/secrets/clev-ams-shopee-tokens",
    "Mosseru": "projects/231801348950/secrets/moss-ams-shopee-tokens",
    "Evoke": "projects/231801348950/secrets/evoke-ams-shopee-tokens",
    "Dr.Jou": "projects/231801348950/secrets/drjou-ams-shopee-tokens",
    "Mirae": "projects/231801348950/secrets/mirae-ams-shopee-tokens",
    "Swissvita": "projects/231801348950/secrets/sv-ams-shopee-tokens",
    "G-Belle": "projects/231801348950/secrets/gb-ams-shopee-tokens",
    "Past Nine": "projects/231801348950/secrets/pn-ams-shopee-tokens",
    "Nutri & Beyond": "projects/231801348950/secrets/nb-ams-shopee-tokens",
    "Ivy & Lily": "projects/231801348950/secrets/il-ams-shopee-tokens",
}

async function saveTokensToSecret(brand, tokens) {
    let parent = brandSecret[brand];
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'utf-8');
    try {
        const [newVersion] = await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });

        console.log("[AMS] Saved Shopee tokens to Secret Manager");

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
        console.log("[AMS] Successfully saved tokens to AMS Secret Manager: ", parent);
    } catch (e) {
        console.error("[AMS] Error saving tokens to Secret Manager: ", e);
    }
}


async function loadTokensFromSecret(brand) {
    let brandSecretName = {
        "Eileen Grace": "projects/231801348950/secrets/ams-shopee-tokens/versions/latest",
        "Mamaway": "projects/231801348950/secrets/mamaway-ams-shopee-tokens/versions/latest",
        "SH-RD": "projects/231801348950/secrets/shrd-ams-shopee-tokens/versions/latest",
        "Miss Daisy": "projects/231801348950/secrets/md-ams-shopee-tokens/versions/latest",
        "Polynia": "projects/231801348950/secrets/poly-ams-shopee-tokens/versions/latest",
        "Chess": "projects/231801348950/secrets/chess-ams-shopee-tokens/versions/latest",
        "Cleviant": "projects/231801348950/secrets/clev-ams-shopee-tokens/versions/latest",
        "Mosseru": "projects/231801348950/secrets/moss-ams-shopee-tokens/versions/latest",
        "Evoke": "projects/231801348950/secrets/evoke-ams-shopee-tokens/versions/latest",
        "Dr.Jou": "projects/231801348950/secrets/drjou-ams-shopee-tokens/versions/latest",
        "Mirae": "projects/231801348950/secrets/mirae-ams-shopee-tokens/versions/latest",
        "Swissvita": "projects/231801348950/secrets/sv-ams-shopee-tokens/versions/latest",
        "G-Belle": "projects/231801348950/secrets/gb-ams-shopee-tokens/versions/latest",
        "Past Nine": "projects/231801348950/secrets/pn-ams-shopee-tokens/versions/latest",
        "Nutri & Beyond": "projects/231801348950/secrets/nb-ams-shopee-tokens/versions/latest",
        "Ivy & Lily": "projects/231801348950/secrets/il-ams-shopee-tokens/versions/latest",
    }
    const secretName = brandSecretName[brand];
    console.log("SECRET NAME: ", secretName);
    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.error("[AMS] Error loading tokens from Secret Manager: ", e);
    }
}

async function getPerformanceUpdateTime(brand, shop_id) {
    console.log("Running performance update time for brand: ", brand);

    let path = "/api/v2/ams/get_performance_data_update_time";

    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}${AMS_ACCESS_TOKEN}${shop_id}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');

    const params = new URLSearchParams({
        partner_id: PARTNER_ID, 
        timestamp,
        access_token: AMS_ACCESS_TOKEN,
        shop_id: shop_id,
        sign,
        marker_type: "AmsMarker"
    });

    const fullUrl = `${HOST}${path}?${params.toString()}`;
    
    try {
        const response = await axios.get(fullUrl, {
            headers: {
                'Content-Type': 'application/json'
            }
        })    
        if(response && response.data && response.data.response) {
            return response.data.response.last_report_date;
        }
        return "No data";
    } catch (e) {
        console.error("Error get performance update time: ", e.response);
    }
}

export async function fetchAffiliateData(brand, shop_id, sleepValue) {
    
    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    
    await sleep(sleepValue);

    
    console.log('Fetch affiliate data on brand: ', brand);
    
    const loadedTokens = await loadTokensFromSecret(brand);
    AMS_ACCESS_TOKEN = loadedTokens.accessToken;
    AMS_REFRESH_TOKEN = loadedTokens.refreshToken;
    
    await refreshToken(brand, shop_id);
    
    const updateTime = await getPerformanceUpdateTime(brand, shop_id);
    if(updateTime) {
        console.log(`Performance Update Time for ${brand} is ${updateTime}`);
    }
    // Fetch affiliate data per shop_id

    const startDateUpdateTime = new Date(updateTime);
    // const startDateUpdateTime = new Date("2026-01-18");
    const startY = startDateUpdateTime.getFullYear();
    const startM = String(startDateUpdateTime.getMonth() + 1).padStart(2, '0');
    const startD = String(startDateUpdateTime.getDate()).padStart(2, '0');
    const startStr = `${startY}${startM}${startD}`;
    const startForData = `${startY}-${startM}-${startD}`;
    
    let success = false;
    let retries = 5;
    let data;
    
    const yesterday = new Date("2026-01-02");
    yesterday.setDate(yesterday.getDate());
    const yyyy = yesterday.getFullYear();
    const mm = String(yesterday.getMonth() + 1).padStart(2, '0');
    const dd = String(yesterday.getDate()).padStart(2, '0');
    const yesterdayStr = `${yyyy}${mm}${dd}`;
    const yesterdayForData = `${yyyy}-${mm}-${dd}`;

    const today = new Date();
    const year = today.getFullYear();
    const month = String(yesterday.getMonth() + 1).padStart(2, '0');
    const day = String(yesterday.getDate()).padStart(2, '0');
    const todayStr = `${year}${month}${day}`;
    
    while(!success && retries > 0) {
        try {
            
            let path = "/api/v2/ams/get_shop_performance";
            
            const timestamp = Math.floor(Date.now() / 1000);
            const baseString = `${PARTNER_ID}${path}${timestamp}${AMS_ACCESS_TOKEN}${shop_id}`;
            const sign = crypto.createHmac('sha256', PARTNER_KEY)
                .update(baseString)
                .digest('hex');
            

            const params = new URLSearchParams({
                partner_id: PARTNER_ID, 
                timestamp,
                access_token: AMS_ACCESS_TOKEN,
                shop_id: shop_id,
                sign,
                period_type: 'Day',
                start_date: startStr,
                end_date: startStr,
                order_type: 'ConfirmedOrder',
                channel: 'AllChannel',
            });
            
            const fullUrl = `${HOST}${path}?${params.toString()}`;
            console.log(`[AMS] Hitting Affiliate Spending for ${brand}`);
            console.log(fullUrl);
            
            const response = await axios.get(fullUrl, {
                headers: {
                    'Content-Type': 'application/json'
                }
            })
    
            if(response && response.data && response.data.response) {
                success = true;
                console.log(`[AMS] res AMS data on brand: ${brand}`);
                data = response.data.response;
            } else {
                success = true;
                console.log("Non-retryable error.");
                console.log(response);
            }
        } catch (e) {
            console.error("[AMS] Error fetching AMS data on brand: ", brand);
            if(e.response?.status == 429) {
                console.log("Rate limit error");
                retries -= 1;
                await sleep(sleepValue * 1.5)
            } else {
                success = true;
                console.log("Non-rate-limit error: ");
                console.log(e.response);
            }
        }
    }
    console.log('Data before mergeData\n');
    console.log(data);

    await mergeData(data, brand, startForData);
}

const brandTables = {
    "Chess": "chess_ams",
    "Cleviant": "cleviant_ams",
    "Dr.Jou": "dr_jou_ams",
    "Evoke": "evoke_ams",
    "G-Belle": "gbelle_ams",
    "Ivy & Lily": "ivy_lily_ams",
    "Naruko": "naruko_ams",
    "Miss Daisy": "miss_daisy_ams",
    "Mirae": "mirae_ams",
    "Mamaway": "mamaway_ams",
    "Mosseru": "mosseru_ams",
    "Nutri & Beyond": "nutri_beyond_ams",
    "Past Nine": "past_nine_ams",
    "Polynia": "polynia_ams",
    "SH-RD": "shrd_ams",
    "Swissvita": "swissvita_ams",
    "Eileen Grace": "eileen_grace_ams",
    "Relove": "relove_ams",
    "Joey & Roo": "joey_roo_ams",
    "Enchante": "enchante_ams",
    "Rocketindo Shop": "rocketindo_shop_ams",
}

async function mergeData(data, brand, data_date) {
    console.log("[AMS] Start merging for brand: ", brand);
    console.log(data);
    const tableName = brandTables[brand];
    const bigquery = new BigQuery();
    const datasetId = 'shopee_api';

    try {
        const query = `
            SELECT date
            FROM \`${datasetId}.${tableName}\`
            WHERE date = @date
        `;

        const options = {
            query,
            params: {
                date: data_date
            }
        }

        const [rows] = await bigquery.query(options);

        if(rows.length > 0) {
            console.log("[AMS] Row already exists");
            return;
        }

        await bigquery 
            .dataset(datasetId)
            .table(tableName)
            .insert({
                date: data_date,
                sales: data.sales,
                gross_item_sold: data.gross_item_sold,
                orders: data.orders,
                clicks: data.clicks,
                est_commission: data.est_commission,
                roi: data.roi,
                total_buyers: data.total_buyers,
                new_buyers: data.new_buyers,
                process_dttm: new Date(Date.now() + 7 * 60 * 60 * 1000).toISOString().replace('T', ' ').substring(0, 19)
            });
        console.log(`[AMS] Merged to table ${tableName}`);
    } catch (e) {
        console.error(`Error inserting AMS data on ${brand}: ${e}`);
    }
}