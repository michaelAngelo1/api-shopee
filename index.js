const express = require('express')
const app = express()
const axios = require('axios')
const crypto = require('crypto')
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();

const fs = require('fs');
const path = require('path');

require('dotenv').config();
const port = 3000

const HOST = "https://partner.shopeemobile.com";
const PATH = "/api/v2/order/get_order_list";
const ORDER_DETAIL_PATH = "/api/v2/order/get_order_detail";

const PARTNER_ID = parseInt(process.env.PARTNER_ID);
const PARTNER_KEY = process.env.PARTNER_KEY;
const SHOP_ID = parseInt(process.env.SHOP_ID);

const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";

const token_file_path = path.join(__dirname, 'shopee-tokens.json');

function saveTokensToFile(tokens) {
    try {
        fs.writeFileSync(token_file_path, JSON.stringify(tokens, null, 2));
        console.log("Tokens saved to file.");
    } catch (e) {
        console.log("Error saving tokens to file: ", e.message);
    }
}

function loadTokensFromFile() {
    try {
        if(fs.existsSync(token_file_path)) {
            const data = fs.readFileSync(token_file_path, 'utf-8');
            const tokens = JSON.parse(data);
            console.log("Tokens loaded from file.");
            return tokens;
        }
    } catch (e) {
        console.log("Error loading tokens from file: ", e.message);
    }

    return {
        accessToken: process.env.INITIAL_ACCESS_TOKEN,
        refreshToken: process.env.INITIAL_REFRESH_TOKEN
    };
}

let loadedTokens = loadTokensFromFile();
let ACCESS_TOKEN = loadedTokens.accessToken;
let REFRESH_TOKEN = loadedTokens.refreshToken;

async function refreshToken() {
    try {
        const path = "/api/v2/auth/access_token/get";
        const timestamp = Math.floor(Date.now() / 1000);
        const baseString = `${PARTNER_ID}${path}${timestamp}`;
        const sign = crypto.createHmac('sha256', PARTNER_KEY)
            .update(baseString)
            .digest('hex');

        const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

        const body = {
            refresh_token: REFRESH_TOKEN,
            partner_id: PARTNER_ID,
            shop_id: SHOP_ID
        }

        console.log("Hitting Refresh Token endpoint: ", fullUrl);

        const response = await axios.post(fullUrl, body, {
            headers: {
                'Content-Type': 'application/json'
            }
        });

        const newAccessToken = response.data.access_token;
        const newRefreshToken = response.data.refresh_token;

        if(newAccessToken && newRefreshToken) {
            ACCESS_TOKEN = newAccessToken;
            REFRESH_TOKEN = newRefreshToken;

            saveTokensToFile({ accessToken: ACCESS_TOKEN, refreshToken: REFRESH_TOKEN });
        } else {
            throw new Error("Tokens dont exist");
        }
    } catch (e) {
        console.log("Error refreshing token: ", e.response ? e.response.data : e.message);
    }
}


const jakartaOffset = 7 * 60 * 60;

function getJakartaTimestampTimeFrom(year, month, day, hour, minute, second) {
    const date = new Date(Date.UTC(year, month, day, hour, minute, second));
    return Math.floor(date.getTime() / 1000) - jakartaOffset;
}

function getJakartaTimestampTimeTo(year, month, day, hour, minute, second) {
    const date = new Date(Date.UTC(year, month, day, hour, minute, second));
    return Math.floor(date.getTime() / 1000);
}

async function writesToBigQuery(orders) {

    const datasetId = 'shopee_api';
    const tableId = 'eileen_grace_orders';

    const ordersToWrite = orders.map(order => ({
        order_sn: order.order_sn,
        order_status: order.order_status,
        created_at: order.create_time,
    }));

    console.log("\n");
    console.log("Writing to BigQuery - Eileen  Grace");
    console.log(`Writing ${ordersToWrite.length} rows to BigQuery...`);
    try {
        await bigquery
            .dataset(datasetId)
            .table(tableId)
            .insert(ordersToWrite);
        console.log(`Inserted ${ordersToWrite.length} rows`);
    } catch (error) {
        console.error('Error inserting rows:', error);
    }
    console.log("\n");
}

async function writesToChangeLog(orders) {

    const [rows] = await bigquery.query({
        query: `
            SELECT order_sn, status
            FROM \`shopee_api.eileen_grace_orders_log\`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY order_sn ORDER BY updated_at DESC) = 1
        `
    });
    const lastStatusMap = {};
    rows.forEach(row => {
        lastStatusMap[row.order_sn] = row.status;
    })

    // Filter by change of status
    const ordersLogToWrite = orders
        .filter(order => lastStatusMap[order.order_sn] !== order.order_status)
        .map(order => ({
            order_sn: order.order_sn,
            status: order.order_status,
            updated_at: order.update_time,
        }));

    console.log("Writing to Eileen Grace Change Log");
    const datasetId = 'shopee_api';
    const tableId = 'eileen_grace_orders_log_staging';

    console.log("Writing to Order Change Log - Eileen Grace");

    if(ordersLogToWrite.length > 0) {
        try {
            
            await bigquery
                .dataset(datasetId)
                .table(tableId)
                .insert(ordersLogToWrite);
            console.log(`Inserted ${ordersLogToWrite.length} rows to staging change log`);

            const mergeQuery = `
                MERGE \`shopee_api.eileen_grace_orders_log\` T
                USING \`shopee_api.eileen_grace_orders_log_staging\` S
                ON T.order_sn = S.order_sn
                WHEN MATCHED THEN
                    UPDATE SET status = S.status, updated_at = S.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (order_sn, status, updated_at)
                    VALUES (S.order_sn, S.status, S.updated_at)
            `;
            await bigquery.query({ query: mergeQuery});
            await bigquery.query({ query: `TRUNCATE TABLE \`shopee_api.eileen_grace_orders_log_staging\``});
            console.log(`Inserted ${ordersLogToWrite.length} rows to prod change log`);

        } catch (error) {
            console.error('Error inserting rows:', error);
        }
        console.log("\n");
    } else {
        console.log("No status changes to log.");
    }
}

async function getOrderDetail(orderList) {

    const orderIds = orderList.map(order => order.order_sn);
    const orderIdChunks = [];

    for(let i=0; i<orderIds.length; i+=50) {
        orderIdChunks.push(orderIds.slice(i, i+50).join(','));
    }

    try {
        let allOrdersWithDetail = [];

        for (orderIdChunk of orderIdChunks) {

            const path = ORDER_DETAIL_PATH;
            const timestamp = Math.floor(Date.now() / 1000);
            const baseString = `${PARTNER_ID}${path}${timestamp}${ACCESS_TOKEN}${SHOP_ID}`;
    
            const sign = crypto.createHmac('sha256', PARTNER_KEY)
                .update(baseString)
                .digest('hex');
            
            optional_fields = [
                "buyer_user_id",
                "buyer_username",
                "estimated_shipping_fee",
                "recipient_address",
                "actual_shipping_fee",
                "goods_to_declare",
                "note",
                "payment_method",
                "item_list",       
                "pay_time",
                "dropshipper",
                "cancel_reason",
                "cancel_by",
                "package_list",
                "total_amount",     
            ]
            
            const params = new URLSearchParams({
                partner_id: PARTNER_ID,
                timestamp: timestamp,
                access_token: ACCESS_TOKEN,
                shop_id: SHOP_ID,
                sign: sign,
                order_sn_list: orderIdChunk,
                response_optional_fields: optional_fields.join(','),
            });

    
            const fullUrl = `${HOST}${path}?${params.toString()}`;
            console.log("Hitting Order Detail endpoint:", fullUrl);
            
            let ordersWithDetail = [];
    
            const response = await axios.get(fullUrl, {
                headers: {
                    'Content-Type': 'application/json'
                }
            });
    
            if(response && response.data.response && Array.isArray(response.data.response.order_list)) {
                console.log("Order Detail response: ", response.data.response.order_list);
                allOrdersWithDetail = allOrdersWithDetail.concat(response.data.response.order_list);
            }
        
        }

        return allOrdersWithDetail;

    } catch (e) {
        console.log("Error getting order detail: ", e);
    }
}


app.get('/orders', async (req, res) => {
    try {
        
        const now = new Date();
        let response;
        
        // Jakarta time for first day of month, 00:00:00
        const timeFrom = getJakartaTimestampTimeFrom(now.getFullYear(), now.getMonth(), 1, 0, 0, 0);
        // Jakarta time for yesterday, 23:59:59
        const yesterday = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
        const timeTo = getJakartaTimestampTimeTo(
            yesterday.getFullYear(),
            yesterday.getMonth(),
            yesterday.getDate(),
            23, 59, 59
        );

        // If date is around 1 - 16
        if(now.getDate() <= 16) {

            
            const timestamp = Math.floor(Date.now() / 1000);
            
    
            // const timeFrom = 1758423113;
            // const timeTo = 1758509513;
            
            const timeRangeField = 'create_time';
    
            const baseString = `${PARTNER_ID}${PATH}${timestamp}${ACCESS_TOKEN}${SHOP_ID}`;
            const sign = crypto.createHmac('sha256', PARTNER_KEY)
                .update(baseString)
                .digest('hex');
    
            // Order List Request Parameters
            const params = new URLSearchParams({
                partner_id: PARTNER_ID,
                timestamp: timestamp,
                access_token: ACCESS_TOKEN,
                shop_id: SHOP_ID,
                sign: sign,
                time_range_field: timeRangeField,
                time_from: timeFrom,
                time_to: timeTo,
                page_size: 100,
                response_optional_fields: 'order_status',
            });
    
            const fullUrl = `${HOST}${PATH}?${params.toString()}`;
    
            console.log("Hitting Order List endpoint:", fullUrl);
    
            // Order List API Call
            response = await axios.get(fullUrl, {
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            res.json(response.data);
        
        } else {

            let intervals = [];
            let start = timeFrom;

            while (start < timeTo) {
                let end = Math.min(start + 15 * 24 * 60 * 60 - 1, timeTo);
                intervals.push({ from: start, to: end });
                start = end + 1;
            }

            let allOrders = [];

            for (const interval of intervals) {
                let hasMore = true;
                let cursor = "";

                while(hasMore) {
                    console.log("\n");
                    console.log(`Fetching data... Interval: ${interval.from} - ${interval.to}, Cursor: ${cursor}`);
                    console.log("\n");
                    
                    const timestamp = Math.floor(Date.now() / 1000);
                    const baseString = `${PARTNER_ID}${PATH}${timestamp}${ACCESS_TOKEN}${SHOP_ID}`;
                    const sign = crypto.createHmac('sha256', PARTNER_KEY)
                        .update(baseString)
                        .digest('hex');
    
                    const params = new URLSearchParams({
                        partner_id: PARTNER_ID,
                        timestamp: timestamp,
                        access_token: ACCESS_TOKEN,
                        shop_id: SHOP_ID,
                        sign: sign,
                        time_range_field: 'create_time',
                        time_from: interval.from,
                        time_to: interval.to,
                        page_size: 100,
                        response_optional_fields: 'order_status',
                    });

                    if (cursor) params.append('cursor', cursor);
    
                    const fullUrl = `${HOST}${PATH}?${params.toString()}`;
                    const response = await axios.get(fullUrl, {
                        headers: { 'Content-Type': 'application/json' }
                    });

                    if (response.data && response.data.response && Array.isArray(response.data.response.order_list)) {
                        allOrders = allOrders.concat(response.data.response.order_list);
                        hasMore = response.data.response.more;
                        cursor = response.data.response.next_cursor || "";
                    } else {
                        hasMore = false;
                    }
                
                }
                

            }

            // Commented for a minute
            const allOrdersWithDetail = await getOrderDetail(allOrders);

            console.log("\n");

            if(allOrdersWithDetail.length > 0) { 
                console.log("Writing to BigQuery. Please do not do it twice :)");
                console.log("Defense mechanism will be implemented later.");
                // await writesToBigQuery(allOrdersWithDetail);
            }

            if(allOrdersWithDetail.length > 0) {
                console.log("Writing to Change Log - Eileen Grace");
                await writesToChangeLog(allOrdersWithDetail);
            }

            res.json({ 
                count: allOrdersWithDetail.length, 
                // count: allOrders.length,
                // orders: allOrders, 
                ordersWithDetail: allOrdersWithDetail
            });
        }

    } catch (e) {
        console.log("Error fetching orders: ", e);
        res.status(500).json({ 
            error: 'Failed to fetch orders',
            details: e.response ? e.response.data : e.message 
        });
    }
})

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
  console.log(`Visit http://localhost:${port}/orders to fetch Shopee orders.`);


  // Refreshing access token every 4 hours
  refreshToken();

  const fourHours = 14400000;
  const fiveMins = 300000;
  setInterval(refreshToken, fourHours - fiveMins);
})
