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
const ESCROW_DETAIL_PATH = "/api/v2/payment/get_escrow_detail_batch";

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
    return Math.floor(date.getTime() / 1000) - jakartaOffset;
}

// This has become the new order_list
async function writesToChangeLog(orders) {

    const [rows] = await bigquery.query({
        query: `
            SELECT order_sn, status
            FROM \`shopee_api.eileen_grace_orders_list\`
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
    const tableId = 'eileen_grace_orders_list_staging';

    console.log("Writing to Order Change Log - Eileen Grace");

    if(ordersLogToWrite.length > 0) {
        try {
            
            await bigquery
                .dataset(datasetId)
                .table(tableId)
                .insert(ordersLogToWrite);
            console.log(`Inserted ${ordersLogToWrite.length} rows to staging change log`);

            const mergeQuery = `
                MERGE \`shopee_api.eileen_grace_orders_list\` T
                USING \`shopee_api.eileen_grace_orders_list_staging\` S
                ON T.order_sn = S.order_sn
                WHEN MATCHED THEN
                    UPDATE SET status = S.status, updated_at = S.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (order_sn, status, updated_at)
                    VALUES (S.order_sn, S.status, S.updated_at)
            `;
            await bigquery.query({ query: mergeQuery});
            await bigquery.query({ query: `TRUNCATE TABLE \`shopee_api.eileen_grace_orders_list_staging\``});
            console.log(`Inserted ${ordersLogToWrite.length} rows to prod change log`);

        } catch (error) {
            console.error('Error inserting rows:', error);
        }
        console.log("\n");
    } else {
        console.log("No status changes to log.");
    }
}

async function writesToOrderDetail(orders) {

    const [rows] = await bigquery.query({
        query: `
            SELECT order_sn, order_status
            FROM \`shopee_api.eileen_grace_order_detail\`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY order_sn ORDER BY update_time DESC) = 1
        `
    });
    const lastStatusMap = {};
    rows.forEach(row => {
        lastStatusMap[row.order_sn] = row.order_status;
    })

    const orderDetailsToWrite = orders
        .filter(order => lastStatusMap[order.order_sn] !== order.order_status)
        .map(order => ({
                actual_shipping_fee: order.actual_shipping_fee,
                buyer_user_id: order.buyer_user_id,
                buyer_username: order.buyer_username,
                cancel_by: order.cancel_by,
                cancel_reason: order.cancel_reason,
                cod: order.cod,
                create_time: order.create_time,
                days_to_ship: order.days_to_ship,
                estimated_shipping_fee: order.estimated_shipping_fee,
                
                // item_list: order.item_list,
                item_list: order.item_list.map(item => ({
                    item_id: item.item_id,
                    item_name: item.item_name,
                    item_sku: item.item_sku,
                    main_item: item.main_item,
                    model_discounted_price: item.model_discounted_price,
                    model_id: item.model_id,
                    model_name: item.model_name,
                    model_original_price: item.model_original_price,
                    model_quantity_purchased: item.model_quantity_purchased,
                    model_sku: item.model_sku,
                    order_item_id: item.order_item_id,
                })),

                order_sn: order.order_sn,
                order_status: order.order_status,

                package_list: order.package_list.map(item => ({
                    package_number: item.package_number,
                    group_shipment_id: item.group_shipment_id,
                    logistics_status: item.logistics_status,
                    shipping_carrier: item.shipping_carrier,
                    parcel_chargeable_weight_gram: item.parcel_chargeable_weight_gram,
                    item_list: item.item_list.map(subItem => ({
                        item_id: subItem.item_id,
                        model_id: subItem.model_id,
                        model_quantity: subItem.model_quantity,
                        order_item_id: subItem.order_item_id,
                        promotion_group_id: subItem.promotion_group_id,
                        product_location_id: subItem.product_location_id,
                    }))
                })),
            
                pay_time: order.pay_time,
                payment_method: order.payment_method,
                reverse_shipping_fee: order.reverse_shipping_fee,
                ship_by_date: order.ship_by_date,
                total_amount: order.total_amount,
                update_time: order.update_time,
            })
        );
    
    console.log("Writing to Eileen Grace Order Detail");
    const datasetId = 'shopee_api';
    const tableIdStaging = 'eileen_grace_order_detail_staging';

    console.log("Writing to Order Detail - Eileen Grace");

    if(orderDetailsToWrite.length > 0) {
        try {

            const BATCH_SIZE = 500;
            const insertPromises = [];

            for(let i=0; i<orderDetailsToWrite.length; i+=BATCH_SIZE) {
                const chunk = orderDetailsToWrite.slice(i, i+BATCH_SIZE);
                const promise = bigquery
                    .dataset(datasetId)
                    .table(tableIdStaging)
                    .insert(chunk);
                insertPromises.push(promise);
            }

            await Promise.all(insertPromises);

            console.log(`Inserted ${orderDetailsToWrite.length} rows to staging order detail`);

            const mergeQuery = `
                MERGE \`shopee_api.eileen_grace_order_detail\` T
                USING \`shopee_api.eileen_grace_order_detail_staging\` S
                ON T.order_sn = S.order_sn

                -- When an order already exists, update all its fields
                WHEN MATCHED THEN
                    UPDATE SET
                        T.actual_shipping_fee = S.actual_shipping_fee,
                        T.buyer_user_id = S.buyer_user_id,
                        T.buyer_username = S.buyer_username,
                        T.cancel_by = S.cancel_by,
                        T.cancel_reason = S.cancel_reason,
                        T.cod = S.cod,
                        T.create_time = S.create_time,
                        T.days_to_ship = S.days_to_ship,
                        T.estimated_shipping_fee = S.estimated_shipping_fee,
                        T.item_list = S.item_list,
                        T.order_status = S.order_status,
                        T.package_list = S.package_list,
                        T.pay_time = S.pay_time,
                        T.payment_method = S.payment_method,
                        T.reverse_shipping_fee = S.reverse_shipping_fee,
                        T.ship_by_date = S.ship_by_date,
                        T.total_amount = S.total_amount,
                        T.update_time = S.update_time

                -- When it's a new order, insert the entire row
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ROW
            `;
            await bigquery.query({ query: mergeQuery});
            await bigquery.query({ query: `TRUNCATE TABLE \`shopee_api.eileen_grace_order_detail_staging\``});
            console.log(`Inserted ${orderDetailsToWrite.length} rows to prod change log`);
        
        } catch (e) {
            console.error("Error during BigQuery insert/merge:");

            if (e.name === 'PartialFailureError' && e.errors && e.errors.length > 0) {
                console.log('Some rows failed to insert. Details below:');
                e.errors.forEach((errorDetail, index) => {
                    console.log(`\n--- Failure #${index + 1} ---`);
                    console.log(`Problematic Row (order_sn: ${errorDetail.row.order_sn}):`, JSON.stringify(errorDetail.row, null, 2));
                    console.log(`Error Reasons:`);
                    errorDetail.errors.forEach((err, errIndex) => {
                        console.log(`  - ${errIndex + 1}: ${err.message} (Reason: ${err.reason})`);
                    });
                    console.log('------\n');
                });
            } else {
                console.error("A non-partial failure error occurred:", e);
            }
        }
    }
}

async function getEscrowDetail(orderList) {

    const orderIds = orderList.map(order => order.order_sn);
    
    let hasMore = true;
    let encapsOrderIds = [];

    try {
        let allEscrowsDetail = [];

        console.log("\n ORDER ID CHUNKS \n");
        console.log(encapsOrderIds);

        for(orderIdChunk of orderIdChunks) {
            
            const path = ESCROW_DETAIL_PATH;
        }
    } catch (e) {   
        console.log("error getting escrow detail: ", e);
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
                "actual_shipping_fee",
                "buyer_user_id",
                "buyer_username",
                "estimated_shipping_fee",
                "payment_method",
                "item_list",       
                "pay_time",
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


async function fetchAndProcessOrders() {
    console.log("Starting fetchAndProcessOrders job...");
    try {
        await refreshToken();
        
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

            let allOrdersInBlock = [];

            // If it is not first day of the month
            if(now.getDate() > 1) {

                console.log("Fetching MTD Orders");

                let intervals = [];
                let start = timeFrom;
                while(start < timeTo) {
                    let end = Math.min(start + 15 * 24 * 60 * 60 - 1, timeTo);
                    intervals.push({ from: start, to: end });
                    start = end + 1;
                }

                for(const interval of intervals) {
                    let hasMore = true;
                    let cursor = "";
                    while(hasMore) {
                        console.log("\n");
                        console.log(`Fetching data... Interval: ${interval.from} - ${interval.to}, Cursor: ${cursor}`);
                        console.log("\n");
                        const timestamp = Math.floor(Date.now() / 1000);
                        const baseString = `${PARTNER_ID}${PATH}${timestamp}${ACCESS_TOKEN}${SHOP_ID}`;
                        const sign = crypto.createHmac('sha256', PARTNER_KEY).update(baseString).digest('hex');
                        const params = new URLSearchParams({ partner_id: PARTNER_ID, timestamp, access_token: ACCESS_TOKEN, shop_id: SHOP_ID, sign, time_range_field: 'create_time', time_from: interval.from, time_to: interval.to, page_size: 100, response_optional_fields: 'order_status' });
                        if (cursor) params.append('cursor', cursor);
                        const fullUrl = `${HOST}${PATH}?${params.toString()}`;
                        const response = await axios.get(fullUrl, { headers: { 'Content-Type': 'application/json' } });
                        if (response.data && response.data.response && Array.isArray(response.data.response.order_list)) {
                            allOrdersInBlock = allOrdersInBlock.concat(response.data.response.order_list);
                            hasMore = response.data.response.more;
                            cursor = response.data.response.next_cursor || "";
                        } else { 
                            hasMore = false; 
                        }
                    }
                }

            } else {
                // If this is the first day of the month

                console.log("Fetching all orders from the previous month");

                const lastDayOfPrevMonth = new Date(now.getFullYear(), now.getMonth(), 0);
                const firstDayOfPrevMonth = new Date(lastDayOfPrevMonth.getFullYear(), lastDayOfPrevMonth.getMonth(), 1);
                const prevMonthTimeFrom = getJakartaTimestampTimeFrom(firstDayOfPrevMonth.getFullYear(), firstDayOfPrevMonth.getMonth(), 1, 0, 0, 0);
                const prevMonthTimeTo = getJakartaTimestampTimeTo(lastDayOfPrevMonth.getFullYear(), lastDayOfPrevMonth.getMonth(), lastDayOfPrevMonth.getDate(), 23, 59, 59);
            
                let intervals = [];
                let start = prevMonthTimeFrom;
                while(start < prevMonthTimeTo) {
                    let end = Math.min(start + 15 * 24 * 60 * 60 - 1, prevMonthTimeTo);
                    intervals.push({ from: start, to: end });
                    start = end + 1;
                }

                for(const interval of intervals) {
                    let hasMore = true;
                    let cursor = "";
                    while(hasMore) {
                        console.log("\n");
                        console.log(`Fetching data... Interval: ${interval.from} - ${interval.to}, Cursor: ${cursor}`);
                        console.log("\n");
                        const timestamp = Math.floor(Date.now() / 1000);
                        const baseString = `${PARTNER_ID}${PATH}${timestamp}${ACCESS_TOKEN}${SHOP_ID}`;
                        const sign = crypto.createHmac('sha256', PARTNER_KEY).update(baseString).digest('hex');
                        const params = new URLSearchParams(
                            { 
                                partner_id: PARTNER_ID, 
                                timestamp, 
                                access_token: ACCESS_TOKEN, 
                                shop_id: SHOP_ID, 
                                sign, 
                                time_range_field: 'create_time', 
                                time_from: interval.from, 
                                time_to: interval.to, 
                                page_size: 100, 
                                response_optional_fields: 'order_status' 
                            }
                        );
                        if (cursor) params.append('cursor', cursor);
                        const fullUrl = `${HOST}${PATH}?${params.toString()}`;
                        const response = await axios.get(fullUrl, { headers: { 'Content-Type': 'application/json' } });
                        if (response.data && response.data.response && Array.isArray(response.data.response.order_list)) {
                            allOrdersInBlock = allOrdersInBlock.concat(response.data.response.order_list);
                            hasMore = response.data.response.more;
                            cursor = response.data.response.next_cursor || "";
                        } else { hasMore = false; }
                    }
                }
            }

            const allOrdersWithDetail = await getOrderDetail(allOrdersInBlock);
            const allEscrowsDetail = await getEscrowDetail(allOrdersInBlock);

            console.log("\n");
            if(allOrdersWithDetail && allOrdersWithDetail.length > 0) {
                console.log("Writing to Order Detail - Eileen Grace");
                console.log("\nRecent Order Detail - on 1 - 2 October");

                await writesToChangeLog(allOrdersWithDetail);
                await writesToOrderDetail(allOrdersWithDetail);
            }

            if(allEscrowsDetail && allEscrowsDetail.length > 0) {
                console.log("All Escrows");
                allEscrowsDetail.forEach(e => {
                    console.log("Escrow: " + e);
                })
            }

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
                console.log("Writing to Orders Log - Eileen Grace");
                await writesToChangeLog(allOrdersWithDetail);
                await writesToOrderDetail(allOrdersWithDetail);
            }
        }

    } catch (e) {
        console.log("Error fetching orders: ", e);
    }
}

module.exports = { fetchAndProcessOrders };