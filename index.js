const express = require('express')
const app = express()
const axios = require('axios')
const crypto = require('crypto')

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


const jakartaOffset = 7 * 60 * 60; // UTC+7 in seconds

function getJakartaTimestampTimeFrom(year, month, day, hour, minute, second) {
    // month is 0-based in JS Date
    const date = new Date(Date.UTC(year, month, day, hour, minute, second));
    // ADD 7 hours to get Jakarta time in UTC
    // return Math.floor(date.getTime() / 1000) + jakartaOffset;
    return Math.floor(date.getTime() / 1000) - jakartaOffset;
}

function getJakartaTimestampTimeTo(year, month, day, hour, minute, second) {
    // month is 0-based in JS Date
    const date = new Date(Date.UTC(year, month, day, hour, minute, second));
    // ADD 7 hours to get Jakarta time in UTC
    return Math.floor(date.getTime() / 1000);
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
            
            const params = new URLSearchParams({
                partner_id: PARTNER_ID,
                timestamp: timestamp,
                access_token: ACCESS_TOKEN,
                shop_id: SHOP_ID,
                sign: sign,
                order_sn_list: orderIdChunk,
                response_optional_fields: "total_amount",
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

            const allOrdersWithDetail = await getOrderDetail(allOrders);

            console.log("\n");

            res.json({ 
                count: allOrdersWithDetail.length, 
                // orders: allOrders, 
                ordersWithDetail: allOrdersWithDetail
            });
        }

    } catch (e) {
        console.log("Error fetching orders: ", e.response ? e.response.data : e.message);
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
