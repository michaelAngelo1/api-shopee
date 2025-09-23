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

app.get('/orders', async (req, res) => {
    try {
        
        const now = new Date();
        
        const firstDayOfMonth = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0);
        const yesterday = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1, 23, 59, 59);
        
        const timestamp = Math.floor(Date.now() / 1000);
        
        const timeFrom = Math.floor(firstDayOfMonth.getTime() / 1000);
        const timeTo = Math.floor(yesterday.getTime() / 1000);
        
        const timeRangeField = 'create_time';

        const baseString = `${PARTNER_ID}${PATH}${timestamp}${ACCESS_TOKEN}${SHOP_ID}`;
        const sign = crypto.createHmac('sha256', PARTNER_KEY)
            .update(baseString)
            .digest('hex');

        // Shopee API Request Parameters
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

        const response = await axios.get(fullUrl, {
            headers: {
                'Content-Type': 'application/json'
            }
        });

        // Send response back to the client
        res.json(response.data);

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

  refreshToken();

  const fourHours = 14400000;
  const fiveMins = 300000;
  setInterval(refreshToken, fourHours - fiveMins);
})
