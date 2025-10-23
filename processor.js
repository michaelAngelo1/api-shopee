import { getReturnDetail, getReturnList } from './api/eileen_grace/getReturns.js';
import { getEscrowDetail } from './api/eileen_grace/getEscrowDetail.js';
import { getOrderDetail } from './api/eileen_grace/getOrderDetail.js';
import { handleReturns } from './api/eileen_grace/handleReturns.js';
import { handleOrders } from './api/eileen_grace/handleOrders.js';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios from 'axios';
import crypto from 'crypto';
import 'dotenv/config';
// import fs from 'fs';
// import path from 'path';
// import { fileURLToPath } from 'url';

const port = 3000
let secretClient;

export const HOST = "https://partner.shopeemobile.com";
const PATH = "/api/v2/order/get_order_list";

export const PARTNER_ID = parseInt(process.env.PARTNER_ID);
export const PARTNER_KEY = process.env.PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.SHOP_ID);

const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";

export let ACCESS_TOKEN;
let REFRESH_TOKEN;

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

            // saveTokensToFile({ accessToken: ACCESS_TOKEN, refreshToken: REFRESH_TOKEN });

            saveTokensToSecret({ accessToken: ACCESS_TOKEN, refreshToken: REFRESH_TOKEN });
        } else {
            throw new Error("Tokens dont exist");
        }
    } catch (e) {
        console.log("Error refreshing token: ", e.response ? e.response.data : e.message);
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/shopee-tokens';
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'UTF-8');
    secretClient = new SecretManagerServiceClient();

    try {
        await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            },
        });
        console.log("Saved Shopee tokens to Secret Manager");
    } catch (e) {
        console.log("Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    secretClient = new SecretManagerServiceClient();
    const secretName = 'projects/231801348950/secrets/shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.log("Error loading tokens from Secret Manager: ", e);
        return {
            accessToken: process.env.INITIAL_ACCESS_TOKEN,
            refreshToken: process.env.INITIAL_REFRESH_TOKEN
        }
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

export function getStartOfMonthTimestampWIB() {
    const now = new Date(); 
    const year = now.getFullYear();
    const month = now.getMonth();
    const startOfMonthUTC = new Date(Date.UTC(year, month, 0, 0, 0, 0)); 
    const startOfMonthWIB = new Date(startOfMonthUTC.getTime() - (7 * 60 * 60 * 1000));
    return Math.floor(startOfMonthWIB.getTime() / 1000);
}

export function getEndOfYesterdayTimestampWIB() {
    const now = new Date(); 
    const startOfTodayUTC = new Date(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0));
    const startOfTodayWIB = new Date(startOfTodayUTC.getTime() - (7 * 60 * 60 * 1000));
    const endOfYesterdayWIB = new Date(startOfTodayWIB.getTime() - 1000); // Subtract 1 second
    return Math.floor(endOfYesterdayWIB.getTime() / 1000);
}

// Function to get UTC Unix timestamp for the START of the PREVIOUS month in WIB
export function getStartOfPreviousMonthTimestampWIB() {
    const now = new Date();
    // Calculate the year and month of the previous month
    let prevMonthYear = now.getFullYear();
    let prevMonthMonth = now.getMonth() - 1; // Month is 0-indexed (Jan=0)
    if (prevMonthMonth < 0) {
        prevMonthMonth = 11; // December
        prevMonthYear--;
    }
    // Create date for the 1st of the PREVIOUS month IN UTC, then adjust back 7 hours for WIB
    const startOfPrevMonthUTC = new Date(Date.UTC(prevMonthYear, prevMonthMonth, 1, 0, 0, 0));
    const startOfPrevMonthWIB = new Date(startOfPrevMonthUTC.getTime() - (7 * 60 * 60 * 1000));
    return Math.floor(startOfPrevMonthWIB.getTime() / 1000);
}

// Function to get UTC Unix timestamp for the END of the PREVIOUS month in WIB
export function getEndOfPreviousMonthTimestampWIB() {
    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth(); // Current month
    // Create date for the 1st of the CURRENT month IN UTC, adjust back 7 hours for WIB midnight
    const startOfMonthUTC = new Date(Date.UTC(year, month, 1, 0, 0, 0));
    const startOfMonthWIB = new Date(startOfMonthUTC.getTime() - (7 * 60 * 60 * 1000));
    // Subtract 1 second to get the end of the PREVIOUS month WIB
    const endOfPrevMonthWIB = new Date(startOfMonthWIB.getTime() - 1000);
    return Math.floor(endOfPrevMonthWIB.getTime() / 1000);
}

async function fetchOrdersAndReturnsFromPrevMonth(now, ACCESS_TOKEN) {
    console.log("Fetching all orders from the previous month");

    let allOrdersInBlock = [];
    let allReturns = [];
    const lastDayOfPrevMonth = new Date(now.getFullYear(), now.getMonth(), 0);
    const firstDayOfPrevMonth = new Date(lastDayOfPrevMonth.getFullYear(), lastDayOfPrevMonth.getMonth(), 1);
    const prevMonthTimeFrom = getJakartaTimestampTimeFrom(firstDayOfPrevMonth.getFullYear(), firstDayOfPrevMonth.getMonth(), 1, 0, 0, 0);
    const prevMonthTimeTo = getJakartaTimestampTimeTo(lastDayOfPrevMonth.getFullYear(), lastDayOfPrevMonth.getMonth(), lastDayOfPrevMonth.getDate(), 23, 59, 59);

    const timeFrom = getStartOfMonthTimestampWIB();
    const timeTo = getEndOfYesterdayTimestampWIB();

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

    console.log("Fetching All Returns from Previous Month");
    console.log("Intervals for Return List");

    let allReturnList = [];
    for(const interval of intervals) {
        allReturnList = await getReturnList(interval.from, interval.to);
        // Pass to getReturnDetail, with return_sn being request parameters
        if(allReturnList && allReturnList.length > 0) {
            const allReturnDetails = await getReturnDetail(allReturnList);

            allReturns = allReturns.concat(allReturnDetails);
        } else {
            console.log("allReturnList does not exist.\n");
            console.log(allReturnList);
        }
    }

    return {
        allOrdersInBlock,
        allReturns
    }
}

export async function fetchAndProcessOrders() {
    console.log("Starting fetchAndProcessOrders job...");
    try {

        const loadedTokens = await loadTokensFromSecret();
        if(!loadedTokens && !loadedTokens.refreshToken) {
            throw new Error("INDEXJS: Failed to load valid tokens from secret manager");
        }

        ACCESS_TOKEN = loadedTokens.accessToken;
        REFRESH_TOKEN = loadedTokens.refreshToken;

        await refreshToken();
        
        const now = new Date();
        let response;
        
        // Jakarta time for first day of month, 00:00:00. - 1
        // const timeFrom = getJakartaTimestampTimeFrom(now.getFullYear(), now.getMonth(), 0, 0, 0, 0);
        // Jakarta time for yesterday, 23:59:59
        // const yesterday = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
        // const timeTo = getJakartaTimestampTimeTo(
        //     yesterday.getFullYear(),
        //     yesterday.getMonth(),
        //     yesterday.getDate(),
        //     23, 59, 59
        // );
        const timeFrom = getStartOfMonthTimestampWIB();
        const timeTo = getEndOfYesterdayTimestampWIB();

        // If date is around 1 - 16
        // Returns are done in this if-block.
        if(now.getDate() <= 16) {

            // Uncomment if batch submission does not work
            // let allOrdersInBlock = [];
            let allReturns = [];

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

                // THIS IS IMPORTANT. Uncomment later.
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
                        
                        const params = new URLSearchParams({ 
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
                        });
                        
                        if (cursor) params.append('cursor', cursor);
                        const fullUrl = `${HOST}${PATH}?${params.toString()}`;
                        const response = await axios.get(fullUrl, { headers: { 'Content-Type': 'application/json' } });

                        // ## Comment this out if this does not work.
                        if (response.data && response.data.response && Array.isArray(response.data.response.order_list)) {
                        
                            const onePageOfOrders = response.data.response.order_list;

                            if (onePageOfOrders.length > 0) {
                                // 2. PROCESS ONE PAGE AT A TIME
                                console.log(`Processing batch of ${onePageOfOrders.length} orders...`);
                                
                                const onePageWithDetail = await getOrderDetail(onePageOfOrders);
                                const onePageOfEscrows = await getEscrowDetail(onePageOfOrders);

                                // 3. CALL handleOrders FOR JUST THIS PAGE
                                //    (handleOrders will then call mergeOrders for this small batch)
                                await handleOrders(onePageWithDetail, onePageOfEscrows);
                            }

                            hasMore = response.data.response.more;
                            cursor = response.data.response.next_cursor || "";
                        } else {
                            hasMore = false;
                        }

                        // ## Uncomment if the above case does not work.
                        // if (response.data && response.data.response && Array.isArray(response.data.response.order_list)) {
                        //     allOrdersInBlock = allOrdersInBlock.concat(response.data.response.order_list);
                        //     hasMore = response.data.response.more;
                        //     cursor = response.data.response.next_cursor || "";
                        // } else { 
                        //     hasMore = false; 
                        // }
                    }

                }
                
                console.log("Fetching Return List Orders");
                console.log("Intervals for Return List");

                let allReturnList = [];
                for(const interval of intervals) {
                    allReturnList = await getReturnList(interval.from, interval.to);
                    // Pass to getReturnDetail, with return_sn being request parameters
                    if(allReturnList && allReturnList.length > 0) {
                        const allReturnDetails = await getReturnDetail(allReturnList);

                        allReturns = allReturns.concat(allReturnDetails);
                    } else {
                        console.log("allReturnList does not exist.\n");
                        console.log(allReturnList);
                    }
                }

                if(now.getDate() === 16) {
                    console.log("Today is 16th, triggering Lock GMV function.");
                }

            } else {
                // If this is the first day of the month

                // const prevMonthData = await fetchOrdersAndReturnsFromPrevMonth(now, ACCESS_TOKEN);

                // allOrdersInBlock = prevMonthData.allOrdersInBlock;
                // allReturns = prevMonthData.allReturns;

                console.log("Fetching all orders from the previous month");

                const lastDayOfPrevMonth = new Date(now.getFullYear(), now.getMonth(), 0);

                // This should also fetch the day before the first day of the previous month.
                const firstDayOfPrevMonth = new Date(lastDayOfPrevMonth.getFullYear(), lastDayOfPrevMonth.getMonth(), 0);

                // const prevMonthTimeFrom = getJakartaTimestampTimeFrom(
                //     firstDayOfPrevMonth.getFullYear(), 
                //     firstDayOfPrevMonth.getMonth(), 
                //     firstDayOfPrevMonth.getDate(),
                //     0, 0, 0
                // );
                // const prevMonthTimeTo = getJakartaTimestampTimeTo(
                //     lastDayOfPrevMonth.getFullYear(), 
                //     lastDayOfPrevMonth.getMonth(), 
                //     lastDayOfPrevMonth.getDate(), 
                //     23, 59, 59
                // );

                const prevMonthTimeFrom = getStartOfPreviousMonthTimestampWIB();
                const prevMonthTimeTo = getEndOfPreviousMonthTimestampWIB();
            
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

                        // ## Comment this out if this does not work.
                        if (response.data && response.data.response && Array.isArray(response.data.response.order_list)) {
                        
                            const onePageOfOrders = response.data.response.order_list;

                            if (onePageOfOrders.length > 0) {
                                // 2. PROCESS ONE PAGE AT A TIME
                                console.log(`Processing batch of ${onePageOfOrders.length} orders...`);
                                
                                const onePageWithDetail = await getOrderDetail(onePageOfOrders);
                                const onePageOfEscrows = await getEscrowDetail(onePageOfOrders);

                                // 3. CALL handleOrders FOR JUST THIS PAGE
                                //    (handleOrders will then call mergeOrders for this small batch)
                                await handleOrders(onePageWithDetail, onePageOfEscrows);
                            }

                            hasMore = response.data.response.more;
                            cursor = response.data.response.next_cursor || "";
                        } else {
                            hasMore = false;
                        }

                        // ## Uncomment if the above case does not work.
                        // if (response.data && response.data.response && Array.isArray(response.data.response.order_list)) {
                        //     allOrdersInBlock = allOrdersInBlock.concat(response.data.response.order_list);
                        //     hasMore = response.data.response.more;
                        //     cursor = response.data.response.next_cursor || "";
                        // } else { hasMore = false; }
                    }
                }

                console.log("Fetching All Returns from Previous Month");
                console.log("Intervals for Return List");

                let allReturnList = [];
                for(const interval of intervals) {
                    allReturnList = await getReturnList(interval.from, interval.to);
                    // Pass to getReturnDetail, with return_sn being request parameters
                    if(allReturnList && allReturnList.length > 0) {
                        const allReturnDetails = await getReturnDetail(allReturnList);

                        allReturns = allReturns.concat(allReturnDetails);
                    } else {
                        console.log("allReturnList does not exist.\n");
                        console.log(allReturnList);
                    }
                }
            }

            // Secure check if allOrdersInBlock exists
            // const allOrdersWithDetail = await getOrderDetail(allOrdersInBlock && allOrdersInBlock);
            // const allEscrowsDetail = await getEscrowDetail(allOrdersInBlock && allOrdersInBlock);

            // console.log("\n");
            // if(allOrdersWithDetail && allOrdersWithDetail.length > 0) {
            //     console.log("Writing to Order List - Eileen Grace");

                // await writesToChangeLog(allOrdersWithDetail);

                // console.log("Writing to Order Detail - Eileen Grace");

                // await writesToOrderDetail(allOrdersWithDetail);
            // }

            // if(allEscrowsDetail && allEscrowsDetail.length > 0) {
            //     console.log("All Escrows on Date <= 16");
            //     console.log(allEscrowsDetail.slice(0, 2));
            // }

            // if(allReturns && allReturns.length > 0) {
            //     console.log("All Returns on Date <= 16");
            //     console.log(allReturns.slice(0, 2));
            // }

            if(allReturns && allReturns.length > 0) {
                console.log("Pass to handle returns function");
                // handleOrders(allOrdersWithDetail, allEscrowsDetail);
                handleReturns(allReturns);
            } else {
                console.log("Returns doesnt exist");
                console.log("All returns: ", allEscrowsDetail.slice(0, 1));
            }

        } 
        // Else, if date ranges from 17 - 30 or 31
        else {
            console.log("Fetching MTD Orders. Case 17 - 31");

            let intervals = [];
            let start = timeFrom;

            while (start < timeTo) {
                let end = Math.min(start + 15 * 24 * 60 * 60 - 1, timeTo);
                intervals.push({ from: start, to: end });
                start = end + 1;
            }

            console.log("Intervals\n");
            console.log("Interval from: ", intervals[0].from);
            console.log("\n");
            console.log("Interval to: ", intervals[intervals.length - 1].to);
            console.log("\n");

            console.log("Full Intervals\n");
            intervals.forEach(interval => {
                console.log(`From: ${interval.from} To: ${interval.to}`);
            });
            console.log("\n");

            let allOrders = [];
            let allReturns = [];

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

                    // ## Comment this out if this does not work.
                    if (response.data && response.data.response && Array.isArray(response.data.response.order_list)) {
                    
                        const onePageOfOrders = response.data.response.order_list;

                        if (onePageOfOrders.length > 0) {
                            // 2. PROCESS ONE PAGE AT A TIME
                            console.log(`Processing batch of ${onePageOfOrders.length} orders...`);
                            
                            const onePageWithDetail = await getOrderDetail(onePageOfOrders);
                            const onePageOfEscrows = await getEscrowDetail(onePageOfOrders);

                            // 3. CALL handleOrders FOR JUST THIS PAGE
                            //    (handleOrders will then call mergeOrders for this small batch)
                            await handleOrders(onePageWithDetail, onePageOfEscrows);
                        }

                        hasMore = response.data.response.more;
                        cursor = response.data.response.next_cursor || "";
                    } else {
                        hasMore = false;
                    }

                    // ## Uncomment to rollback if the above case fails.
                    // if (response.data && response.data.response && Array.isArray(response.data.response.order_list)) {
                    //     allOrders = allOrders.concat(response.data.response.order_list);
                    //     hasMore = response.data.response.more;
                    //     cursor = response.data.response.next_cursor || "";
                    // } else {
                    //     hasMore = false;
                    // }
                
                }
                

            }

            let allReturnList = [];
            for(const interval of intervals) {
                allReturnList = await getReturnList(interval.from, interval.to);
                // Pass to getReturnDetail, with return_sn being request parameters
                if(allReturnList && allReturnList.length > 0) {
                    const allReturnDetails = await getReturnDetail(allReturnList);

                    allReturns = allReturns.concat(allReturnDetails);
                } else {
                    console.log("allReturnList does not exist.\n");
                    console.log(allReturnList);
                }
            }

            // Commented for a minute
            // const allOrdersWithDetail = await getOrderDetail(allOrders && allOrders);
            // const allEscrowsDetail = await getEscrowDetail(allOrders && allOrders);

            // console.log("\n");
            // if(allOrdersWithDetail && allOrdersWithDetail.length > 0) {
                // console.log("Writing to Order List - Eileen Grace");

                // await writesToChangeLog(allOrdersWithDetail);

                // console.log("Writing to Order Detail - Eileen Grace");

                // await writesToOrderDetail(allOrdersWithDetail);
            // }

            // if(allEscrowsDetail && allEscrowsDetail.length > 0) {
            //     console.log("All Escrows on Date <= 16");
            //     console.log(allEscrowsDetail.slice(0, 2));
            // }

            // if(allReturns && allReturns.length > 0) {
            //     console.log("All Returns on Date <= 16");
            //     console.log(allReturns.slice(0, 2));
            // }

            if(allReturns && allReturns.length > 0) {
                console.log("Pass to returns function");
                // handleOrders(allOrdersWithDetail, allEscrowsDetail);
                handleReturns(allReturns);
            } else {
                console.log("Returns doesnt exist");
            }


        }

    } catch (e) {
        console.log("Error fetching orders: ", e);
    }
}