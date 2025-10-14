import { getReturnDetail, getReturnList } from './api/getReturns.js';
import { getEscrowDetail } from './api/getEscrowDetail.js';
import { getOrderDetail } from './api/getOrderDetail.js';
import { handleReturns } from './api/handleReturns.js';
import { writesToChangeLog } from './api/writesToChangeLog.js';
import { writesToOrderDetail } from './api/writesToOrderDetail.js';
import { formatUnixTime } from './functions/formatUnixTime.js';
import axios from 'axios';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import 'dotenv/config';
import { fileURLToPath } from 'url';

const port = 3000

export const HOST = "https://partner.shopeemobile.com";
const PATH = "/api/v2/order/get_order_list";

export const PARTNER_ID = parseInt(process.env.PARTNER_ID);
export const PARTNER_KEY = process.env.PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.SHOP_ID);

const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
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
export let ACCESS_TOKEN = loadedTokens.accessToken;
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

async function handleOrders(orderDetails, orderEscrows) {
    console.log("Wrangling order details, escrows, and returns. From October 1st to yesterday.");

    console.log("First 3 order details\n");
    console.log(orderDetails.length);

    console.log("First 3 order escrows\n");
    console.log(orderEscrows.length);

    const sampleOrderObjectList = [];
    
    orderDetails.forEach(o => {
        const sampleOrderObject = {
            "No_Pesanan": o.order_sn,
            "Status_Pesanan": o.order_status,
            "Alasan_Pembatalan": o.cancel_reason,
            "Status_Pembatalan_Pengembalian": "",
            "No_Resi": o.package_number,
            "Opsi_Pengiriman": o.shipping_carrier,
            "Pesanan_Harus_Dikirimkan_Sebelum": formatUnixTime("processor - ship by date", o.ship_by_date),
            "Waktu_Pesanan_Dibuat": formatUnixTime("processor - create time", o.create_time),
            "Waktu_Pembayaran_Dilakukan": o.pay_time ? formatUnixTime("processor - pay time", o.pay_time) : "BELUM BAYAR",
            "Metode_Pembayaran": o.payment_method ? o.payment_method : "BELUM BAYAR",
            "SKU_Induk": o.item_sku,
            "Nama_Produk": o.item_name,
            "Nomor_Referensi_SKU": o.item_sku,
            "Nama_Variasi": o.model_name,
            "Harga_Awal": o.model_original_price,
            "Harga_Setelah_Diskon": o.model_discounted_price,
            "Jumlah": o.model_quantity_purchased
        }

        o.item_list.forEach(i => {
            sampleOrderObject.SKU_Induk = i.item_sku;
            sampleOrderObject.Nama_Produk = i.item_name;
            sampleOrderObject.Nomor_Referensi_SKU = i.item_sku;
            sampleOrderObject.Nama_Variasi = i.model_name;
            sampleOrderObject.Harga_Awal = i.model_original_price;
            sampleOrderObject.Harga_Setelah_Diskon = i.model_discounted_price;
            sampleOrderObject.Jumlah = i.model_quantity_purchased;
        })

        sampleOrderObjectList.push(sampleOrderObject);
    });

    // console.log("Sample Order Object List\n");
    // console.log(sampleOrderObjectList)

    const orderObjectList = [];
    const orderObject = {
        "No_Pesanan": "",
        "Status_Pesanan": "",
        "Alasan_Pembatalan": "",
        "Status_Pembatalan_Pengembalian": "",
        "No_Resi": "",
        "Opsi_Pengiriman": "",
        "Pesanan_Harus_Dikirimkan_Sebelum": "",
        "Waktu_Pesanan_Dibuat": "",
        "Waktu_Pembayaran_Dilakukan": "",
        "Metode_Pembayaran": "",
        "SKU_Induk": "",
        "Nama_Produk": "",
        "Nomor_Referensi_SKU": "",
        "Nama_Variasi": "",
        "Harga_Awal": 0,
        "Harga_Setelah_Diskon": 0,
        "Jumlah": 0,
        "Returned_quantity": 0,
        "Total_Harga_Produk": 0,
        "Total_Diskon": 0,
        "Diskon_Dari_Penjual": 0,
        "Diskon_Dari_Shopee": 0,
        "Jumlah_Produk_di_Pesan": 0,
        "Total_Berat": "",
        "Voucher_Ditanggung_Penjual": 0,
        "Voucher_Ditanggung_Shopee": 0,
        "Total_Pembayaran": 0,
        "Perkiraan_Ongkos_Kirim": 0,
    }
}

export async function fetchAndProcessOrders() {
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
                        if (response.data && response.data.response && Array.isArray(response.data.response.order_list)) {
                            allOrdersInBlock = allOrdersInBlock.concat(response.data.response.order_list);
                            hasMore = response.data.response.more;
                            cursor = response.data.response.next_cursor || "";
                        } else { 
                            hasMore = false; 
                        }
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


            } else {
                // If this is the first day of the month
                // Returns belom disini

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

            // Secure check if allOrdersInBlock exists
            const allOrdersWithDetail = await getOrderDetail(allOrdersInBlock && allOrdersInBlock);
            const allEscrowsDetail = await getEscrowDetail(allOrdersInBlock && allOrdersInBlock);

            console.log("\n");
            if(allOrdersWithDetail && allOrdersWithDetail.length > 0) {
                console.log("Writing to Order List - Eileen Grace");

                // await writesToChangeLog(allOrdersWithDetail);

                console.log("Writing to Order Detail - Eileen Grace");

                // await writesToOrderDetail(allOrdersWithDetail);
            }

            // if(allEscrowsDetail && allEscrowsDetail.length > 0) {
            //     console.log("All Escrows on Date <= 16");
            //     console.log(allEscrowsDetail.slice(0, 2));
            // }

            // if(allReturns && allReturns.length > 0) {
            //     console.log("All Returns on Date <= 16");
            //     console.log(allReturns.slice(0, 2));
            // }

            if((allOrdersWithDetail && allOrdersWithDetail.length > 0) && (allEscrowsDetail && allEscrowsDetail.length > 0) && (allReturns && allReturns.length > 0)) {
                console.log("Pass to handle orders & handle returns function");
                handleOrders(allOrdersWithDetail, allEscrowsDetail);
                handleReturns(allReturns);
            } else {
                console.log("Either orders, escrows, or returns doesnt exist");
            }

        } 
        // Else, if date ranges from 17 - 30 or 31
        // Returns belom juga disini
        else {
            console.log("Fetching MTD Orders. Case 17 - 31");

            let intervals = [];
            let start = timeFrom;

            while (start < timeTo) {
                let end = Math.min(start + 15 * 24 * 60 * 60 - 1, timeTo);
                intervals.push({ from: start, to: end });
                start = end + 1;
            }

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
            const allEscrowsDetail = await getEscrowDetail(allOrders);
            console.log("\n");

            if(allOrdersWithDetail.length > 0) {
                console.log("Writing to Orders Log - Eileen Grace");
                await writesToChangeLog(allOrdersWithDetail);
                await writesToOrderDetail(allOrdersWithDetail);
            }

            if(allEscrowsDetail.length > 0) {
                console.log("All Escrows on date > 16");
                console.log(allEscrowsDetail.slice(0, 2));
            }

            if(allReturns && allReturns.length > 0) {
                console.log("All Returns on date > 16");
                console.log(allReturns.slice(0, 2));
            } 
        }

    } catch (e) {
        console.log("Error fetching orders: ", e);
    }
}