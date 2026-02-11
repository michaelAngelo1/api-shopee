import axios from 'axios';
import crypto from 'crypto';

async function getOrderList(brand, partner_id, partner_key, access_token, shop_id) {
    console.log("[REALTIME-SALES] Handle realtime get order list on brand: ", brand);
    let allOrderSns = [];
    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/order/get_order_list";

    // Define the statuses you want to fetch
    const statusesToFetch = ['UNPAID', 'READY_TO_SHIP', 'PROCESSED', 'SHIPPED', 'IN_CANCEL', 'CANCELLED'];

    try {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const time_to = nowSeconds;

        // --- ROBUST TIMEZONE FIX (MATH BASED) ---
        // 1. Jakarta is UTC+7 (25200 seconds)
        // 2. Add offset to current time to get "Jakarta Seconds"
        const jakartaSeconds = nowSeconds + 25200;
        // 3. Find how many seconds have passed today in Jakarta (Mod 86400)
        const secondsPassedToday = jakartaSeconds % 86400;
        // 4. Subtract those seconds from 'now' to get Jakarta Midnight in UTC
        const time_from = nowSeconds - secondsPassedToday;

        // DEBUG LOG: Verify this matches 00:00:00 WIB
        console.log(`[DEBUG] Fetching Range (UTC): ${new Date(time_from * 1000).toISOString()} to ${new Date(time_to * 1000).toISOString()}`);

        // Loop through each status one by one
        for (const status of statusesToFetch) {
            let cursor = "";
            let more = true;

            while (more) {
                const timestamp = Math.floor(Date.now() / 1000);
                const baseString = `${partner_id}${PATH}${timestamp}${access_token}${shop_id}`;
                const sign = crypto.createHmac('sha256', partner_key)
                    .update(baseString)
                    .digest('hex');

                const { data } = await axios.get(HOST + PATH, {
                    params: {
                        partner_id,
                        shop_id,
                        access_token,
                        timestamp,
                        sign,
                        time_range_field: "create_time",
                        time_from,
                        time_to,
                        page_size: 100,
                        cursor,
                        order_status: status, // Dynamic status here
                        response_optional_fields: 'order_status'
                    }
                });

                if (data.error) {
                    // Log error but maybe continue to next status? 
                    // For now, throwing error to stop execution as per strict requirements.
                    throw new Error(`Shopee API Error [${status}]: ${data.message || data.error}`);
                }

                const responseData = data.response;
                if (responseData && responseData.order_list) {
                    responseData.order_list.forEach(order => {
                        allOrderSns.push(order.order_sn);
                    });
                    
                    more = responseData.more;
                    cursor = responseData.next_cursor;
                } else {
                    more = false;
                }
            }
        }

    } catch (e) {
        console.log("[REALTIME-SALES] Error get order list on brand: ", brand);
        console.log(e);
    }

    // Optional: Remove duplicates if an order changed status during the fetch (rare but possible)
    return [...new Set(allOrderSns)];
}

async function getOrderDetail(brand, batch, partner_id, partner_key, access_token, shop_id) {
    console.log("[REALTIME-SALES] Handle realtime get order detail on brand: ", brand);
    let totalSales = 0;
    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/order/get_order_detail";

    try {
        const order_sn_list = batch.join(',');
        const timestamp = Math.floor(Date.now() / 1000);
        const baseString = `${partner_id}${PATH}${timestamp}${access_token}${shop_id}`;
        const sign = crypto.createHmac('sha256', partner_key)
            .update(baseString)
            .digest('hex');

        const { data } = await axios.get(HOST + PATH, {
            params: {
                partner_id,
                shop_id,
                access_token,
                timestamp,
                sign,
                order_sn_list,
                // Request order_status so we can filter out CANCELLED orders
                response_optional_fields: 'total_amount,order_status'
            }
        });

        if (data.error) {
             throw new Error(`Shopee API Detail Error: ${data.message || data.error}`);
        }

        if (data.response && data.response.order_list) {
            data.response.order_list.forEach(order => {
                // IMPORTANT: Do not count sales from CANCELLED orders
                // if (order.order_status === 'CANCELLED') return;
                
                totalSales += order.total_amount || 0;
            });
        }

    } catch (e) {
        console.log("[REALTIME-SALES] Error get order detail on brand: ", brand);
        console.log(e);
    }

    return totalSales;
}

export async function mainRealtime(brand, partner_id, partner_key, access_token, shop_id) {
    // 1. Get List (now includes READY_TO_SHIP, SHIPPED, etc, AND CANCELLED)
    const allOrderSns = await getOrderList(brand, partner_id, partner_key, access_token, shop_id);
    
    // Remove duplicates just in case an order changed status between fetch loops
    const uniqueOrderSns = [...new Set(allOrderSns)];

    console.log("All order list count:", uniqueOrderSns.length);
    console.log("Last three:", uniqueOrderSns.slice(-3));

    let batchSize = 50;
    let totalSalesBrand = 0;
    
    for(let i = 0; i < uniqueOrderSns.length; i += batchSize) {
        const batchOrderSns = uniqueOrderSns.slice(i, i + batchSize);
        // 2. Get Detail (Passes all auth params)
        const subTotal = await getOrderDetail(brand, batchOrderSns, partner_id, partner_key, access_token, shop_id);
        totalSalesBrand += subTotal;
    }

    console.log("[REALTIME-SALES] Total sales on brand: ", brand);
    console.log(totalSalesBrand);
}