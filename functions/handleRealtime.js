import crypto from 'crypto';
import { handleMergeRealtime } from './handleMergeRealtime.js';
import { requestWithRetry } from './httpRetry.js';
import 'dotenv/config';

const cancelledOrders = [
    "260404U6KDPMBA",
    "260404U7UK5GFK",
    "260404U8BHESQK",
    "260404UAYMPPCN",
    "260404UN3BGPK4",
    "260404V6WYG2CV",
    "260404V7JQ7QMP",
    "260404VD9BBMKX",
    "260404VE2SU3HE",
    "260404VEBXK8WX",
    "260404VNECK9FB",
    "260404VSDPNVKU"
];

async function getOrderList(brand, partner_id, partner_key, access_token, shop_id, JAKARTA_MIDNIGHT_TODAY, nowSeconds) {
    console.log("[REALTIME-SALES] Handle realtime get order list on brand: ", brand);
    let allOrderSns = [];
    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/order/get_order_list";

    // Removed INVOICE_PENDING (Invalid) and UNPAID
    const statusesToFetch = [
        'READY_TO_SHIP', 
        'PROCESSED', 
        'SHIPPED', 
        'COMPLETED', 
        'IN_CANCEL', 
        'CANCELLED',
    ];

    try {
        // Production
        const time_from = JAKARTA_MIDNIGHT_TODAY;
        const time_to = nowSeconds; 

        // // FOR DEBUGGING / TESTING. COMMENT LATER
        // const time_from = JAKARTA_MIDNIGHT_TODAY - (5 * 86400); // prints 09-08 00:00:00. was 2 * 86400. do 5 * 86400 to take into account orders that are created before 09-08 but not paid until 09-08. 
        // const time_to = JAKARTA_MIDNIGHT_TODAY - (1 * 86400) - 1; // prints 09-08 23:59:59

        // DEBUGGING 09-09. 
        // const time_from = JAKARTA_MIDNIGHT_TODAY - (3 * 86400);
        // const time_to = JAKARTA_MIDNIGHT_TODAY - 1;
        
        const fmt = (s) => new Date(s * 1000).toLocaleString('sv-SE', { timeZone: 'Asia/Jakarta' });

        console.log("Readable time_from: ", fmt(time_from)); // e.g. 2026-09-08 00:00:00
        console.log("Readable time_to: ",   fmt(time_to));

        for (const status of statusesToFetch) {
            let cursor = "";
            let more = true;

            while (more) {
                const timestamp = Math.floor(Date.now() / 1000);
                const baseString = `${partner_id}${PATH}${timestamp}${access_token}${shop_id}`;
                const sign = crypto.createHmac('sha256', partner_key)
                    .update(baseString)
                    .digest('hex');

                const { data } = await requestWithRetry({
                    method: 'get',
                    url: HOST + PATH,
                    params: {
                        partner_id,
                        shop_id,
                        access_token,
                        timestamp,
                        sign,
                        time_range_field: 'update_time', // the production one
                        // time_range_field: 'create_time',
                        time_from: time_from,
                        time_to: time_to,
                        page_size: 100,
                        cursor,
                        order_status: status,
                        response_optional_fields: 'order_status'
                    }
                }, { label: `order-list ${brand}/${status}` });

                if (data.error) {
                    console.log(`[REALTIME-SALES] API Skip [${status}]: ${data.message || data.error}`);
                    break;
                }

                const responseData = data.response;
                // console.log("Raw response order list: ", responseData.order_list);

                if (responseData && responseData.order_list) {
                    responseData.order_list.forEach(order => {
                        // console.log("order: ", order)
                        allOrderSns.push(order.order_sn);
                        // let obj = {
                        //     'order_sn': order.order_sn,
                        //     'status': order.order_status
                        // }
                        // allOrderSns.push(obj)
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

    return [...new Set(allOrderSns)];
    // return allOrderSns
}

async function getOrderDetail(brand, batch, partner_id, partner_key, access_token, shop_id, JAKARTA_MIDNIGHT_TODAY) {
    let totalGMV = 0;
    let orderSnForEscrow = [];
    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/order/get_order_detail";
    let orderCount = 0;

    try {
        
        // // FOR DEBUGGING / TESTING. COMMENT LATER
        // const time_from = JAKARTA_MIDNIGHT_TODAY - (2 * 86400); // prints 09-08 00:00:00. was 2 * 86400. for order_detail logic, this is true. 
        // const time_to = JAKARTA_MIDNIGHT_TODAY - (1 * 86400) - 1; // prints 09-08 23:59:59

        // DEBUGGING 09-09. 
        // const time_from = JAKARTA_MIDNIGHT_TODAY - (1 * 86400);
        // const time_to = JAKARTA_MIDNIGHT_TODAY - 1;

        // Reassignment for readability
        const time_from = JAKARTA_MIDNIGHT_TODAY;
        const time_to = JAKARTA_MIDNIGHT_TODAY
        
        const fmt = (s) => new Date(s * 1000).toLocaleString('sv-SE', { timeZone: 'Asia/Jakarta' });

        // console.log("Readable time_from: ", fmt(time_from)); // e.g. 2026-09-08 00:00:00
        // console.log("Readable time_to: ",   fmt(time_to));

        const order_sn_list = batch.join(',');
        const timestamp = Math.floor(Date.now() / 1000);
        const baseString = `${partner_id}${PATH}${timestamp}${access_token}${shop_id}`;
        const sign = crypto.createHmac('sha256', partner_key)
            .update(baseString)
            .digest('hex');

        const { data } = await requestWithRetry({
            method: 'get',
            url: HOST + PATH,
            params: {
                partner_id,
                shop_id,
                access_token,
                timestamp,
                sign,
                order_sn_list,
                // FIX: Only request item_list.
                // create_time and order_status are returned BY DEFAULT, so don't request them.
                response_optional_fields: 'item_list,pay_time,payment_method'
            }
        }, { label: `order-detail ${brand}` });

        if (data.error) throw new Error(data.message || data.error);

        if (data.response && data.response.order_list) {
            data.response.order_list.forEach(order => {
                let isTargetDate = false;

                // // For debugging: yesterday's orders
                // if (order.payment_method !== 'Cash on Delivery') {
                //     // Non-COD must be PAID today
                //     if (order.pay_time && order.pay_time >= time_from && order.pay_time < time_to) {
                //         isTargetDate = true;
                //     }
                // } else {
                //     // COD must be CREATED today
                //     if (order.create_time && order.create_time >= time_from && order.create_time < time_to) {
                //         isTargetDate = true;
                //     }
                // }
                
                // // Production
                if (order.payment_method !== 'Cash on Delivery') {
                    // Non-COD must be PAID today
                    if (order.pay_time && order.pay_time >= time_from) {
                        isTargetDate = true;
                    }
                } else {
                    // COD must be CREATED today
                    if (order.create_time && order.create_time >= time_from) {
                        isTargetDate = true;
                    }
                }

                if (!isTargetDate) {
                    return; // Drop if it wasn't paid/confirmed yesterday
                }

                if (order.item_list) {
                    let orderTotal = 0;
                    order.item_list.forEach(item => {
                        let price = parseFloat(item.model_discounted_price || 0);
                        const qty = item.model_quantity_purchased || 0;
                        let itemTotal = (price * qty);
                        orderTotal += itemTotal;
                    });
                    orderSnForEscrow.push(order.order_sn);
                    totalGMV += orderTotal;

                    orderCount += 1;
                }
            });
        }

    } catch (e) {
        console.log(`[REALTIME-SALES] Detail Error (${brand}): ${e.message}`);
    }

    // Voucher from seller section
    let voucherFromSellerTotal = 0;
    let batchSize = 20;

    for(let i=0; i<orderSnForEscrow.length; i+=batchSize) {
        const batchOrderSns = orderSnForEscrow.slice(i, i+batchSize);
        const voucherFromSellerBatch = await getEscrowDetailBatch(brand, batchOrderSns, partner_id, partner_key, access_token, shop_id);
        voucherFromSellerTotal += voucherFromSellerBatch;
    }

    console.log("Voucher from seller on brand: ", brand, " per batch: ", voucherFromSellerTotal);

    // return totalGMV - voucherFromSellerTotal;
    // console.log("Order count: ", orderCount);

    return { 
        gmv: totalGMV - voucherFromSellerTotal, 
        count: orderCount 
    }
}

async function getEscrowDetailBatch(brand, batchOrderSns, partner_id, partner_key, access_token, shop_id) {
    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/payment/get_escrow_detail_batch"
    let voucherFromSellerTotal = 0;

    try {
        console.log("Hitting escrow detail batch on brand: ", brand);
        const timestamp = Math.floor(Date.now() / 1000);
        const baseString = `${partner_id}${PATH}${timestamp}${access_token}${shop_id}`;
        const sign = crypto.createHmac('sha256', partner_key)
            .update(baseString)
            .digest('hex');
        const fullUrl = HOST + PATH;

        const { data } = await requestWithRetry({
            method: 'post',
            url: fullUrl,
            data: {
                order_sn_list: batchOrderSns,
            },
            params: {
                partner_id,
                timestamp,
                access_token,
                shop_id,
                sign,
                batchOrderSns
            }
        }, { label: `escrow-detail ${brand}` });

        if (data.error) throw new Error(data.message || data.error);

        // console.log("Voucher from seller on brand: ", brand);
        if(data.response) {
            let escrowDetails = data.response;
            escrowDetails.forEach(e => {
                
                // if(cancelledOrders.includes(e.escrow_detail.order_sn)) {
                //     console.log("Order sn: ", e.escrow_detail.order_sn)
                //     console.log("Voucher from seller: ", e.escrow_detail.order_income.voucher_from_seller);
                // }
                voucherFromSellerTotal += e.escrow_detail.order_income.voucher_from_seller;
            })
        }
    } catch (e) {
        console.log("Error getting escrow detail batch: ", e);
    }

    return voucherFromSellerTotal;
}

export async function mainRealtime(brand, partner_id, partner_key, access_token, shop_id) {
    const nowSeconds = Math.floor(Date.now() / 1000);
    const jakartaOffset = 25200; 
    const secondsPassedToday = (nowSeconds + jakartaOffset) % 86400;
    const JAKARTA_MIDNIGHT_TODAY = nowSeconds - secondsPassedToday;

    const allOrderSns = await getOrderList(brand, partner_id, partner_key, access_token, shop_id, JAKARTA_MIDNIGHT_TODAY, nowSeconds);
    
    console.log(`[REALTIME-SALES] Total ${brand} orders fetched: ${allOrderSns.length}`);
    
    let batchSize = 50;
    let totalSalesBrand = 0;
    let totalSalesCount = 0;

    for(let i = 0; i < allOrderSns.length; i += batchSize) {
        const batchOrderSns = allOrderSns.slice(i, i + batchSize); // Batch order sns here is still unclean. getOrderDetail helps filtering it. 
        const { gmv, count } = await getOrderDetail(brand, batchOrderSns, partner_id, partner_key, access_token, shop_id, JAKARTA_MIDNIGHT_TODAY); // Should get a clean GMV, after voucher from seller. 
        totalSalesBrand += gmv;
        totalSalesCount += count;
    }

    console.log("[REALTIME-SALES] Total GMV on brand: ", brand);
    console.log(totalSalesBrand.toLocaleString('id-ID'));
    console.log(totalSalesCount, " orders");

    let marketplace = "Shopee";
    await handleMergeRealtime(brand, marketplace, totalSalesBrand, totalSalesCount);
}

// await mainM2();

// async function testBed() {
//     let accessToken = "eyJhbGciOiJIUzI1NiJ9.CMvyehABGIzKkkAgASjU3Y3VBjCz9bC_ATgBQAFIBw.LCmq80fyf_ScSj7iL7tryOkHQDyfa3MQE-p5rW0UyiA"
//     await mainRealtime("Mamaway", process.env.MOSS_PARTNER_ID, process.env.MOSS_PARTNER_KEY, accessToken, process.env.MMW_SHOP_ID)
// }

// await testBed()