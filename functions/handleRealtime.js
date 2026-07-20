import axios from 'axios';
import crypto from 'crypto';
import { handleMergeRealtime } from './handleMergeRealtime.js';
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
        'CANCELLED'
    ];

    try {
        // 1. Calculate Jakarta Midnight ONCE globally to ensure consistency
        // Jakarta is UTC+7 (25200 seconds)
        // const nowSeconds = Math.floor(Date.now() / 1000);
        // const jakartaOffset = 25200; 
        // const secondsPassedToday = (nowSeconds + jakartaOffset) % 86400;
        // const JAKARTA_MIDNIGHT_TS = nowSeconds - secondsPassedToday;
        // Use the Jakarta Midnight timestamp we calculated
        
        // Production
        const time_from = JAKARTA_MIDNIGHT_TODAY;
        const time_to = nowSeconds; 

        // // FOR DEBUGGING / TESTING. COMMENT LATER
        // const time_from = JAKARTA_MIDNIGHT_TODAY - (2 * 86400); 
        // const time_to = nowSeconds;

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
                        time_range_field: 'update_time',
                        time_from: time_from,
                        time_to: time_to,
                        page_size: 100,
                        cursor,
                        order_status: status,
                        response_optional_fields: 'order_status'
                    }
                });

                if (data.error) {
                    console.log(`[REALTIME-SALES] API Skip [${status}]: ${data.message || data.error}`);
                    break;
                }

                const responseData = data.response;
                console.log("Raw response order list: ", responseData.order_list);

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
        // 1. Calculate Jakarta Midnight ONCE globally to ensure consistency
        // Jakarta is UTC+7 (25200 seconds)
        // const nowSeconds = Math.floor(Date.now() / 1000);
        // const jakartaOffset = 25200; 
        // const secondsPassedToday = (nowSeconds + jakartaOffset) % 86400;
        // const JAKARTA_MIDNIGHT_TODAY = nowSeconds - secondsPassedToday;
        // const JAKARTA_MIDNIGHT_YESTERDAY = JAKARTA_MIDNIGHT_TODAY - (2 * 86400);
        // const JAKARTA_MIDNIGHT_TODAY_ADJUSTED = JAKARTA_MIDNIGHT_TODAY - 86400;
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
                // FIX: Only request item_list. 
                // create_time and order_status are returned BY DEFAULT, so don't request them.
                response_optional_fields: 'item_list,pay_time,payment_method'
            }
        });

        if (data.error) throw new Error(data.message || data.error);

        if (data.response && data.response.order_list) {
            data.response.order_list.forEach(order => {
                let isTargetDate = false;

                // // For debugging: yesterday's orders
                // if (order.payment_method !== 'Cash on Delivery') {
                //     // Non-COD must be PAID today
                //     if (order.pay_time && order.pay_time >= JAKARTA_MIDNIGHT_YESTERDAY && order.pay_time < JAKARTA_MIDNIGHT_TODAY) {
                //         isTargetDate = true;
                //     }
                // } else {
                //     // COD must be CREATED today
                //     if (order.create_time && order.create_time >= JAKARTA_MIDNIGHT_YESTERDAY && order.create_time < JAKARTA_MIDNIGHT_TODAY) {
                //         isTargetDate = true;
                //     }
                // }
                
                // // Production
                if (order.payment_method !== 'Cash on Delivery') {
                    // Non-COD must be PAID today
                    if (order.pay_time && order.pay_time >= JAKARTA_MIDNIGHT_TODAY) {
                        isTargetDate = true;
                    }
                } else {
                    // COD must be CREATED today
                    if (order.create_time && order.create_time >= JAKARTA_MIDNIGHT_TODAY) {
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
                        // console.log("Item model discounted price: ", price, " for brand: ", brand);
                        
                        // if (cancelledOrders.includes(order.order_sn)) {
                        //     console.log("Order Sn cancelled: ", order.order_sn);
                        //     console.log(item);                       
                        // }
                        
                        const qty = item.model_quantity_purchased || 0;
                        let itemTotal = (price * qty);
                        // console.log("Item Subtotal: ", itemTotal)
                        
                        // if (order.order_status === 'CANCELLED') {
                        //     console.log(`[GHOST CAUGHT] Cancelled Order added to GMV: ${order.order_sn} | Value: Rp ${itemTotal} | COD: ${order.payment_method === 'Cash on Delivery'}`);
                        // }


                        
                        orderTotal += itemTotal;
                        // console.log("Total GMV running total: ", totalGMV, " for brand: ", brand);
                    });
                    orderSnForEscrow.push(order.order_sn);
                    // console.log("Order sn: ", order.order_sn, " order status: ", order.order_status, " order value: ", orderTotal, " payment method: ", order.payment_method);
                    totalGMV += orderTotal;

                    orderCount += 1;
                }
            });
        }

    } catch (e) {
        console.log(`[REALTIME-SALES] Detail Error (${brand}): ${e.message}`);
    }

    // let voucherFromSellerTotal = 0;
    // let batchSize = 20;

    // for(let i=0; i<orderSnForEscrow.length; i+=batchSize) {
    //     const batchOrderSns = orderSnForEscrow.slice(i, i+batchSize);
    //     const voucherFromSellerBatch = await getEscrowDetailBatch(brand, batchOrderSns, partner_id, partner_key, access_token, shop_id);
    //     voucherFromSellerTotal += voucherFromSellerBatch;
    // }

    // console.log("Voucher from seller on brand: ", brand, " per batch: ", voucherFromSellerTotal);

    // return totalGMV - voucherFromSellerTotal;
    // console.log("Order count: ", orderCount);

    return { 
        gmv: totalGMV, 
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

        const { data } = await axios.post(fullUrl, 
            {
                order_sn_list: batchOrderSns,
            },    
            {
                params: {
                    partner_id,
                    timestamp,
                    access_token,
                    shop_id,
                    sign,
                    batchOrderSns
                }
            }
        );

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

// async function testbed() {

//     // let egPartnerId = "2010478"
//     // let egPartnerKey = "6a5873534a6c6b574a795a734579634a4c5253746c4e66496d6a517a626f5643"
//     // let egShopId = 33221984
//     // let egAccessToken = "eyJhbGciOiJIUzI1NiJ9.CO7aehABGODa6w8gASjMvcPOBjCEoZrAAjgBQAFIBw.gWgusgv9Tv5R5bGZJibuS20pWWa05xrRfVEyChhTf4s"
//     // await mainRealtime("Eileen Grace", egPartnerId, egPartnerKey,  egAccessToken, egShopId)

//     // let mdPartnerId = "2010423"
//     // let mdPartnerKey = "64595a4c7368546c7a6276564673645a4c784d74745a6745647a7176455a4278"
//     // let mdShopId = 332381969	
//     // let mdAccessToken = "eyJhbGciOiJIUzI1NiJ9.CLfaehABGJH-vp4BIAEogazMzgYw5qOSwg44AUABSAc.aWtivHpTHxHygeBHvBRgTNPZqY2hj0ClbxuS-BmMX_E"
//     // await mainRealtime("Miss Daisy", mdPartnerId, mdPartnerKey, mdAccessToken, mdShopId);

//     let shrdPartnerId = "2013428"
//     let shrdPartnerKey = "shpk4663436e7a76624c59524742635a55544c7670686a4e6d417465626a4651"
//     let shrdShopId = 167106407
//     let shrdAccessToken = "eyJhbGciOiJIUzI1NiJ9.CPTxehABGOeu108gASim5MzOBjCklpz0DDgBQAFIBw.852t6wfLVZRcQZd1gy3SFnbkDR5RMweNfznXIxK0b9k"
//     await mainRealtime("SHRD", shrdPartnerId, shrdPartnerKey, shrdAccessToken, shrdShopId)
// }

// await testbed();