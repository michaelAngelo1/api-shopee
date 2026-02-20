import axios from 'axios';
import crypto from 'crypto';
import { handleMergeRealtime } from './handleMergeRealtime.js';
import 'dotenv/config';

async function getOrderList(brand, partner_id, partner_key, access_token, shop_id) {
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
        const nowSeconds = Math.floor(Date.now() / 1000);
        const jakartaOffset = 25200; 
        const secondsPassedToday = (nowSeconds + jakartaOffset) % 86400;
        const JAKARTA_MIDNIGHT_TS = nowSeconds - secondsPassedToday;
        // Use the Jakarta Midnight timestamp we calculated
        const time_from = JAKARTA_MIDNIGHT_TS;
        const time_to = nowSeconds; 

        // TESTING. DELETE LATER
        // const time_from = JAKARTA_MIDNIGHT_TS - 86400; 
        // const time_to = JAKARTA_MIDNIGHT_TS - 1;
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

async function getOrderDetail(brand, batch, partner_id, partner_key, access_token, shop_id) {
    let totalGMV = 0;
    let orderSnForEscrow = [];
    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/order/get_order_detail";

    try {
        // 1. Calculate Jakarta Midnight ONCE globally to ensure consistency
        // Jakarta is UTC+7 (25200 seconds)
        const nowSeconds = Math.floor(Date.now() / 1000);
        const jakartaOffset = 25200; 
        const secondsPassedToday = (nowSeconds + jakartaOffset) % 86400;
        const JAKARTA_MIDNIGHT_TS = nowSeconds - secondsPassedToday;

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
                if (order.payment_method !== 'Cash on Delivery') {
                    // Non-COD must be PAID today
                    if (order.pay_time && order.pay_time >= JAKARTA_MIDNIGHT_TS) {
                        isTargetDate = true;
                    }
                } else {
                    // COD must be CREATED today
                    if (order.create_time && order.create_time >= JAKARTA_MIDNIGHT_TS) {
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
                        
                        if (price === 0) {
                            price = parseFloat(item.model_original_price || 0);
                            console.log(`[TRAP] Bundle Deal Fallback on ${order.order_sn}: Overcounting by using full price Rp ${price}`);
                        }

                        
                        const qty = item.model_quantity_purchased || 0;
                        let itemTotal = (price * qty);
                        
                        if (order.order_status === 'CANCELLED') {
                            console.log(`[GHOST CAUGHT] Cancelled Order added to GMV: ${order.order_sn} | Value: Rp ${itemTotal} | COD: ${order.payment_method === 'Cash on Delivery'}`);
                        }
                        
                        orderTotal += itemTotal;
                        // console.log("Total GMV running total: ", totalGMV, " for brand: ", brand);
                        orderSnForEscrow.push(order.order_sn);
                    });
                    // console.log("Order sn: ", order.order_sn, " order status: ", order.order_status, " order value: ", orderTotal, " payment method: ", order.payment_method);
                    totalGMV += orderTotal;
                }
            });
        }

    } catch (e) {
        console.log(`[REALTIME-SALES] Detail Error (${brand}): ${e.message}`);
    }

    let voucherFromSellerTotal = 0;
    let batchSize = 20;

    for(let i=0; i<orderSnForEscrow.length; i+=batchSize) {
        const batchOrderSns = orderSnForEscrow.slice(i, i+batchSize);
        const voucherFromSellerBatch = await getEscrowDetailBatch(brand, batchOrderSns, partner_id, partner_key, access_token, shop_id);
        voucherFromSellerTotal += voucherFromSellerBatch;
    }

    // console.log("Voucher from seller on brand: ", brand, " per batch: ", voucherFromSellerTotal);

    return totalGMV - voucherFromSellerTotal;
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
                // console.log("Voucher from seller: ", e.escrow_detail.order_income.voucher_from_seller);
                voucherFromSellerTotal += e.escrow_detail.order_income.voucher_from_seller;
            })
        }
    } catch (e) {
        console.log("Error getting escrow detail batch: ", e);
    }

    return voucherFromSellerTotal;
}

export async function mainRealtime(brand, partner_id, partner_key, access_token, shop_id) {
    const allOrderSns = await getOrderList(brand, partner_id, partner_key, access_token, shop_id);
    
    console.log(`[REALTIME-SALES] Total ${brand} orders fetched: ${allOrderSns.length}`);
    
    let batchSize = 50;
    let totalSalesBrand = 0;

    // allOrderSns.forEach(a => {
    //     console.log("order: ", a);
    // })

    // console.log('Three earliest orders: ');
    // console.log(allOrderSns.slice(0, 3));

    // console.log("Three latest orders: ");
    // console.log(allOrderSns.slice(-3));
    
    for(let i = 0; i < allOrderSns.length; i += batchSize) {
        const batchOrderSns = allOrderSns.slice(i, i + batchSize); // Batch order sns here is still unclean. getOrderDetail helps filtering it. 
        const subTotal = await getOrderDetail(brand, batchOrderSns, partner_id, partner_key, access_token, shop_id); // Should get a clean GMV, after voucher from seller. 
        totalSalesBrand += subTotal;
    }

    console.log("[REALTIME-SALES] Total GMV on brand: ", brand);
    console.log(totalSalesBrand.toLocaleString('id-ID'));

    let marketplace = "Shopee";
    // await handleMergeRealtime(brand, marketplace, totalSalesBrand);
}
