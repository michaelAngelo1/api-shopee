import axios from 'axios';
import crypto from 'crypto';

async function getOrderList(brand, partner_id, partner_key, access_token, shop_id) {
    console.log("[REALTIME-SALES] Handle realtime get order list on brand: ", brand);
    let allOrderSns = [];
    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/order/get_order_list";

    const statusesToFetch = ['READY_TO_SHIP', 'PROCESSED', 'SHIPPED', 'COMPLETED', 'IN_CANCEL', 'CANCELLED'];

    try {
        let time_from = Math.floor(new Date("2026-02-12T00:00:00+07:00") / 1000);
        let time_to = Math.floor(new Date("2026-02-12T23:59:59+07:00") / 1000);

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
                        // Double check: Shopee API is sometimes loose with boundaries
                        if (order.create_time >= time_from) {
                            allOrderSns.push(order.order_sn);
                        }
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
}

async function getOrderDetail(brand, batch, partner_id, partner_key, access_token, shop_id) {
    let totalGMV = 0;
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
                response_optional_fields: 'item_list,order_status'
            }
        });

        if (data.error) throw new Error(data.message || data.error);

        if (data.response && data.response.order_list) {
            data.response.order_list.forEach(order => {
                if (order.order_status === 'CANCELLED') return;

                if (order.item_list) {
                    order.item_list.forEach(item => {
                        // Calculate GMV (Gross Merchandise Value)
                        let price = parseFloat(item.model_discounted_price || 0);

                        // Fallback: If bundle deal (price is 0), use original price
                        if (price === 0) {
                            price = parseFloat(item.model_original_price || 0);
                        }

                        const qty = item.model_quantity_purchased || 0;
                        totalGMV += (price * qty);
                    });
                }
            });
        }

    } catch (e) {
        console.log(`[REALTIME-SALES] Detail Error (${brand}): ${e.message}`);
    }

    return totalGMV;
}

export async function mainRealtime(brand, partner_id, partner_key, access_token, shop_id) {
    const allOrderSns = await getOrderList(brand, partner_id, partner_key, access_token, shop_id);
    
    console.log(`[REALTIME-SALES] Total unique orders (Today): ${allOrderSns.length}`);
    if (allOrderSns.length > 0) {
        // Log last 3 to verify we caught the early morning orders
        console.log("Last three (Oldest fetched):", allOrderSns.slice(-3));
    }

    let batchSize = 50;
    let totalSalesBrand = 0;
    
    for(let i = 0; i < allOrderSns.length; i += batchSize) {
        const batchOrderSns = allOrderSns.slice(i, i + batchSize);
        const subTotal = await getOrderDetail(brand, batchOrderSns, partner_id, partner_key, access_token, shop_id);
        totalSalesBrand += subTotal;
    }

    console.log("[REALTIME-SALES] Total GMV on brand: ", brand);
    console.log(totalSalesBrand.toLocaleString('id-ID'));
}