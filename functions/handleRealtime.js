import axios from 'axios';
import crypto from 'crypto';

// 1. Calculate Jakarta Midnight ONCE globally to ensure consistency
// Jakarta is UTC+7 (25200 seconds)
const nowSeconds = Math.floor(Date.now() / 1000);
const jakartaOffset = 25200; 
const secondsPassedToday = (nowSeconds + jakartaOffset) % 86400;
const JAKARTA_MIDNIGHT_TS = nowSeconds - secondsPassedToday;

async function getOrderList(brand, partner_id, partner_key, access_token, shop_id) {
    console.log("[REALTIME-SALES] Handle realtime get order list on brand: ", brand);
    let allOrderSns = [];
    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/order/get_order_list";

    // Removed INVOICE_PENDING (Invalid) and UNPAID
    const statusesToFetch = ['READY_TO_SHIP', 'PROCESSED', 'SHIPPED', 'COMPLETED', 'IN_CANCEL', 'CANCELLED'];

    try {
        // Use the Jakarta Midnight timestamp we calculated
        const time_from = JAKARTA_MIDNIGHT_TS;
        const time_to = nowSeconds; 

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
                        time_range_field: 'create_time',
                        time_from: time_from,
                        time_to: time_to,
                        page_size: 100,
                        cursor,
                        order_status: status,
                        // REMOVED response_optional_fields completely for list
                        // create_time is NOT supported here, and we don't need order_status here
                    }
                });

                if (data.error) {
                    console.log(`[REALTIME-SALES] API Skip [${status}]: ${data.message || data.error}`);
                    break;
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
                // FIX: Only request item_list. 
                // create_time and order_status are returned BY DEFAULT, so don't request them.
                response_optional_fields: 'item_list'
            }
        });

        if (data.error) throw new Error(data.message || data.error);

        if (data.response && data.response.order_list) {
            data.response.order_list.forEach(order => {
                if (order.create_time < JAKARTA_MIDNIGHT_TS) return;

                // 2. FILTER: Ignore Cancelled
                // if (order.order_status === 'CANCELLED') return;

                if (order.item_list) {
                    order.item_list.forEach(item => {
                        let price = parseFloat(item.model_discounted_price || 0);

                        // 3. FIX: Bundle Deal 0 Price Fallback
                        if (price === 0) {
                            console.log("[RS-DEBUG] Possible bundle deal: ", order.order_sn);
                            console.log("[RS-DEBUG] Bundle discounted price: ", price);
                            console.log("[RS-DEBUG] Bundle model original price: ", item.model_original_price);
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
    
    console.log(`[REALTIME-SALES] Total orders fetched: ${allOrderSns.length}`);
    
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