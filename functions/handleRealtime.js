import axios from 'axios';
import crypto from 'crypto';

async function getOrderList(brand, partner_id, partner_key, access_token, shop_id) {
    console.log("[REALTIME-SALES] Handle realtime get order list on brand: ", brand);
    let allOrderSns;
    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/order/get_order_list";

    try {
        const timestamp = Math.floor(Date.now() / 1000);
        const baseString = `${partner_id}${PATH}${timestamp}${access_token}${shop_id}`;
        const sign = crypto.createHmac('sha256', partner_key)
            .update(baseString)
            .digest('hex');
        
        const now = new Date();
        const time_to = Math.floor(now.getTime() / 1000);
        now.setHours(0, 0, 0, 0); 
        const time_from = Math.floor(now.getTime() / 1000);

        let cursor = "";
        let more = true;

        while (more) {
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
                    order_status: 'READY_TO_SHIP',
                    response_optional_fields: 'order_status'
                }
            });

            if (data.error) {
                throw new Error(`Shopee API Error: ${data.message || data.error}`);
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

    } catch (e) {
        console.log("[REALTIME-SALES] Error get order list on brand: ", brand);
        console.log(e);
    }

    return allOrderSns;
}

async function getOrderDetail(brand, batch, partner_id, partner_key, access_token, shop_id) {
    console.log("[REALTIME-SALES] Handle realtime get order detail on brand: ", brand);
    let totalSales;



    return totalSales;
}

export async function mainRealtime(brand, partner_id, partner_key, access_token, shop_id) {
    const allOrderSns = await getOrderList(brand, partner_id, partner_key, access_token, shop_id);
    
    console.log("All order list. First three: ");
    console.log(allOrderSns.slice(0, 3));

    // let batchSize = 50;
    // let totalSalesBrand = 0;
    // for(let i=0; i<allOrderSns.length; i+=batchSize) {
    //     const batchOrderSns = allOrderSns.slice(i, i+batchSize);
    //     const subTotal = await getOrderDetail(brand, batchOrderSns);
    //     totalSalesBrand += subTotal;
    // }
}