import axios from 'axios';
import crypto from 'crypto';
import { 
    HOST,
    PARTNER_ID,
    PARTNER_KEY,
    SHOP_ID,
    MD_ACCESS_TOKEN
} from '../../sample-fetch/md_processor.js';

const ORDER_DETAIL_PATH = "/api/v2/order/get_order_detail";

export async function getOrderDetailMD(orderList) {
    const orderIds = orderList.map(order => order.order_sn);
    const orderIdChunks = [];

    for(let i=0; i<orderIds.length; i+=50) {
        orderIdChunks.push(orderIds.slice(i, i+50).join(','));
    }

    try {
        let MDOrdersWithDetail = [];

        for(const orderIdChunk of orderIdChunks) {
            const path = ORDER_DETAIL_PATH;
            const timestamp = Math.floor(Date.now() / 1000);
            const baseString = `${PARTNER_ID}${path}${timestamp}${MD_ACCESS_TOKEN}${SHOP_ID}`;
    
            const sign = crypto.createHmac('sha256', PARTNER_KEY)
                .update(baseString)
                .digest('hex');

            let optional_fields = [
                "actual_shipping_fee",
                "buyer_user_id",
                "buyer_username",
                "estimated_shipping_fee",
                "payment_method",
                "item_list",       
                "pay_time",
                "cancel_reason",
                "cancel_by",
                "package_list",
                "total_amount",     
            ]
            
            const params = new URLSearchParams({
                partner_id: PARTNER_ID,
                timestamp: timestamp,
                access_token: MD_ACCESS_TOKEN,
                shop_id: SHOP_ID,
                sign: sign,
                order_sn_list: orderIdChunk,
                response_optional_fields: optional_fields.join(','),
            });

            const fullUrl = `${HOST}${path}?${params.toString()}`;
            console.log("MD: Hitting Order Detail endpoint: ", fullUrl);

            const response = await axios.get(fullUrl, {
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            if(response && response.data.response && Array.isArray(response.data.response.order_list)) {
                MDOrdersWithDetail = MDOrdersWithDetail.concat(response.data.response.order_list);
                console.log("MD: order detail exists");
            }
        }

        return MDOrdersWithDetail
    } catch (e) {
        console.log("MD: Error getting order detail: ", e);
    }
}