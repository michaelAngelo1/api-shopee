import axios from 'axios';
import crypto from 'crypto';
import { 
    HOST,
    PARTNER_ID,
    PARTNER_KEY,
    SHOP_ID,
    CLEV_ACCESS_TOKEN
} from '../../sample-fetch/clev_processor.js';

const ESCROW_DETAIL_PATH = "/api/v2/payment/get_escrow_detail_batch";

export async function getEscrowDetailCLEV(orderList) {
    const orderIds = orderList.map(order => order.order_sn);
    let length = orderIds.length;
    const orderIdsContainer = [];

    for(let i=0; i<length; i+=50) {
        let chunk = orderIds.slice(i, i+50);
        orderIdsContainer.push(chunk);
    }
    try { 
        let CLEVEscrowsDetail = [];

        for(const orderIdChunk of orderIdsContainer) {
            const path = ESCROW_DETAIL_PATH;
            const timestamp = Math.floor(Date.now() / 1000);
            const baseString = `${PARTNER_ID}${path}${timestamp}${CLEV_ACCESS_TOKEN}${SHOP_ID}`;
            const sign = crypto.createHmac('sha256', PARTNER_KEY)
                .update(baseString)
                .digest('hex');
            
            const params = new URLSearchParams({
                partner_id: PARTNER_ID,
                timestamp: timestamp,
                access_token: CLEV_ACCESS_TOKEN,
                shop_id: SHOP_ID,
                sign: sign,
            });

            const fullUrl = `${HOST}${path}?${params.toString()}`;
            console.log("\nCLEV: Hitting Escrow Detail Batch endpoint:", fullUrl);
            console.log("\n");

            const responseEscrow = await axios.post(fullUrl, {
                "order_sn_list": orderIdChunk
            });

            if(responseEscrow && responseEscrow.data && responseEscrow.data.response) {
                CLEVEscrowsDetail = CLEVEscrowsDetail.concat(responseEscrow.data.response);
            }
        }
        
        return CLEVEscrowsDetail;
    } catch (e) {
        console.log("CLEV: Error fetching escrow detail: ", e);
    }
}
