import { formatUnixTime } from '../../functions/formatUnixTime.js';
import axios from 'axios';
import crypto from 'crypto';
import { 
    HOST,
    PARTNER_ID,
    PARTNER_KEY,
    SHOP_ID,
    SHRD_ACCESS_TOKEN
} from '../../sample-fetch/shrd_processor.js';

const RETURN_LIST_PATH = "/api/v2/returns/get_return_list";

export async function getReturnDetailSHRD(returnList) {
    console.log("SHRD: Get return detail based on return_sn")

    const sanitizedReturnList = returnList.map(r => ({
        order_sn: r.order_sn,
        return_sn: r.return_sn,
        status: r.status,
        item_returned_qty: r.item.reduce((sum, currentItem) => {
            return sum + currentItem.amount;
        }, 0),
        item_list: r.item.map(r => ({
            item_id: r.item_id,
            variant_sku: r.variation_sku,
            item_sku: r.item_sku,
            item_name: r.name,
            item_amount: r.amount,
            item_price: r.item_price
        })),
        refund_amount: r.refund_amount,
        create_time: formatUnixTime("getReturns", r.create_time),
        update_time: formatUnixTime("getReturns", r.update_time)
    }));
    
    return sanitizedReturnList;
}

export async function getReturnListSHRD(timeFrom, timeTo, accessToken) {
    console.log("SHRD: Get return list function");

    const path = RETURN_LIST_PATH;
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}${accessToken}${SHOP_ID}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');

    // Common parameters
    const params = new URLSearchParams({
        partner_id: PARTNER_ID,
        timestamp: timestamp,
        access_token: SHRD_ACCESS_TOKEN,
        shop_id: SHOP_ID,
        sign: sign,
        // Required request parameters
        page_no: 0, 
        page_size: 100,
        create_time_from: timeFrom,
        create_time_to: timeTo
    });

    try {
        // let timeFromIso = formatUnixTime(timeFrom);
        // let timeToIso = formatUnixTime(timeTo);
        let SHRDReturnList = [];

        // Copy-paste this fullUrl in the browser, see if it returns any response
        const fullUrl = `${HOST}${path}?${params.toString()}`;
        console.log("SHRD: Hitting Return List API endpoint: ", fullUrl);

        const response = await axios.get(fullUrl, {
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if(response && response.data && response.data.response) {
            SHRDReturnList = SHRDReturnList.concat(response.data.response.return);
        }

        return SHRDReturnList;
    } catch (e) {
        console.log("SHRD: error getting return list: \n", e);
    }
}