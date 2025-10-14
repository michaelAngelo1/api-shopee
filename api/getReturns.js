import { formatUnixTime } from '../functions/formatUnixTime.js';
import axios from 'axios';
import crypto from 'crypto';
import {
    HOST,
    PARTNER_ID,
    PARTNER_KEY,
    SHOP_ID,
    ACCESS_TOKEN
} from '../processor.js';

const RETURN_LIST_PATH = "/api/v2/returns/get_return_list";

export async function getReturnDetail(returnList) {
    console.log("Get return detail based on return_sn")
    // Harus nembak berkali-kali cos this is not a batch. One response per id. 
    // Must contain in an array, then return it.

    // Reconfigure here. Must include: order_sn, return_sn, and returned_quantity

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

    // Need to calculate item_returned_quantity by summing all "amount" on the item_returned array

    return sanitizedReturnList;
}

export async function getReturnList(timeFrom, timeTo) {
    console.log("Get return list function");

    // Common Parameters:
    // - partner_id
    // - timestamp
    // - access_token
    // - shop_id
    // - sign

    // Request Parameters:
    // - page_no (required)
    // - page_size (required)
    // - create_time_from (required for this case)
    // - create_time_to (required for this case)
    // - update_time_from
    // - update_time_to
    // - status
    // - negotiation_status
    // - seller_proof_status
    // - seller_compensation_status

    let customTimeFrom = 1754529907;
    let customTimeTo = 1755739507;

    const path = RETURN_LIST_PATH;
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}${ACCESS_TOKEN}${SHOP_ID}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');

    // Common parameters
    const params = new URLSearchParams({
        partner_id: PARTNER_ID,
        timestamp: timestamp,
        access_token: ACCESS_TOKEN,
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
        let allReturnList = [];

        // Copy-paste this fullUrl in the browser, see if it returns any response
        const fullUrl = `${HOST}${path}?${params.toString()}`;
        console.log("Hitting Return List API endpoint: ", fullUrl);

        const response = await axios.get(fullUrl, {
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if(response && response.data && response.data.response) {
            allReturnList = allReturnList.concat(response.data.response.return);
        }

        return allReturnList;
    } catch (e) {
        console.log("error getting return list: \n", e);
    }
}