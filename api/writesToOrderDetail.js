import { formatUnixTime } from '../functions/formatUnixTime.js';
import { BigQuery } from '@google-cloud/bigquery';

const bigquery = new BigQuery();
export async function writesToOrderDetail(orders) {

    const [rows] = await bigquery.query({
        query: `
            SELECT order_sn, order_status
            FROM \`shopee_api.eileen_grace_order_detail\`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY order_sn ORDER BY update_time DESC) = 1
        `
    });
    const lastStatusMap = {};
    rows.forEach(row => {
        lastStatusMap[row.order_sn] = row.order_status;
    })

    const orderDetailsToWrite = orders
        .filter(order => lastStatusMap[order.order_sn] !== order.order_status)
        .map(order => ({
                actual_shipping_fee: order.actual_shipping_fee,
                buyer_user_id: order.buyer_user_id,
                buyer_username: order.buyer_username,
                cancel_by: order.cancel_by,
                cancel_reason: order.cancel_reason,
                cod: order.cod,
                create_time: order.create_time,
                days_to_ship: order.days_to_ship,
                estimated_shipping_fee: order.estimated_shipping_fee,
                
                // item_list: order.item_list,
                item_list: order.item_list.map(item => ({
                    item_id: item.item_id,
                    item_name: item.item_name,
                    item_sku: item.item_sku,
                    main_item: item.main_item,
                    model_discounted_price: item.model_discounted_price,
                    model_id: item.model_id,
                    model_name: item.model_name,
                    model_original_price: item.model_original_price,
                    model_quantity_purchased: item.model_quantity_purchased,
                    model_sku: item.model_sku,
                    order_item_id: item.order_item_id,
                })),

                order_sn: order.order_sn,
                order_status: order.order_status,

                package_list: order.package_list.map(item => ({
                    package_number: item.package_number,
                    group_shipment_id: item.group_shipment_id,
                    logistics_status: item.logistics_status,
                    shipping_carrier: item.shipping_carrier,
                    parcel_chargeable_weight_gram: item.parcel_chargeable_weight_gram,
                    item_list: item.item_list.map(subItem => ({
                        item_id: subItem.item_id,
                        model_id: subItem.model_id,
                        model_quantity: subItem.model_quantity,
                        order_item_id: subItem.order_item_id,
                        promotion_group_id: subItem.promotion_group_id,
                        product_location_id: subItem.product_location_id,
                    }))
                })),
            
                pay_time: order.pay_time,
                payment_method: order.payment_method,
                reverse_shipping_fee: order.reverse_shipping_fee,
                ship_by_date: order.ship_by_date,
                total_amount: order.total_amount,
                update_time: formatUnixTime("writesToOrderDetail", order.update_time),
            })
        );
    
    console.log("Writing to Eileen Grace Order Detail");
    const datasetId = 'shopee_api';
    const tableIdStaging = 'eileen_grace_order_detail_staging';

    console.log("Writing to Order Detail - Eileen Grace");

    if(orderDetailsToWrite.length > 0) {
        try {

            const BATCH_SIZE = 500;
            const insertPromises = [];

            for(let i=0; i<orderDetailsToWrite.length; i+=BATCH_SIZE) {
                const chunk = orderDetailsToWrite.slice(i, i+BATCH_SIZE);
                const promise = bigquery
                    .dataset(datasetId)
                    .table(tableIdStaging)
                    .insert(chunk);
                insertPromises.push(promise);
            }

            await Promise.all(insertPromises);

            console.log(`Inserted ${orderDetailsToWrite.length} rows to staging order detail`);

            const mergeQuery = `
                MERGE \`shopee_api.eileen_grace_order_detail\` T
                USING \`shopee_api.eileen_grace_order_detail_staging\` S
                ON T.order_sn = S.order_sn

                -- When an order already exists, update all its fields
                WHEN MATCHED THEN
                    UPDATE SET
                        T.actual_shipping_fee = S.actual_shipping_fee,
                        T.buyer_user_id = S.buyer_user_id,
                        T.buyer_username = S.buyer_username,
                        T.cancel_by = S.cancel_by,
                        T.cancel_reason = S.cancel_reason,
                        T.cod = S.cod,
                        T.create_time = S.create_time,
                        T.days_to_ship = S.days_to_ship,
                        T.estimated_shipping_fee = S.estimated_shipping_fee,
                        T.item_list = S.item_list,
                        T.order_status = S.order_status,
                        T.package_list = S.package_list,
                        T.pay_time = S.pay_time,
                        T.payment_method = S.payment_method,
                        T.reverse_shipping_fee = S.reverse_shipping_fee,
                        T.ship_by_date = S.ship_by_date,
                        T.total_amount = S.total_amount,
                        T.update_time = S.update_time

                -- When it's a new order, insert the entire row
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ROW
            `;
            await bigquery.query({ query: mergeQuery});
            await bigquery.query({ query: `TRUNCATE TABLE \`shopee_api.eileen_grace_order_detail_staging\``});
            console.log(`Inserted ${orderDetailsToWrite.length} rows to prod change log`);
        
        } catch (e) {
            console.error("Error during BigQuery insert/merge:");

            if (e.name === 'PartialFailureError' && e.errors && e.errors.length > 0) {
                console.log('Some rows failed to insert. Details below:');
                e.errors.forEach((errorDetail, index) => {
                    console.log(`\n--- Failure #${index + 1} ---`);
                    console.log(`Problematic Row (order_sn: ${errorDetail.row.order_sn}):`, JSON.stringify(errorDetail.row, null, 2));
                    console.log(`Error Reasons:`);
                    errorDetail.errors.forEach((err, errIndex) => {
                        console.log(`  - ${errIndex + 1}: ${err.message} (Reason: ${err.reason})`);
                    });
                    console.log('------\n');
                });
            } else {
                console.error("A non-partial failure error occurred:", e);
            }
        }
    }
}