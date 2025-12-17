import { formatUnixTime } from '../functions/formatUnixTime.js';
import { BigQuery } from '@google-cloud/bigquery';

const bigquery = new BigQuery();
// This has become the new order_list
export async function writesToChangeLog(orders) {

    const [rows] = await bigquery.query({
        query: `
            SELECT order_sn, status
            FROM \`shopee_api.eileen_grace_order_list\`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY order_sn ORDER BY update_time DESC) = 1
        `
    });
    const lastStatusMap = {};
    rows.forEach(row => {
        lastStatusMap[row.order_sn] = row.status;
    })

    // Filter by change of status
    const ordersLogToWrite = orders
        .filter(order => lastStatusMap[order.order_sn] !== order.order_status)
        .map(order => ({
            order_sn: order.order_sn,
            status: order.order_status,
            update_time: formatUnixTime("writesToChangeLog", order.update_time),
        }));

    console.log("Writing to Eileen Grace Change Log");
    const datasetId = 'shopee_api';
    const tableId = 'eileen_grace_order_list_staging';

    console.log("Writing to Order Change Log - Eileen Grace");

    if(ordersLogToWrite.length > 0) {
        try {
            
            await bigquery
                .dataset(datasetId)
                .table(tableId)
                .insert(ordersLogToWrite);
            console.log(`Inserted ${ordersLogToWrite.length} rows to staging change log`);

            const mergeQuery = `
                MERGE \`shopee_api.eileen_grace_order_list\` T
                USING \`shopee_api.eileen_grace_order_list_staging\` S
                ON T.order_sn = S.order_sn
                WHEN MATCHED THEN
                    UPDATE SET status = S.status, update_time = S.update_time
                WHEN NOT MATCHED THEN
                    INSERT (order_sn, status, update_time)
                    VALUES (S.order_sn, S.status, S.update_time)
            `;
            await bigquery.query({ query: mergeQuery});
            await bigquery.query({ query: `TRUNCATE TABLE \`shopee_api.eileen_grace_order_list_staging\``});
            console.log(`Inserted ${ordersLogToWrite.length} rows to prod change log`);

        } catch (error) {
            if (error.name === 'PartialFailureError' && error.errors && error.errors.length > 0) {
                console.log('Some rows failed to insert into the change log. Details below:');
                error.errors.forEach((errorDetail, index) => {
                    console.log(`\n--- Failure #${index + 1} ---`);
                    // Make sure the row object has order_sn before trying to access it
                    const orderSn = errorDetail.row ? errorDetail.row.order_sn : 'UNKNOWN';
                    console.log(`Problematic Row (order_sn: ${orderSn}):`, JSON.stringify(errorDetail.row, null, 2));
                    console.log(`Error Reasons:`);
                    errorDetail.errors.forEach((err, errIndex) => {
                        console.log(`  - ${errIndex + 1}: ${err.message}`);
                    });
                    console.log('------\n');
                });
            } else {
                console.error("A non-partial failure error occurred:", error);
            }
        }
        console.log("\n");
    } else {
        console.log("No status changes to log.");
    }
}