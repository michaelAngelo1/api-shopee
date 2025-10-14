import { formatUnixTime } from '../functions/formatUnixTime.js';
import { BigQuery } from '@google-cloud/bigquery';

const bigquery = new BigQuery();
export async function handleReturns(orderReturns) {
    console.log("Merging order returns Eileen Grace");

    console.log("First 3 order returns");
    // console.log(JSON.stringify(orderReturns.slice(0, 3), 0, 2));

     const [rows] = await bigquery.query({
        query: `
            SELECT No_Pesanan, Return_Status
            FROM \`shopee_api.eileen_grace_return_refund\`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY No_Pesanan ORDER BY Update_Time DESC) = 1
        `
    });
    const lastStatusMap = {};
    rows.forEach(row => {
        lastStatusMap[row.No_Pesanan] = row.Return_Status;
    });

    const orderReturnsToWrite = orderReturns
        .filter(r => lastStatusMap[r.order_sn] !== r.status)
        .map(r => ({
            No_Pesanan: r.order_sn,
            Return_Id: r.return_sn,
            Return_Status: r.status,
            Jumlah_Item_Return: r.item_returned_qty,
            Daftar_Item: r.item_list.map(item => ({
                Item_Id: item.item_id,
                SKU_Varian: item.variation_sku,
                SKU_Item: item.item_sku,
                Nama_Item: item.name,
                Jumlah_Item: item.amount,
                Harga_Item: item.item_price
            })),
            Jumlah_Refund: r.refund_amount,
            Create_Time: r.create_time,
            Update_Time: r.update_time
        }));
    


    const datasetId = 'shopee_api';
    const tableIdStaging = 'eileen_grace_return_refund_staging';

    try {
        await bigquery
            .dataset(datasetId)
            .table(tableIdStaging)
            .insert(orderReturnsToWrite);
        console.log(`Merged ${orderReturnsToWrite.length} to eileen_grace_return_refund`);

        const mergeQuery = `
                MERGE \`shopee_api.eileen_grace_return_refund\` T
                USING \`shopee_api.eileen_grace_return_refund_staging\` S
                ON T.No_Pesanan = S.No_Pesanan
                WHEN MATCHED THEN
                    UPDATE SET
                        T.No_Pesanan = S.No_Pesanan,
                        T.Return_Status = S.Return_Status,
                        T.Jumlah_Refund = S.Jumlah_Refund,
                        T.Update_Time = S.Update_Time

                -- When it's a new order, insert the entire row
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ROW
            `;
        await bigquery.query({ query: mergeQuery});
        await bigquery.query({ query: `TRUNCATE TABLE \`shopee_api.eileen_grace_return_refund_staging\``});
        console.log(`Inserted ${orderReturnsToWrite.length} rows to eileen_grace_return_refund`);
    } catch (e) {
        console.error("An error occurred during the BigQuery operation.");

        if (e.name === 'PartialFailureError' && e.errors && e.errors.length > 0) {
            console.error("Some rows failed to insert into the staging table. See details below:");

            e.errors.forEach((errorDetail, index) => {
                console.log(`\n--- Failure #${index + 1} ---`);

                console.error("Problematic Row Data:", JSON.stringify(errorDetail.row, null, 2));

                console.error("Error Reasons:");
                errorDetail.errors.forEach(reason => {
                    console.error(`  - [${reason.reason}] ${reason.message}`);
                });
                console.log("----------------------");
            });
        } else {
            console.error("A non-partial or unknown error occurred:", e);
        }
    }
}