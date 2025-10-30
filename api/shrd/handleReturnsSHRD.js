import { BigQuery } from '@google-cloud/bigquery';

const bigquery = new BigQuery();

export async function handleReturnsSHRD(orderReturns) {
    console.log("SHRD: Merging order returns Miss Daisy");

    const latestReturnsMap = {};
    for (const returnItem of orderReturns) {
        const existing = latestReturnsMap[returnItem.order_sn];
        if (!existing || returnItem.update_time > existing.update_time) {
            latestReturnsMap[returnItem.order_sn] = returnItem;
        }
    }
    const uniqueLatestReturns = Object.values(latestReturnsMap);
    console.log(`SHRD: Received ${orderReturns.length} raw returns, de-duplicated to ${uniqueLatestReturns.length} unique latest returns.`);

    const [rows] = await bigquery.query({
        query: `
            SELECT No_Pesanan, Return_Status
            FROM \`shopee_api.shrd_return_refund\`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY No_Pesanan ORDER BY Update_Time DESC) = 1
        `
    });
    const lastStatusMap = {};
    rows.forEach(row => {
        lastStatusMap[row.No_Pesanan] = row.Return_Status;
    });

    const orderReturnsToWrite = uniqueLatestReturns
        .filter(r => lastStatusMap[r.order_sn] !== r.status)
        .map(r => ({
            No_Pesanan: r.order_sn,
            Return_Id: r.return_sn,
            Return_Status: r.status,
            Jumlah_Item_Return: r.item_returned_qty,
            Daftar_Item: r.item_list.map(item => ({
                Item_Id: item.item_id,
                SKU_Varian: item.variant_sku,
                SKU_Item: item.item_sku,
                Nama_Item: item.item_name,
                Jumlah_Item: item.item_amount,
                Harga_Item: item.item_price
            })),
            Jumlah_Refund: r.refund_amount,
            Create_Time: r.create_time,
            Update_Time: r.update_time
        }));

    const datasetId = 'shopee_api';
    const tableIdStaging = 'shrd_return_refund_staging';

    if (orderReturnsToWrite.length === 0) {
        console.log("SHRD: No new or updated returns to process.");
        return; // Exit the function early
    }

    try {
        await bigquery
            .dataset(datasetId)
            .table(tableIdStaging)
            .insert(orderReturnsToWrite);
        console.log(`SHRD: Inserted ${orderReturnsToWrite.length} shrd_return_refund_staging`);

        const mergeQuery = `
            MERGE \`shopee_api.shrd_return_refund\` T
            USING (
              SELECT * FROM \`shopee_api.shrd_return_refund_staging\`
              QUALIFY ROW_NUMBER() OVER(PARTITION BY No_Pesanan ORDER BY Update_Time DESC) = 1
            ) S
            ON T.No_Pesanan = S.No_Pesanan
            WHEN MATCHED THEN
                UPDATE SET
                    Return_Status = S.Return_Status,
                    Jumlah_Refund = S.Jumlah_Refund,
                    Update_Time = S.Update_Time
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (No_Pesanan, Return_Id, Return_Status, Jumlah_Item_Return, Daftar_Item, Jumlah_Refund, Create_Time, Update_Time)
                VALUES (S.No_Pesanan, S.Return_Id, S.Return_Status, S.Jumlah_Item_Return, S.Daftar_Item, S.Jumlah_Refund, S.Create_Time, S.Update_Time);
        `;
        await bigquery.query({ query: mergeQuery});
        await bigquery.query({ query: `TRUNCATE TABLE \`shopee_api.shrd_return_refund_staging\``});
        console.log(`SHRD HandleReturns: Inserted ${orderReturnsToWrite.length} rows to shrd_return_refund`);
    } catch (e) {
        console.error("SHRD HandleReturns: An error occurred during the BigQuery operation.");

        if (e.name === 'PartialFailureError' && e.errors && e.errors.length > 0) {
            console.error("SHRD HandleReturns: Some rows failed to insert into the staging table. See details below:");

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
            console.error("SHRD HandleReturns: A non-partial or unknown error occurred:", e);
        }
    }
}