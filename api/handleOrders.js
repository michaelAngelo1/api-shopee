import { BigQuery } from '@google-cloud/bigquery';
import { formatUnixTime } from '../functions/formatUnixTime.js';

const bigquery = new BigQuery();

export async function handleOrders(orderDetails, orderEscrows) {
    console.log("Wrangling order details, escrows, and returns. From October 1st to yesterday.");

    const escrowMap = new Map();
    orderEscrows.forEach(e => {
        if(e.escrow_detail && e.escrow_detail.order_sn) {
            escrowMap.set(e.escrow_detail.order_sn, e.escrow_detail);
        }
    });

    console.log(`Total orderDetails received: ${orderDetails.length}`);
    console.log(`Total orderEscrows received: ${escrowMap.size}`);

    const sampleOrderObjectList = [];
    
    orderDetails.forEach(o => {
        
        // const escrow = orderEscrows.find(e => e.escrow_detail.order_sn === o.order_sn);
        const escrowDetail = escrowMap.get(o.order_sn);

        if(!escrowDetail || !escrowDetail.order_income || !escrowDetail.order_income.items) {
            console.log(`Couldn't find escrow for order_sn: ${o.order_sn}`);
            return;
        }

        const jumlahProdukDiPesan = escrowDetail.order_income.items.length;

        const totalOrderWeight = o.item_list.reduce((sum, currentItem) => {
            return sum + (currentItem.weight * currentItem.model_quantity_purchased);
        }, 0);

        if(o.item_list.length > 1) {
            for(let i=0; i<o.item_list.length; i++) {

                const escrowItem = escrowDetail.order_income.items[i];

                const sampleOrderObject = {
                    "No_Pesanan": o.order_sn,
                    "Status_Pesanan": o.order_status,
                    "Alasan_Pembatalan": o.cancel_reason,
                    "No_Resi": o.package_list?.[0]?.package_number ?? "",
                    "Opsi_Pengiriman": o.package_list?.[0]?.shipping_carrier ?? "",
                    "Pesanan_Harus_Dikirimkan_Sebelum": o.ship_by_date ? formatUnixTime("processor - ship by date", o.ship_by_date) : null,
                    "Waktu_Pesanan_Dibuat": formatUnixTime("processor - create time", o.create_time),
                    "Waktu_Pembayaran_Dilakukan": o.pay_time ? formatUnixTime("processor - pay time", o.pay_time) : null,
                    "Metode_Pembayaran": o.payment_method ? o.payment_method : "BELUM BAYAR",
                    "SKU_Induk": o.item_list[i].item_sku,
                    "SKU_Varian": o.item_list[i].model_sku,
                    "Nama_Produk": o.item_list[i].item_name,
                    "Nomor_Referensi_SKU": o.item_list[i].item_sku,
                    "Nama_Variasi": o.item_list[i].model_name,
                    "Harga_Awal": o.item_list[i].model_original_price,
                    "Harga_Setelah_Diskon": o.item_list[i].model_discounted_price,
                    "Jumlah": o.item_list[i].model_quantity_purchased,
                    "Total_Harga_Produk": o.item_list[i].model_discounted_price * o.item_list[i].model_quantity_purchased, 
                    "Total_Diskon": escrowItem.seller_discount + escrowItem.shopee_discount,
                    "Diskon_Dari_Penjual": escrowItem.seller_discount,
                    "Diskon_Dari_Shopee": escrowItem.shopee_discount,
                    "Jumlah_Produk_di_Pesan": jumlahProdukDiPesan,
                    "Total_Berat": totalOrderWeight,
                    "Voucher_Ditanggung_Penjual": escrowItem.discount_from_voucher_seller,
                    "Voucher_Ditanggung_Shopee": escrowItem.discount_from_voucher_shopee,
                    "Total_Pembayaran": o.total_amount,
                    "Perkiraan_Ongkos_Kirim": o.estimated_shipping_fee
                }
                sampleOrderObjectList.push(sampleOrderObject);
            }
        } else {

            const escrowItem = escrowDetail.order_income.items[0];

            const sampleOrderObject = {
                "No_Pesanan": o.order_sn,
                "Status_Pesanan": o.order_status,
                "Alasan_Pembatalan": o.cancel_reason,
                "No_Resi": o.package_list?.[0]?.package_number ?? "",
                "Opsi_Pengiriman": o.package_list?.[0]?.shipping_carrier ?? "",
                "Pesanan_Harus_Dikirimkan_Sebelum": o.ship_by_date ? formatUnixTime("processor - ship by date", o.ship_by_date) : null,
                "Waktu_Pesanan_Dibuat": formatUnixTime("processor - create time", o.create_time),
                "Waktu_Pembayaran_Dilakukan": o.pay_time ? formatUnixTime("processor - pay time", o.pay_time) : null,
                "Metode_Pembayaran": o.payment_method ? o.payment_method : "BELUM BAYAR",
                "SKU_Induk": o.item_list[0].item_sku,
                "SKU_Varian": o.item_list[0].model_sku,
                "Nama_Produk": o.item_list[0].item_name,
                "Nomor_Referensi_SKU": o.item_list[0].item_sku,
                "Nama_Variasi": o.item_list[0].model_name,
                "Harga_Awal": o.item_list[0].model_original_price,
                "Harga_Setelah_Diskon": o.item_list[0].model_discounted_price,
                "Jumlah": o.item_list[0].model_quantity_purchased,
                "Total_Harga_Produk": o.item_list[0].model_discounted_price * o.item_list[0].model_quantity_purchased,  
                "Total_Diskon": escrowItem.seller_discount + escrowItem.shopee_discount,
                "Diskon_Dari_Penjual": escrowItem.seller_discount,
                "Diskon_Dari_Shopee": escrowItem.shopee_discount,
                "Jumlah_Produk_di_Pesan": jumlahProdukDiPesan,
                "Total_Berat": totalOrderWeight,
                "Voucher_Ditanggung_Penjual": escrowItem.discount_from_voucher_seller,
                "Voucher_Ditanggung_Shopee": escrowItem.discount_from_voucher_shopee,
                "Total_Pembayaran": o.total_amount,
                "Perkiraan_Ongkos_Kirim": o.estimated_shipping_fee
            }
    
            sampleOrderObjectList.push(sampleOrderObject);
        }

    });

    if(sampleOrderObjectList && sampleOrderObjectList.length > 0) {
        console.log("Passing orderObjectList to mergeOrders \n");

        await mergeOrders(sampleOrderObjectList);
    }
}

async function mergeOrders(orders) {
    console.log("Orders to Merge");

    console.log(JSON.stringify(orders.slice(0, 5), null, 2));

    try {
        const datasetId = 'shopee_api';
        const tableNameStaging = 'eileen_grace_orders_staging';
        const insertPromises = [];
        const batchSize = 500;

        for(let i=0; i<orders.length; i+=batchSize) {
            const chunk = orders.slice(i, i+batchSize);
            const promise = bigquery
                .dataset(datasetId)
                .table(tableNameStaging)
                .insert(chunk);
            insertPromises.push(promise);
        }
        await Promise.all(insertPromises);
        console.log("Successfully written orders to eileen_grace_orders_staging");

        const mergeQuery = `
            MERGE \`shopee_api.eileen_grace_orders\` T
            USING \`shopee_api.eileen_grace_orders_staging\` S
            ON T.No_Pesanan = S.No_Pesanan
            WHEN MATCHED THEN
                UPDATE SET
                    T.Status_Pesanan = S.Status_Pesanan,
                    T.Alasan_Pembatalan = S.Alasan_Pembatalan,
                    T.Waktu_Pembayaran_Dilakukan = S.Waktu_Pembayaran_Dilakukan
            WHEN NOT MATCHED THEN
                INSERT ROW
        `;

        await bigquery.query({ query: mergeQuery });
        await bigquery.query({ query: `TRUNCATE TABLE \`shopee_api.eileen_grace_orders_staging\`` });
        console.log(`Inserted ${orders.length} rows to eileen_grace_orders`);
    } catch (e) {
        console.error("An error occurred during the BigQuery operation.");

        if (e.name === 'PartialFailureError' && e.errors && e.errors.length > 0) {
            console.error("Some rows failed to insert into the staging table. See details below:");

            // Limit logging to the first 5 failures to avoid flooding the console
            e.errors.slice(0, 5).forEach((errorDetail, index) => {
                console.log(`\n--- Failure #${index + 1} ---`);
                console.error("Problematic Row Data:", JSON.stringify(errorDetail.row, null, 2));
                console.error("Error Reasons:");
                
                // --- THIS IS THE ENHANCED PART ---
                errorDetail.errors.forEach(reason => {
                    // Log the reason, the problematic field (location), and the message
                    console.error(`  - Field: [${reason.location || 'UNKNOWN'}] | Reason: [${reason.reason}] | Message: ${reason.message}`);
                });
                console.log("----------------------");
            });
        } else {
            console.error("A non-partial or unknown error occurred:", e);
        }
    }
}

