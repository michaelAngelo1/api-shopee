import { BigQuery } from '@google-cloud/bigquery';
import { formatUnixTime } from '../functions/formatUnixTime.js';

const bigquery = new BigQuery();

export async function handleOrders(orderDetails, orderEscrows) {
    console.log("Wrangling order details, escrows, and returns. From October 1st to yesterday.");


    console.log(`Total orderDetails received: ${orderDetails.length}`);
    console.log(`Total orderEscrows received: ${orderEscrows.length}`);

    const escrowSnSet = new Set(orderEscrows.map(e => e.escrow_detail.order_sn));

    const mismatchedOrders = orderDetails.filter(o => !escrowSnSet.has(o.order_sn));

    if (mismatchedOrders.length > 0) {
        console.error(`\nCRITICAL DATA MISMATCH: Found ${mismatchedOrders.length} orders that are missing their escrow details.`);
        console.error("This confirms the API fetch for escrows is incomplete. Please check your pagination logic (next_cursor).");
        const sampleMismatchedIds = mismatchedOrders.slice(0, 5).map(o => o.order_sn);
        console.error("Sample of missing order_sn:", sampleMismatchedIds);
    } else {
        console.log("\nData integrity check passed: All orders have a corresponding escrow detail.");
    }

    const sampleOrderObjectList = [];
    
    orderDetails.forEach(o => {
        
        const escrow = orderEscrows.find(e => e.escrow_detail.order_sn === o.order_sn);

        if(!escrow || !escrow.escrow_detail.order_income || !escrow.escrow_detail.order_income.items) {
            console.log(`Couldn't find escrow for order_sn: ${o.order_sn}`);
            return;
        }

        const jumlahProdukDiPesan = escrow.escrow_detail.order_income.items.length;

        const totalOrderWeight = o.item_list.reduce((sum, currentItem) => {
            return sum + (currentItem.weight * currentItem.model_quantity_purchased);
        }, 0);

        if(o.item_list.length > 1) {
            for(let i=0; i<o.item_list.length; i++) {

                const escrowItem = escrow.escrow_detail.order_income.items[i];

                const sampleOrderObject = {
                    "No_Pesanan": o.order_sn,
                    "Status_Pesanan": o.order_status,
                    "Alasan_Pembatalan": o.cancel_reason,
                    "No_Resi": o.package_number,
                    "Opsi_Pengiriman": o.shipping_carrier,
                    "Pesanan_Harus_Dikirimkan_Sebelum": formatUnixTime("processor - ship by date", o.ship_by_date),
                    "Waktu_Pesanan_Dibuat": formatUnixTime("processor - create time", o.create_time),
                    "Waktu_Pembayaran_Dilakukan": o.pay_time ? formatUnixTime("processor - pay time", o.pay_time) : "BELUM BAYAR",
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

            const escrowItem = escrow.escrow_detail.order_income.items[0];

            const sampleOrderObject = {
                "No_Pesanan": o.order_sn,
                "Status_Pesanan": o.order_status,
                "Alasan_Pembatalan": o.cancel_reason,
                "Status_Pembatalan_Pengembalian": "",
                "No_Resi": o.package_number,
                "Opsi_Pengiriman": o.shipping_carrier,
                "Pesanan_Harus_Dikirimkan_Sebelum": formatUnixTime("processor - ship by date", o.ship_by_date),
                "Waktu_Pesanan_Dibuat": formatUnixTime("processor - create time", o.create_time),
                "Waktu_Pembayaran_Dilakukan": o.pay_time ? formatUnixTime("processor - pay time", o.pay_time) : "BELUM BAYAR",
                "Metode_Pembayaran": o.payment_method ? o.payment_method : "BELUM BAYAR",
                "SKU_Induk": o.item_list[0].item_sku,
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
        console.log("Sample Order Object List\n");
        console.log(sampleOrderObjectList.slice(0, 3));
        console.log(JSON.stringify(sampleOrderObjectList.slice(0, 3), null, 2));
    }

    const orderObjectList = [];
    const orderObject = {
        "No_Pesanan": "",
        "Status_Pesanan": "",
        "Alasan_Pembatalan": "",
        "Status_Pembatalan_Pengembalian": "",
        "No_Resi": "",
        "Opsi_Pengiriman": "",
        "Pesanan_Harus_Dikirimkan_Sebelum": "",
        "Waktu_Pesanan_Dibuat": "",
        "Waktu_Pembayaran_Dilakukan": "",
        "Metode_Pembayaran": "",
        "SKU_Induk": "",
        "Nama_Produk": "",
        "Nomor_Referensi_SKU": "",
        "Nama_Variasi": "",
        "Harga_Awal": 0,
        "Harga_Setelah_Diskon": 0,
        "Jumlah": 0,
        "Returned_quantity": 0,
        "Total_Harga_Produk": 0,
        "Total_Diskon": 0,
        "Diskon_Dari_Penjual": 0,
        "Diskon_Dari_Shopee": 0,
        "Jumlah_Produk_di_Pesan": 0,
        "Total_Berat": "",
        "Voucher_Ditanggung_Penjual": 0,
        "Voucher_Ditanggung_Shopee": 0,
        "Total_Pembayaran": 0,
        "Perkiraan_Ongkos_Kirim": 0,
    }
}