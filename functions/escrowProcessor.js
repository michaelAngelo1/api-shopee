import { BigQuery } from '@google-cloud/bigquery';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import axios from 'axios';
import crypto from 'crypto';

export async function fetchDanaDilepas(brand, partner_id, partner_key, access_token, shop_id) {
    console.log("Fetch Dana Dilepas of brand: ", brand);

    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/payment/get_escrow_list";

    try {
        const timestamp = Math.floor(Date.now() / 1000);
        const baseString = `${partner_id}${PATH}${timestamp}${access_token}${shop_id}`;
        const sign = crypto.createHmac('sha256', partner_key)
            .update(baseString)
            .digest('hex');
        
        let count = 0;
        let hasMore = true;
        let pageNumber = 1;
        let escrowContainer = [];

        while(hasMore) {

            const releaseTimeStart = Math.floor(new Date("2026-01-01T00:00:00+07:00") / 1000);
            const releaseTimeEnd = Math.floor(new Date("2026-01-31T23:59:59+07:00") / 1000);
            const params = new URLSearchParams({
                partner_id: partner_id,
                timestamp,
                access_token: access_token,
                shop_id: shop_id,
                sign,
                release_time_from: releaseTimeStart,
                release_time_to: releaseTimeEnd,
                page_size: 100,
                page_no: pageNumber,
            });

            const fullUrl = `${HOST}${PATH}?${params.toString()}`;
            // console.log(`Hitting Dana Dilepas for ${brand}: `, fullUrl, " - page: ", pageNumber);

            const response = await axios.get(fullUrl, {
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            let escrowList = response.data.response.escrow_list;

            // console.log("Escrow list raw response: ", escrowList);

            escrowContainer.push(...escrowList);

            count += escrowList.length;
            hasMore = response.data.response.more;
            pageNumber += 1;
        }

        console.log("[SHOPEE-WITHDRAWAL] Data count: ", count, " on: ", brand);
        return escrowContainer;
    } catch (e) {
        console.log("[SHOPEE-WITHDRAWAL] ERROR on fetching Dana Dilepas on brand: ", brand);
        console.log(e);
    }

    // console.log("All Dana Dilepas on brand: ", brand);
    // console.log("Count: ", danaDilepas.length);
}

async function transformData(data, brand) {
    console.log("Dana Dilepas on brand: ", brand)
    console.log("All Order_Sns on Data before Transform: \n");
    
    try {
        let twentyBatchContainer = [];
        let twentyBatch = [];
        data.forEach(d => {
            
            twentyBatch.push(d.order_sn);
    
            if(twentyBatch.length == 20) {
                twentyBatchContainer.push(twentyBatch);
                twentyBatch = [];
            }
        });
    
        if (twentyBatch.length > 0) {
            twentyBatchContainer.push(twentyBatch);
        }
    
        return twentyBatchContainer;
    } catch (e) {
        console.log("[SHOPEE-WITHDRAWAL] Error on transformData on brand: ", brand);
        console.log(e);
    }
}

async function breakdownEscrow(data, brand, partner_id, partner_key, access_token, shop_id, releaseTimeMap) {
    const HOST = "https://partner.shopeemobile.com";
    const PATH = "/api/v2/payment/get_escrow_detail_batch";

    try {
        let escrowBreakdown = [];
        for(let i=0; i<data.length; i++) {

            const timestamp = Math.floor(Date.now() / 1000);
            const baseString = `${partner_id}${PATH}${timestamp}${access_token}${shop_id}`;
            const sign = crypto.createHmac('sha256', partner_key)
                .update(baseString)
                .digest('hex');
            
            const params = new URLSearchParams({
                partner_id: partner_id,
                timestamp,
                access_token: access_token,
                shop_id: shop_id,
                sign,
            });

            const fullUrl = `${HOST}${PATH}?${params.toString()}`;
            // console.log("Hitting withdrawal URL: ", fullUrl, "on batch: ", i);

            // console.log("Data[i]: ", data[i]);

            const response = await axios.post(fullUrl, {
                "order_sn_list": data[i]
            });

            let escrowDetailList = response.data.response;

            escrowDetailList.forEach(e => {
                const sn = e.escrow_detail.order_sn;
                
                // [NEW] Logic to get and format Tanggal_Dana_Dilepas
                let tanggalDana = null;
                if (releaseTimeMap.has(sn)) {
                    const ts = releaseTimeMap.get(sn);
                    // Convert Unix Seconds to Milliseconds + Add 7 Hours for WIB
                    const dateObj = new Date((ts * 1000) + (7 * 60 * 60 * 1000));
                    // Extract YYYY-MM-DD
                    tanggalDana = dateObj.toISOString().replace('T', ' ').split('.')[0];
                }
                const voucherData = e.escrow_detail.order_income.seller_voucher_code;

                let obj = {
                    "No_Pesanan": sn,
                    "No_Pengajuan": null, 
                    "Username": e.escrow_detail.buyer_user_name,
                    "Waktu_Pesanan_Dibuat": null, 
                    "Metode_pembayaran_pembeli": e.escrow_detail.buyer_payment_info?.buyer_payment_method || null,
                    "Tanggal_Dana_Dilepaskan": tanggalDana, 
                    "Harga_Asli_Produk": e.escrow_detail.order_income.order_original_price,
                    "Total_Diskon_Produk": e.escrow_detail.order_income.order_seller_discount + e.escrow_detail.order_income.shopee_discount,
                    "Diskon_Produk_dari_Penjual": e.escrow_detail.order_income.order_seller_discount,
                    "Jumlah_Pengembalian_Dana_ke_Pembeli": e.escrow_detail.order_income.refund_amount_to_buyer,
                    "Diskon_Produk_dari_Shopee": e.escrow_detail.order_income.shopee_discount,
                    "Diskon_Voucher_Ditanggung_Penjual": e.escrow_detail.order_income.voucher_from_seller,
                    "Cashback_Koin_yang_Ditanggung_Penjual": e.escrow_detail.order_income.seller_coin_cash_back,
                    "Ongkir_Dibayar_Pembeli": e.escrow_detail.buyer_payment_info?.shipping_fee, 
                    "Diskon_Ongkir_Ditanggung_Jasa_Kirim": e.escrow_detail.order_income.shipping_fee_discount_from_3pl,
                    "Gratis_Ongkir_dari_Shopee": e.escrow_detail.order_income.shopee_shipping_rebate,
                    "Ongkir_yang_Diteruskan_oleh_Shopee_ke_Jasa_Kirim": e.escrow_detail.order_income.actual_shipping_fee,
                    "Ongkos_Kirim_Pengembalian_Barang": e.escrow_detail.order_income.reverse_shipping_fee,
                    "Kembali_ke_Biaya_Pengiriman_Pengirim": e.escrow_detail.order_income.final_return_to_seller_shipping_fee,
                    "Biaya_Komisi_AMS": e.escrow_detail.order_income.order_ams_commission_fee,
                    "Biaya_Administrasi_with_PPN_11": e.escrow_detail.order_income.commission_fee,
                    "Biaya_Layanan": e.escrow_detail.order_income.service_fee,
                    "Biaya_Proses_Pesanan": e.escrow_detail.order_income.seller_order_processing_fee,
                    "Biaya_Program_Hemat_Biaya_Kirim": e.escrow_detail.order_income.shipping_seller_protection_fee_amount,
                    "Biaya_Transaksi": e.escrow_detail.order_income.seller_transaction_fee,
                    "Biaya_Kampanye": e.escrow_detail.order_income.campaign_fee,
                    "Bea_Masuk_PPN_PPh": e.escrow_detail.order_income.escrow_tax,
                    "Total_Penghasilan": e.escrow_detail.order_income.escrow_amount,
                    "Kode_Voucher": Array.isArray(voucherData) ? voucherData.join(", ") : null,
                    "Kompensasi": e.escrow_detail.order_income.seller_lost_compensation,
                    "Promo_Gratis_Ongkir_dari_Penjual": e.escrow_detail.order_income.seller_shipping_discount,
                    "Jasa_Kirim": null, 
                    "Nama_Kurir": null, 
                    "Pengembalian_Dana_ke_Pembeli": e.escrow_detail.order_income.refund_amount_to_buyer,
                    "Pro_rata_Koin_yang_Ditukarkan_untuk_Pengembalian_Barang": e.escrow_detail.order_income.prorated_coins_value_offset_return_items,
                    "Pro_rata_Voucher_Shopee_untuk_Pengembalian_Barang": e.escrow_detail.order_income.prorated_shopee_voucher_offset_return_items,
                    "Pro_rated_Bank_Payment_Channel_Promotion_for_return_refund_Items": e.escrow_detail.order_income.prorated_payment_channel_promo_bank_offset_return_items,
                    "Pro_rated_Shopee_Payment_Channel_Promotion_for_return_refund_Items": e.escrow_detail.order_income.prorated_payment_channel_promo_shopee_offset_return_items,
                    "Nama_Toko": null,
                    "Cashback_Koin_dari_Penjual": e.escrow_detail.order_income.seller_coin_cash_back,
                    "Voucher_disponsor_oleh_Penjual": e.escrow_detail.order_income.voucher_from_seller,
                    "Voucher_co_fund_disponsor_oleh_Penjual": null, 
                    "Cashback_Koin_disponsori_Penjual": e.escrow_detail.order_income.seller_coin_cash_back,
                    "Cashback_Koin_Co_fund_disponsori_Penjual": null,
                    "process_dttm": new Date(Date.now() + 7 * 60 * 60 * 1000).toISOString().replace('T', ' ').substring(0, 19)
                }
                escrowBreakdown.push(obj);
            });
        }
        await mergeData(escrowBreakdown, brand);
    } catch (e) {
        console.error("[SHOPEE-WITHDRAWAL] Error getting ESCROW DETAIL BATCH: ", brand);
        console.error(e);
    }
}

const brandTables = {
    "Chess": "chess_finance",
    "Cleviant": "cleviant_finance",
    "Dr.Jou": "dr_jou_finance",
    "Evoke": "evoke_finance",
    "G-Belle": "gbelle_finance",
    "Ivy & Lily": "ivy_lily_finance",
    "Naruko": "naruko_finance",
    "Miss Daisy": "miss_daisy_finance",
    "Mirae": "mirae_finance",
    "Mamaway": "mamaway_finance",
    "Mosseru": "mosseru_finance",
    "Nutri & Beyond": "nutri_beyond_finance",
    "Past Nine": "past_nine_finance",
    "Polynia": "polynia_finance",
    "SH-RD": "shrd_finance",
    "Swissvita": "swissvita_finance",
    "Eileen Grace": "eileen_grace_finance",
    "Relove": "relove_finance",
    "Joey & Roo": "joey_roo_finance",
    "Enchante": "enchante_finance",
    "Rocketindo Shop": "pinkrocket_finance",
}

async function mergeData(data, brand) {
    console.log("[SHOPEE-WITHDRAWAL] Start merging for brand: ", brand);
    const tableName = brandTables[brand];
    const bigquery = new BigQuery();
    const datasetId = 'shopee_api';

    let batch = 1000;
    for(let i=0; i<data.length; i+=batch) {
        const batchData = data.slice(i, i+batch);
        try {
            console.log("[SHOPEE-WITHDRAWAL] Data before merging. First two: ");
            // console.log(data.slice(0, 2));
    
            const incomingOrderSNs = batchData.map(row => `'${row.No_Pesanan}'`).join(",");

            if(!incomingOrderSNs) {
                continue;
            }

            const query = `
                SELECT No_Pesanan 
                FROM \`${bigquery.projectId}.${datasetId}.${tableName}\`
                WHERE No_Pesanan IN (${incomingOrderSNs})
            `;
            const [existingRows] = await bigquery.query({ query });
            
            const existingIds = new Set(existingRows.map(row => row.No_Pesanan));
            console.log(`[SHOPEE-WITHDRAWAL] Found ${existingIds.size} duplicates in BigQuery.`);
    
            const recordsToInsert = batchData.filter(row => !existingIds.has(row.No_Pesanan));
    
            if (recordsToInsert.length === 0) {
                console.log("[SHOPEE-WITHDRAWAL] All data already exists. Skipping insert.");
                continue;
            }
    
            console.log(`[SHOPEE-WITHDRAWAL] Inserting ${recordsToInsert.length} new rows`);
            await bigquery
                .dataset(datasetId)
                .table(tableName)
                .insert(recordsToInsert);
    
            console.log(`[SHOPEE-WITHDRAWAL] Successfully inserted rows for ${brand}.`);
        } catch (e) {
            if (e.name === 'PartialFailureError') {
                console.error("[SHOPEE-WITHDRAWAL] !!! INSERTION FAILED !!!");
                console.error("---------------------------------------------------");
                
                // Log the first 3 errors to avoid flooding the logs
                e.errors.slice(0, 3).forEach((err, index) => {
                    console.error(`Error #${index + 1}:`);
                    console.error("Reason:", JSON.stringify(err.errors, null, 2)); // Shows the specific column & issue
                    console.error("Bad Row Data:", JSON.stringify(err.row, null, 2)); // Shows the data that failed
                    console.error("---------------------------------------------------");
                });
            } else {
                console.error("[SHOPEE-WITHDRAWAL] Unexpected Error:", e);
            }
        }
    }
}

/*** 
TODO:
1. Should hit two endpoints: get_escrow_list and get_escrow_detail per order_sn
2. Transform the data with the required structure
***/
export async function mainDanaDilepas(brand, partner_id, partner_key, access_token, shop_id) {
    const escrowContainer = await fetchDanaDilepas(brand, partner_id, partner_key, access_token, shop_id);
    
    // [NEW] Create a Map: OrderSN -> Release Time (seconds)
    const releaseTimeMap = new Map();
    if (escrowContainer) {
        escrowContainer.forEach(item => {
            releaseTimeMap.set(item.order_sn, item.escrow_release_time);
        });
    }

    const twentyBatchContainer = await transformData(escrowContainer, brand);

    if(twentyBatchContainer && twentyBatchContainer.length > 0) {
        // [NEW] Pass the map to the breakdown function
        await breakdownEscrow(twentyBatchContainer, brand, partner_id, partner_key, access_token, shop_id, releaseTimeMap);
    }
}

