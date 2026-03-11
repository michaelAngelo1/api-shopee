import crypto from 'crypto';
import axios from 'axios';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import { getShopCipher, loadTokens, refreshTokens } from "../auth/tiktokAuthTransaction.js";
import { GoogleSpreadsheet } from 'google-spreadsheet';
import fs from 'fs';
import { JWT } from 'google-auth-library';
const secretClient = new SecretManagerServiceClient();

const secondTransactionBrands = [
    "Mirae",
    "Swissvita",
    "G-Belle",
    "Past Nine",
    "Nutri & Beyond",
    "Ivy & Lily",
    "Naruko",
    "Relove",
    "Joey & Roo",
    "Rocketindo Shop"
];

export async function loadCredentials() {
    const secretName = "projects/231801348950/secrets/realtime-service-account/versions/latest";

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName
        });
        const data = version.payload.data.toString('UTF-8');
        const creds = JSON.parse(data);

        return creds;
    } catch (e) {
        console.error("[MERGE-REALTIME] Error getting service account credentials: ", e);
    }
}

function generateDateRanges(targetMonthStr) {
    const [yearStr, monthStr] = targetMonthStr.split('-');
    const year = parseInt(yearStr, 10);
    const month = parseInt(monthStr, 10); 

    function getMonthRange(y, m) {
        const startDate = new Date(Date.UTC(y, m - 1, 1));
        const endDate = new Date(Date.UTC(y, m, 0));
        return {
            start: startDate.toISOString().split('T')[0],
            end: endDate.toISOString().split('T')[0]
        };
    }

    function getNextMonthBuffer(y, m) {
        const startDate = new Date(Date.UTC(y, m, 1));
        const endDate = new Date(Date.UTC(y, m, 7));
        return {
            start: startDate.toISOString().split('T')[0],
            end: endDate.toISOString().split('T')[0]
        };
    }

    return {
        targetMonth: targetMonthStr,
        withdrawalMonths: [
            getMonthRange(year, month - 1), 
            getMonthRange(year, month)      
        ],
        statementMonths: [
            getMonthRange(year, month - 2), 
            getMonthRange(year, month - 1), 
            getMonthRange(year, month),     
            getNextMonthBuffer(year, month) 
        ]
    };
}

function convertTimestampJakarta(orderCreatedTime) {
    const date = new Date(orderCreatedTime * 1000);
    const utc7Date = new Date(date.getTime() + (7 * 60 * 60 * 1000)); 
    return utc7Date.toISOString().replace('T', ' ').substring(0, 19);
}

async function getWithdrawals(brand, shopCipher, accessToken, monthsToFetch) {
    try {
        let appKey = !secondTransactionBrands.includes(brand) ? "6jalpvras8n00" : "6jbdera7i5q9b";
        let appSecret = !secondTransactionBrands.includes(brand) ? "608e7a9c85afc968d0baa47f6f93258d3ab51949" : "7955bd57fa190f1d6ea27514f45b884192e474f0";
        
        const path = "/finance/202309/withdrawals";
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";
        let rawWithdrawals = [];

        for (const month of monthsToFetch) {
            const createTimeFrom = Math.floor(new Date(`${month.start}T00:00:00+07:00`).getTime() / 1000);
            const createTimeTo = Math.floor(new Date(`${month.end}T23:59:59+07:00`).getTime() / 1000);
            
            let keepFetching = true;
            let currPageToken = "";
            
            while(keepFetching) {
                const timestamp = Math.floor(Date.now() / 1000);
                const queryParams = {   
                    app_key: appKey,
                    create_time_ge: createTimeFrom,
                    create_time_lt: createTimeTo,
                    types: ["WITHDRAW", "SETTLE", "TRANSFER", "REVERSE"].join(','),
                    page_size: 100,
                    timestamp: timestamp,
                    shop_cipher: shopCipher
                };  
                if(currPageToken) queryParams.page_token = currPageToken;
                
                const sortedKeys = Object.keys(queryParams).sort();
                let result = appSecret + path;
                for(const key of sortedKeys) result += key + queryParams[key];
                result += appSecret;

                queryParams.sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');
                
                const response = await axios.get(baseUrl + new URLSearchParams(queryParams).toString(), {
                    headers: { 'content-type': 'application/json', 'x-tts-access-token': accessToken }
                });

                if(response.data.data && response.data.data.withdrawals) {
                    rawWithdrawals.push(...response.data.data.withdrawals);
                }
                const nextPageToken = response.data.data?.next_page_token;
                currPageToken = (nextPageToken && nextPageToken.length > 0) ? nextPageToken : "";
                if(!currPageToken) keepFetching = false;
            }
        }

        return rawWithdrawals.map(r => ({
            withdrawal_id: r.id,
            create_time: convertTimestampJakarta(r.create_time),
            status: r.status,
            type: r.type
        }));
    } catch (e) {
        console.log("[TIKTOK-FINANCE] Error getting withdrawals", e?.response?.data || e);
        return [];
    }
}

async function getStatements(brand, shopCipher, accessToken, monthsToFetch) {
    try {
        let appKey = !secondTransactionBrands.includes(brand) ? "6jalpvras8n00" : "6jbdera7i5q9b";
        let appSecret = !secondTransactionBrands.includes(brand) ? "608e7a9c85afc968d0baa47f6f93258d3ab51949" : "7955bd57fa190f1d6ea27514f45b884192e474f0";
        
        const path = "/finance/202309/statements";
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";
        let rawStatements = [];
        
        for (const month of monthsToFetch) {
            const statementTimeFrom = Math.floor(new Date(`${month.start}T00:00:00+07:00`).getTime() / 1000);
            const statementTimeTo = Math.floor(new Date(`${month.end}T23:59:59+07:00`).getTime() / 1000);
            
            let keepFetching = true;
            let currPageToken = "";

            while(keepFetching) {
                const timestamp = Math.floor(Date.now() / 1000);
                const queryParams = {   
                    app_key: appKey,
                    statement_time_ge: statementTimeFrom,
                    statement_time_lt: statementTimeTo,
                    sort_field: "statement_time",
                    sort_order: "DESC",
                    page_size: 100,
                    timestamp: timestamp,
                    shop_cipher: shopCipher
                };  
                if(currPageToken) queryParams.page_token = currPageToken;
                
                const sortedKeys = Object.keys(queryParams).sort();
                let result = appSecret + path;
                for(const key of sortedKeys) result += key + queryParams[key];
                result += appSecret;

                queryParams.sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');
                
                const response = await axios.get(baseUrl + new URLSearchParams(queryParams).toString(), {
                    headers: { 'content-type': 'application/json', 'x-tts-access-token': accessToken }
                });

                if(response.data.data && response.data.data.statements) {
                    rawStatements.push(...response.data.data.statements);
                }
                const nextPageToken = response.data.data?.next_page_token;
                currPageToken = (nextPageToken && nextPageToken.length > 0) ? nextPageToken : "";
                if(!currPageToken) keepFetching = false;
            }
        }

        return rawStatements.map(r => ({
            statement_id: r.id,
            withdrawal_id: r.payment_id
        }));
    } catch (e) {
        console.log("[TIKTOK-FINANCE] Error getting statements", e?.response?.data || e);
        return [];
    }
}

const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function fetchTransactionsByStatement(brand, statementId, shopCipher, accessToken) {
    let appKey = !secondTransactionBrands.includes(brand) ? "6jalpvras8n00" : "6jbdera7i5q9b";
    let appSecret = !secondTransactionBrands.includes(brand) ? "608e7a9c85afc968d0baa47f6f93258d3ab51949" : "7955bd57fa190f1d6ea27514f45b884192e474f0";
    
    const path = `/finance/202501/statements/${statementId}/statement_transactions`;
    const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";
    
    let keepFetching = true;
    let currPageToken = "";
    let rawTransactions = [];

    while(keepFetching) {
        let attempt = 0;
        let success = false;
        let response;

        while (!success && attempt < 5) {
            try {
                const timestamp = Math.floor(Date.now() / 1000);
                const queryParams = {   
                    app_key: appKey,
                    sort_field: "order_create_time",
                    sort_order: "DESC",
                    page_size: 100,
                    timestamp: timestamp,
                    shop_cipher: shopCipher
                };  
                if(currPageToken) queryParams.page_token = currPageToken;
                
                const sortedKeys = Object.keys(queryParams).sort();
                let result = appSecret + path;
                for(const key of sortedKeys) result += key + queryParams[key];
                result += appSecret;

                queryParams.sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');
                
                response = await axios.get(baseUrl + new URLSearchParams(queryParams).toString(), {
                    headers: { 'content-type': 'application/json', 'x-tts-access-token': accessToken }
                });
                
                success = true; 

            } catch (e) {
                const isRateLimit = e?.response?.status === 429 || e?.response?.data?.message?.includes('Too many requests');
                if (isRateLimit) {
                    attempt++;
                    const backoffTime = Math.pow(2, attempt) * 1000; 
                    console.log(`[RATE LIMIT] Statement ${statementId}. Retrying in ${backoffTime/1000}s (Attempt ${attempt}/5)`);
                    await wait(backoffTime);
                } else {
                    console.log(`[TIKTOK-TRANSACTION] Error on statement ${statementId}`, e?.response?.data?.message || e.message);
                    return rawTransactions; 
                }
            }
        }

        if (!success) {
            console.log(`[FAILED] Statement ${statementId} failed after 5 retries.`);
            return rawTransactions;
        }

        if(response.data.data && response.data.data.transactions) {
            rawTransactions.push(...response.data.data.transactions.map(t => ({ ...t, statement_id: statementId })));
        }
        
        const nextPageToken = response.data.data?.next_page_token;
        currPageToken = (nextPageToken && nextPageToken.length > 0) ? nextPageToken : "";
        if(!currPageToken) keepFetching = false;
    }

    return rawTransactions;
}

function mapToSQLSchema(brand, trx) {
    const val = (path) => Number(path || 0);

    const ssp = val(trx.revenue_breakdown?.subtotal_before_discount_amount);
    const sellerDiscount = val(trx.revenue_breakdown?.seller_discount_amount);

    // if (trx.order_id === '581613468706899197' || trx.adjustment_id === '581613468706899197' || trx.associated_order_id === '581613468706899197') {
    //     console.log("\n--- LIVE FEES FOR 581613468706899197 ---");
    //     const fees = trx.fee_tax_breakdown?.fee || {};
    //     for (const [key, value] of Object.entries(fees)) {
    //         if (Number(value) !== 0) console.log(`${key}: ${value}`);
    //     }
    //     console.log("----------------------------------------\n");
    // }

    return {
        brand: brand,
        order_adjustment_id: trx.type === "ORDER" ? trx.order_id : trx.adjustment_id,
        related_order_id: trx.type === "ORDER" ? String(trx.order_id) : String(trx.adjustment_order_id),
        type: trx.type ? String(trx.type) : null,
        create_time: convertTimestampJakarta(val(trx.order_create_time)),

        SSP: ssp,
        SSP_PSP_Discounts: sellerDiscount,
        PSP: ssp + sellerDiscount,

        Total_fees: val(trx.fee_tax_amount) + val(trx.shipping_cost_amount),
        Platform_commission_fee: val(trx.fee_tax_breakdown?.fee?.platform_commission_amount),
        Flat_fee: val(trx.fee_tax_breakdown?.fee?.fee_per_item_sold_amount),
        Sales_fee: val(trx.fee_tax_breakdown?.fee?.referral_fee_amount),
        Pre_Order_Service_Fee: val(trx.fee_tax_breakdown?.fee?.pre_order_service_fee_amount),
        Mall_service_fee: val(trx.fee_tax_breakdown?.fee?.mall_service_fee_amount),
        Payment_fee: val(trx.fee_tax_breakdown?.fee?.transaction_fee_amount) || val(trx.fee_tax_breakdown?.fee?.credit_card_handling_fee_amount),
        
        Shipping_cost: val(trx.shipping_cost_amount),
        Shipping_costs_passed_on_to_the_logistics_provider: val(trx.shipping_cost_breakdown?.actual_shipping_fee_amount),
        Replacement_shipping_fee_passed_on_to_the_customer: val(trx.shipping_cost_breakdown?.replacement_shipping_fee_amount),
        Exchange_shipping_fee_passed_on_to_the_customer: val(trx.shipping_cost_breakdown?.exchange_shipping_fee_amount),
        Shipping_cost_borne_by_the_platform: val(trx.shipping_cost_breakdown?.supplementary_component?.platform_shipping_fee_discount_amount),
        Shipping_cost_paid_by_the_customer: val(trx.shipping_cost_breakdown?.customer_paid_shipping_fee_amount),
        Refunded_shipping_cost_paid_by_the_customer: val(trx.shipping_cost_breakdown?.supplementary_component?.refunded_customer_shipping_fee_amount),
        Return_shipping_costs_passed_on_to_the_customer: val(trx.shipping_cost_breakdown?.return_shipping_fee_amount),
        Shipping_cost_subsidy: val(trx.shipping_cost_breakdown?.supplementary_component?.shipping_fee_subsidy_amount),
        
        Affiliate_commission: val(trx.fee_tax_breakdown?.fee?.affiliate_commission_amount),
        Affiliate_partner_commission: val(trx.fee_tax_breakdown?.fee?.affiliate_partner_commission_amount),
        Affiliate_Shop_Ads_commission: val(trx.fee_tax_breakdown?.fee?.affiliate_ads_commission_amount),

        // This should be 0
        Affiliate_Shop_Ads_commission_before_PIT: val(trx.fee_tax_breakdown?.fee?.affiliate_commission_amount_before_pit),
        
        Personal_income_tax_withheld_from_affiliate_Shop_Ads_commission: val(trx.fee_tax_breakdown?.tax?.pit_amount),
        Affiliate_Partner_shop_ads_commission: val(trx.fee_tax_breakdown?.fee?.tap_shop_ads_commission),
        
        Shipping_Fee_Program_service_fee: val(trx.fee_tax_breakdown?.fee?.sfp_service_fee_amount),
        Dynamic_Commission: val(trx.fee_tax_breakdown?.fee?.dynamic_commission_amount),
        Bonus_cashback_service_fee: val(trx.fee_tax_breakdown?.fee?.bonus_cashback_service_fee_amount),
        LIVE_Specials_Service_Fee: val(trx.fee_tax_breakdown?.fee?.live_specials_fee_amount),
        Voucher_Xtra_Service_Fee: val(trx.fee_tax_breakdown?.fee?.voucher_xtra_service_fee_amount),
        Order_processing_fee: val(trx.fee_tax_breakdown?.fee?.vn_fix_infrastructure_fee),
        EAMS_Program_service_fee: val(trx.fee_tax_breakdown?.fee?.external_affiliate_marketing_fee_amount),
        Brands_Crazy_Deals_Flash_Sale_service_fee: val(trx.fee_tax_breakdown?.fee?.flash_sales_service_fee_amount),
        Dilayani_Tokopedia_fee: val(trx.fee_tax_breakdown?.fee?.tsp_commission_amount),
        Dilayani_Tokopedia_handling_fee: val(trx.fee_tax_breakdown?.fee?.dt_handling_fee_amount),
        PayLater_program_fee: val(trx.fee_tax_breakdown?.fee?.seller_paylater_handling_fee_amount),
        Campaign_resource_fee: val(trx.fee_tax_breakdown?.fee?.campaign_resource_fee),
        Installation_service_fee: val(trx.fee_tax_breakdown?.fee?.installation_service_fee),
        Ajustment_amount: val(trx.fee_tax_breakdown?.fee?.refund_administration_fee_amount)
    };
}

async function handleTransactionsBreakdown(brand, targetMonth) {
    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;

    await refreshTokens(brand, refreshToken);
    const shopCipher = await getShopCipher(brand, accessToken);
    
    const dates = generateDateRanges(targetMonth);

    const rawWithdrawals = await getWithdrawals(brand, shopCipher, accessToken, dates.withdrawalMonths);
    const settleEventMap = new Map();
    rawWithdrawals.filter(w => w.type === 'SETTLE').forEach(w => {
        settleEventMap.set(String(w.withdrawal_id), w.create_time);
    });

    const rawStatements = await getStatements(brand, shopCipher, accessToken, dates.statementMonths);
    const statementToEarningsMap = new Map();
    rawStatements.forEach(stmt => {
        statementToEarningsMap.set(String(stmt.statement_id), String(stmt.withdrawal_id));
    });

    const statementIds = rawStatements.map(s => s.statement_id);
    console.log(`Total Statements to fetch transactions for: ${statementIds.length}`);

    let allRawTransactions = [];
    const CHUNK_SIZE = 10; 
    
    for (let i = 0; i < statementIds.length; i += CHUNK_SIZE) {
        console.log(`Processing Statement batch ${i} to ${i + CHUNK_SIZE}...`);
        
        const chunk = statementIds.slice(i, i + CHUNK_SIZE);
        const chunkPromises = chunk.map(stmtId => 
            fetchTransactionsByStatement(brand, stmtId, shopCipher, accessToken)
        );

        const chunkResults = await Promise.all(chunkPromises);
        
        chunkResults.forEach(res => {
            if (res && res.length > 0) allRawTransactions.push(...res);
        });

        await new Promise(resolve => setTimeout(resolve, 1500)); 
    }

    const transactionBreakdownList = [];

    allRawTransactions.forEach(trx => {
        const earningsId = statementToEarningsMap.get(String(trx.statement_id));
        const settleTime = settleEventMap.get(earningsId);

        if (settleTime && settleTime.startsWith(targetMonth)) {
            const sqlMappedData = mapToSQLSchema(brand, trx);
            transactionBreakdownList.push(sqlMappedData);
        }
    });

    console.log("Transaction Breakdown List length: ", transactionBreakdownList.length);
    console.log("First three: ");
    console.log(transactionBreakdownList.slice(0, 3));
    console.log("Finished Processing.");    
    
    // Checker: sum of SSP, PSP, and Total_fees
    console.log("Sum of SSP: ", transactionBreakdownList.reduce((i, o) => i + o.SSP, 0))
    console.log("Sum of PSP: ", transactionBreakdownList.reduce((i, o) => i + o.PSP, 0));
    console.log("Sum of Total Fees: ", transactionBreakdownList.reduce((i, o) => i + o.Total_fees, 0))
    // console.log("Specific Order ID: ", transactionBreakdownList.filter(o => o.order_adjustment_id === "582272501493630505"));

    const bqFileData = fs.readFileSync('./orders_from_bigquery_2.json', 'utf8');
    const bqOrders = JSON.parse(bqFileData);

    // 3. Run the comparison
    // compareShippingCosts(transactionBreakdownList, bqOrders);
    // await mergeToSheet("1wDwvbp2hy5XtvRFo_ZcuETFWmiiNRh1Urabsa2wYdQs", "General Checker", transactionBreakdownList);
    // return transactionBreakdownList;
}

async function mainTransactionsBreakdown() {
    const targetMonth = "2026-01";

    await handleTransactionsBreakdown("Eileen Grace", targetMonth);
    // await handleTransactionsBreakdown("Mamaway", targetMonth);
}

// Comment out in deployment.
mainTransactionsBreakdown();