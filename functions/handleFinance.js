import 'dotenv/config';
import crypto from 'crypto';
import axios from 'axios';
import { BigQuery } from '@google-cloud/bigquery';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import { loadTokens, refreshTokens, getShopCipher } from '../auth/tiktokAuth.js';

function convertTimestampJakarta(orderCreatedTime) {
    const date = new Date(orderCreatedTime * 1000);
    const utc7Date = new Date(date.getTime() + (7 * 60 * 60 * 1000)); 
    const isoString = utc7Date.toISOString();
    const result = isoString.replace('T', ' ').substring(0, 19);
    return result;
}

function convertTimestamp(orderCreatedTime) {
    if (!orderCreatedTime || isNaN(orderCreatedTime)) {
        return null; 
    }
    const date = new Date(orderCreatedTime * 1000);
    if (isNaN(date.getTime())) {
        return null; 
    }
    return date.toISOString().replace('T', ' ').substring(0, 19);
}

async function getWithdrawals(brand, shopCipher, accessToken) {
    try {
        const appKey = "6j6u4kmpdda19"
        const appSecret = "c4680b9ff6797160adb92104a77e2e1aa085c733"
        
        const path = "/finance/202309/withdrawals";
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";
        const createTimeFrom = Math.floor(new Date("2025-10-01T00:00:00+07:00").getTime() / 1000);
        const createTimeTo = Math.floor(new Date("2025-10-31T23:59:59+07:00").getTime() / 1000);
        
        let keepFetching = true;
        let currPageToken = "";
        
        let rawWithdrawals = [];

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
            if(currPageToken) {
                queryParams.page_token = currPageToken;
            }
            const sortedKeys = Object.keys(queryParams).sort();

            let result = appSecret + path;
            for(const key of sortedKeys) {
                result += key + queryParams[key];
            }
            result += appSecret;

            const sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');
            queryParams.sign = sign;
            const querySearchParams = new URLSearchParams(queryParams);

            const completeUrl = baseUrl + querySearchParams.toString();
            const response = await axios.get(completeUrl, {
                headers: {
                    'content-type': 'application/json',
                    'x-tts-access-token': accessToken,
                }
            });

            // console.log("[TIKTOK-FINANCE] Raw response: ", response.data.data.withdrawals);
            rawWithdrawals.push(...response.data.data.withdrawals);

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        }

        const formattedWithdrawals = rawWithdrawals.map(r => {
            let obj = {}
            obj.amount = parseInt(r.amount);
            obj.create_time = convertTimestampJakarta(r.create_time);
            obj.withdrawal_id = r.id;
            obj.status = r.status;
            obj.type = r.type; // SETTLE: From orders. WITHDRAW: withdraw to wallet. 
            return obj;
        });

        return formattedWithdrawals;
    } catch (e) {
        console.log("[TIKTOK-FINANCE] Error getting withdrawals on brand: ", brand);
        console.log(e);
    }
}

async function getTransactionsByStatement(brand, shopCipher, accessToken, statementId) {
    try {
        const appKey = "6j6u4kmpdda19"
        const appSecret = "c4680b9ff6797160adb92104a77e2e1aa085c733"
        
        const path = `/finance/202501/statements/${statementId}/statement_transactions`;
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";
        
        let keepFetching = true;
        let currPageToken = "";
        

        let rawTransactionsPerStatement = [];
        while(keepFetching) {
            
            const timestamp = Math.floor(Date.now() / 1000);
            const queryParams = {   
                app_key: appKey,
                sort_field: "order_create_time",
                sort_order: "DESC",
                page_size: 100,
                timestamp: timestamp,
                shop_cipher: shopCipher
            };  
            if(currPageToken) {
                queryParams.page_token = currPageToken;
            }
            const sortedKeys = Object.keys(queryParams).sort();

            let result = appSecret + path;
            for(const key of sortedKeys) {
                result += key + queryParams[key];
            }
            result += appSecret;

            const sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');
            queryParams.sign = sign;
            const querySearchParams = new URLSearchParams(queryParams);

            const completeUrl = baseUrl + querySearchParams.toString();
            const response = await axios.get(completeUrl, {
                headers: {
                    'content-type': 'application/json',
                    'x-tts-access-token': accessToken,
                }
            });

            // console.log("[TIKTOK-FINANCE] TRX by statement response: ", response.data.data);

            rawTransactionsPerStatement.push(...response.data.data.transactions);

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        }

        const formattedTransactionsPerStatement = rawTransactionsPerStatement.map((r) => {
            let data = {};
            data.transaction_id = r.id;
            data.order_id = r.order_id;
            data.order_create_time = convertTimestampJakarta(r.order_create_time);
            data.settlement_amount = parseInt(r.settlement_amount);
            data.transaction_type = r.type;
            data.statement_id = statementId;
            return data;
        });

        return formattedTransactionsPerStatement;
    } catch (e) {
        console.log("[TIKTOK-FINANCE] Error getting trx by statement on brand: ", brand);
        console.log(e);
    }
}

async function getStatements(brand, shopCipher, accessToken) {
    try {
        const appKey = "6j6u4kmpdda19"
        const appSecret = "c4680b9ff6797160adb92104a77e2e1aa085c733"
        
        const path = "/finance/202309/statements";
        const baseUrl = "https://open-api.tiktokglobalshop.com" + path + "?";
        const statementTimeFrom = Math.floor(new Date("2025-10-01T00:00:00+07:00").getTime() / 1000);
        const statementTimeTo = Math.floor(new Date("2025-11-01T23:59:59+07:00").getTime() / 1000);
        
        let keepFetching = true;
        let currPageToken = "";

        let rawStatements = [];
        
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
            if(currPageToken) {
                queryParams.page_token = currPageToken;
            }
            const sortedKeys = Object.keys(queryParams).sort();

            let result = appSecret + path;
            for(const key of sortedKeys) {
                result += key + queryParams[key];
            }
            result += appSecret;

            const sign = crypto.createHmac('sha256', appSecret).update(result).digest('hex');
            queryParams.sign = sign;
            const querySearchParams = new URLSearchParams(queryParams);

            const completeUrl = baseUrl + querySearchParams.toString();
            const response = await axios.get(completeUrl, {
                headers: {
                    'content-type': 'application/json',
                    'x-tts-access-token': accessToken,
                }
            });

            // console.log("[TIKTOK-FINANCE] Statements raw response: ", response.data.data);

            rawStatements.push(...response.data.data.statements);

            const nextPageToken = response.data.data.next_page_token;

            if(nextPageToken && nextPageToken.length > 0) {
                currPageToken = nextPageToken;
            } else {
                keepFetching = false;
            }
        }

        const formattedRawStatements = rawStatements
            .map(r => {
                let obj = {};
                obj.statement_id = r.id;
                obj.withdrawal_id = r.payment_id;
                obj.settlement_amount = parseInt(r.settlement_amount);
                obj.statement_time = convertTimestamp(r.statement_time); // Converts to 07:00 (UTC+7). Settled every day at 00:00 (the next day)
                return obj;
            })
            .filter(r => !r.statement_time.includes("2025-09"));

        return formattedRawStatements;
    } catch (e) {
        console.log("[TIKTOK-FINANCE] Error getting statements on brand: ", brand);
        console.log(e);
    }
}

function generateWalletTransactions(rawWithdrawals, rawTransactions) {
  // 1. Build and sort the Order Queue (Inflows)
  // Filter out zero-amount orders and initialize the remaining_balance
  const orderQueue = rawTransactions
    .filter(trx => trx.settlement_amount > 0)
    .sort((a, b) => new Date(a.order_create_time) - new Date(b.order_create_time))
    .map(trx => ({
      ...trx,
      remaining_balance: trx.settlement_amount
    }));

  // 2. Isolate and sort the Withdrawals (Outflows)
  const withdrawals = rawWithdrawals
    .filter(w => w.type === 'WITHDRAW')
    .sort((a, b) => new Date(a.create_time) - new Date(b.create_time));

  const walletTrx = [];

  // 3. The FIFO Matching Loop
  for (const withdrawal of withdrawals) {
    let unfulfilledAmount = withdrawal.amount;

    while (unfulfilledAmount > 0 && orderQueue.length > 0) {
      const currentOrder = orderQueue[0];
      
      // Determine how much we can take from this order
      const allocationAmount = Math.min(unfulfilledAmount, currentOrder.remaining_balance);

      // Create the flat record for BigQuery
      walletTrx.push({
        withdrawal_id: withdrawal.withdrawal_id,
        withdrawal_create_time: withdrawal.create_time,
        withdrawal_total_amount: withdrawal.amount,
        statement_id: currentOrder.statement_id,
        order_id: currentOrder.order_id,
        order_create_time: currentOrder.order_create_time,
        order_total_amount: currentOrder.settlement_amount,
        allocated_amount: allocationAmount
      });

      // Deduct the allocated amount from our trackers
      unfulfilledAmount -= allocationAmount;
      currentOrder.remaining_balance -= allocationAmount;

      // If the order is completely depleted, remove it from the front of the queue
      if (currentOrder.remaining_balance === 0) {
        orderQueue.shift();
      }
    }
  }

  return walletTrx;
}

export async function handleFinance(brand) {

    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;

    await refreshTokens(brand, refreshToken);

    const shopCipher = await getShopCipher(brand, accessToken);
    console.log("Shop cipher: ", shopCipher);

    const rawWithdrawals = await getWithdrawals(brand, shopCipher, accessToken);
    console.log("Raw Withdrawals: ");
    console.log(rawWithdrawals.length, " in size");
    // console.log(rawWithdrawals);

    const rawStatements = await getStatements(brand, shopCipher, accessToken);
    console.log("Raw Statements: ");
    console.log(rawStatements.length, " in size ");
    // console.log(rawStatements);

    let allTransactionsPerStatement = [];

    for (const statement of rawStatements) {
        // console.log(`Fetching transactions for statement: ${statement.statement_id}`);
        const transactions = await getTransactionsByStatement(brand, shopCipher, accessToken, statement.statement_id);
        
        if (transactions && transactions.length > 0) {
            allTransactionsPerStatement.push(...transactions);
        }
    }

    console.log("Total Transactions across all statements: ", allTransactionsPerStatement.length);

    const sortedTransactions = allTransactionsPerStatement.sort((a, b) => new Date(b.order_create_time) - new Date(a.order_create_time));
    console.log("Top 10 Latest Transactions:");
    console.log(sortedTransactions.slice(0, 10));

    const totalSettlementAmount = allTransactionsPerStatement.reduce((sum, t) => sum + (t.settlement_amount || 0), 0);
    console.log("Total Settlement amount for period: ", totalSettlementAmount);
    
    const flattenedLineageData = generateWalletTransactions(rawWithdrawals, allTransactionsPerStatement);
    // console.log(flattenedLineageData);

    await mergeFinanceTiktok(brand, flattenedLineageData);
}

const brandTables = {
    "Eileen Grace": "eileen_grace_wallet_trx",
    "Mamaway": "mamaway_wallet_trx",
    "SHRD": "shrd_wallet_trx",
    "Miss Daisy": "miss_daisy_wallet_trx",
    "Polynia": "polynia_wallet_trx",
    "CHESS": "chess_wallet_trx",
    "Cleviant": "cleviant_wallet_trx",
    "Mosseru": "mosseru_wallet_trx",
    "Evoke": "evoke_wallet_trx",
    "Dr.Jou": "dr_jou_wallet_trx",
    "Mirae": "mirae_wallet_trx",
    "Swissvita": "swissvita_wallet_trx",
    "G-Belle": "gbelle_wallet_trx",
    "Past Nine": "past_nine_wallet_trx",
    "Nutri Beyond": "nutri_beyond_wallet_trx",
    "Ivy Lily": "ivy_lily_wallet_trx",
    "Naruko": "naruko_wallet_trx",
    "Relove": "relove_wallet_trx",
    "Joey & Roo": "joey_roo_wallet_trx",
    "Rocketindo Shop": "pinkrocket_wallet_trx"
}

async function mergeFinanceTiktok(brand, data) {
    try {
        const datasetId = "tiktok_api_us";
        const tableName = brandTables[brand];
        const bigquery = new BigQuery();

        console.log("Data wallet trx before merging on brand: ", brand);
        console.log(data.length);

        let batchSize = 1000;
        for(let i=0; i<data.length; i+=batchSize) {
            const batchData = data.slice(i, i+batchSize);
            
            const incomingOrderIds = batchData.map(row => `'${row.order_id}'`).join(",");
            if(!incomingOrderIds) continue;

            const query = `
                SELECT order_id
                FROM  \`${bigquery.projectId}.${datasetId}.${tableName}\`
                WHERE order_id IN (${incomingOrderIds})
            `
            const [existingRows] = await bigquery.query(query);
            const existingOrderIds = new Set(existingRows.map(row => row.order_id));
            console.log("[TIKTOK-FINANCE] Found: ", existingOrderIds.size, " duplicates in table: ", tableName);

            const dataToInsert = batchData.filter(row => !existingOrderIds.has(row.order_id));
            console.log("[TIKTOK-FINANCE] Data to insert: ", dataToInsert.length);

            if(dataToInsert.length === 0) {
                console.log("[TIKTOK-AFFILIATE] All data already exists. Skip");
                continue;
            }

            await bigquery
                .dataset(datasetId)
                .table(tableName)
                .insert(dataToInsert);

            console.log("[TIKTOK-FINANCE] Successfully inserted rows on: ", brandTables[brand]);
        }

    } catch (e) {
        console.log("[TIKTOK-FINANCE] Error merging wallet trx tiktok on brand: ", brand);
        console.log(e);
    }
}

export async function mainTiktokFinance() {
    // await handleFinance("Eileen Grace")
    await handleFinance("Mamaway");
    // await handleFinance("SHRD");
    // await handleFinance("Miss Daisy");
    // await handleFinance("Polynia");
    // await handleFinance("CHESS");
    // await handleFinance("Cléviant");
    // await handleFinance("Mossèru");
    // await handleFinance("Evoke");
    // await handleFinance("Dr Jou");
    // await handleFinance("Mirae")
    // await handleFinance("Swissvita");
    // await handleFinance("G-Belle");
    // await handleFinance("Past Nine");
    // await handleFinance("Nutri & Beyond");
    // await handleFinance("Ivy & Lily");
    // await handleFinance("Naruko");
    // await handleFinance("Relove");
    // await handleFinance("Joey & Roo");
    // await handleFinance("Rocketindo Shop");
}

// October backfill
await mainTiktokFinance();