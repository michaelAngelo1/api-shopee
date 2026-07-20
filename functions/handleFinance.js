import 'dotenv/config';
import crypto from 'crypto';
import axios from 'axios';
import { BigQuery } from '@google-cloud/bigquery';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import { loadTokens, refreshTokens, getShopCipher } from '../auth/tiktokAuth.js';

function generateDateRanges(targetMonthStr) {
    // targetMonthStr format: "YYYY-MM" (e.g., "2025-11")
    const [yearStr, monthStr] = targetMonthStr.split('-');
    const year = parseInt(yearStr, 10);
    const month = parseInt(monthStr, 10); // 1-indexed

    // Helper: Gets exactly Day 1 to the Last Day of a given month
    function getMonthRange(y, m) {
        // JS Date month is 0-indexed. Day 0 of the *next* month gives the last day of the *current* month.
        const startDate = new Date(Date.UTC(y, m - 1, 1));
        const endDate = new Date(Date.UTC(y, m, 0));
        return {
            start: startDate.toISOString().split('T')[0],
            end: endDate.toISOString().split('T')[0]
        };
    }

    // Helper: Gets exactly Day 1 to Day 7 of the *next* month
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
            getMonthRange(year, month - 1), // 1 Month Prior Buffer
            getMonthRange(year, month)      // Target Month
        ],
        statementMonths: [
            getMonthRange(year, month - 2), // 2 Months Prior Buffer
            getMonthRange(year, month - 1), // 1 Month Prior Buffer
            getMonthRange(year, month),     // Target Month
            getNextMonthBuffer(year, month) // 7-Day Forward Buffer
        ]
    };
}

export function convertTimestampJakarta(orderCreatedTime) {
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

const brandsInternalApp = {
    "Eileen Grace": 1,
    "Mamaway": 1,
    "SHRD": 1,
    "Miss Daisy": 1,
    "CHESS": 1,
    "Polynia": 1,
    "CHESS": 1,
    "Cléviant": 1,
    "Mossèru": 1,
    "Evoke": 1,
    "Dr Jou": 1,
    "Mirae": 2,
    "Swissvita": 2,
    "G-Belle": 2,
    "Past Nine": 2,
    "Nutri & Beyond": 2,
    "Ivy & Lily": 2,
    "Naruko": 2,
    "Relove": 2,
    "Joey & Roo": 2, 
    "Rocketindo Shop": 2,
    "Enchante": 3,
}

async function getWithdrawals(brand, shopCipher, accessToken, monthsToFetch) {
    try {

        let appKey;
        let appSecret;

        if(brandsInternalApp[brand] === 1) {
            appKey = "6j6u4kmpdda19"
            appSecret = "c4680b9ff6797160adb92104a77e2e1aa085c733"
        } else if(brandsInternalApp[brand] === 2) {
            appKey = "6j7inu4s9dkfq"
            appSecret = "3493907831adc26d58c74262f709b48a2205a2d0"
        } else {
            appKey = "6jbrll2ed26dp";
            appSecret = "04679ae180556cdc79b11a3e7cbd8da33f0d6e92"
        }
        
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

                rawWithdrawals.push(...response.data.data.withdrawals);
                const nextPageToken = response.data.data.next_page_token;
                currPageToken = (nextPageToken && nextPageToken.length > 0) ? nextPageToken : "";
                if(!currPageToken) keepFetching = false;
            }
        }

        return rawWithdrawals.map(r => ({
            amount: parseInt(r.amount),
            create_time: convertTimestampJakarta(r.create_time),
            withdrawal_id: r.id,
            status: r.status,
            type: r.type
        }));
    } catch (e) {
        console.log("[TIKTOK-FINANCE] Error getting withdrawals", e);
    }
}

async function getStatements(brand, shopCipher, accessToken, monthsToFetch) {
    try {

        let appKey;
        let appSecret;

        if(brandsInternalApp[brand] === 1) {
            appKey = "6j6u4kmpdda19"
            appSecret = "c4680b9ff6797160adb92104a77e2e1aa085c733"
        } else if(brandsInternalApp[brand] === 2) {
            appKey = "6j7inu4s9dkfq"
            appSecret = "3493907831adc26d58c74262f709b48a2205a2d0"
        } else {
            appKey = "6jbrll2ed26dp";
            appSecret = "04679ae180556cdc79b11a3e7cbd8da33f0d6e92"
        }
        
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

                rawStatements.push(...response.data.data.statements);
                const nextPageToken = response.data.data.next_page_token;
                currPageToken = (nextPageToken && nextPageToken.length > 0) ? nextPageToken : "";
                if(!currPageToken) keepFetching = false;
            }
        }

        return rawStatements.map(r => ({
            statement_id: r.id,
            withdrawal_id: r.payment_id,
            settlement_amount: parseInt(r.settlement_amount),
            statement_time: convertTimestamp(r.statement_time)
        }));
    } catch (e) {
        console.log("[TIKTOK-FINANCE] Error getting statements", e);
    }
}

// Updated. Needs to be updated in the source code in shopee-worker
async function getTransactionsByStatement(brand, shopCipher, accessToken, statementId) {
    try {
        let appKey;
        let appSecret;

        if(brandsInternalApp[brand] === 1) {
            appKey = "6j6u4kmpdda19"
            appSecret = "c4680b9ff6797160adb92104a77e2e1aa085c733"
        } else if(brandsInternalApp[brand] === 2) {
            appKey = "6j7inu4s9dkfq"
            appSecret = "3493907831adc26d58c74262f709b48a2205a2d0"
        } else {
            appKey = "6jbrll2ed26dp";
            appSecret = "04679ae180556cdc79b11a3e7cbd8da33f0d6e92"
        }
        
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

            let response;
            let attempt = 0;
            const maxAttempts = 5;
            while (true) {
                try {
                    response = await axios.get(completeUrl, {
                        headers: {
                            'content-type': 'application/json',
                            'x-tts-access-token': accessToken,
                        }
                    });
                    break;
                } catch (err) {
                    const message = err.response?.data?.message || '';
                    attempt++;
                    if (message.includes('Too many requests') && attempt < maxAttempts) {
                        const backoffMs = 1000 * 2 ** attempt;
                        console.log(`[TIKTOK-FINANCE] Rate limited on statement ${statementId}, retrying in ${backoffMs}ms (attempt ${attempt}/${maxAttempts})`);
                        await new Promise(r => setTimeout(r, backoffMs));
                        continue;
                    }
                    throw err;
                }
            }

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
        console.log(e.response.data.message);
    }
}

function generateWalletTransactions(rawWithdrawals, rawStatements, rawTransactions) {
  // 1. Map Statement -> Earnings ID
  const statementToEarningsMap = new Map();
  rawStatements.forEach(stmt => {
    statementToEarningsMap.set(String(stmt.statement_id), {
        earnings_id: String(stmt.withdrawal_id),
        earnings_create_time: stmt.statement_time 
    });
  });

  // 2. Setup Earnings (Blocks) and Withdrawals
  const earningsQueue = rawWithdrawals
    .filter(w => w.type !== 'WITHDRAW' && w.type !== 'TRANSFER') 
    .map(w => ({
      earnings_id: String(w.withdrawal_id),
      create_time: w.create_time,
      amount: parseInt(w.amount, 10), 
      used: false 
    }))
    .sort((a, b) => new Date(a.create_time) - new Date(b.create_time));

  const bankWithdrawals = rawWithdrawals
    .filter(w => w.type === 'WITHDRAW' || w.type === 'TRANSFER')
    .sort((a, b) => new Date(a.create_time) - new Date(b.create_time));

  // 3. The "PROXIMITY" Sliding Window
  const earningsToWithdrawalMap = new Map();

  for (const w of bankWithdrawals) {
    const target = Math.abs(parseInt(w.amount, 10));
    const withdrawalTime = new Date(w.create_time).getTime();

    let bestMatchIndices = null;
    let minTimeDiff = Infinity;

    // Find ALL possible contiguous blocks that equal the target
    for (let start = 0; start < earningsQueue.length; start++) {
      if (earningsQueue[start].used) continue;

      let sum = 0;
      let candidateIndices = [];

      for (let end = start; end < earningsQueue.length; end++) {
        if (earningsQueue[end].used) continue;

        sum += earningsQueue[end].amount;
        candidateIndices.push(end);

        if (sum === target) {
          // We found a mathematical match!
          // But is it the CLOSEST one to the withdrawal date?
          const earningTime = new Date(earningsQueue[end].create_time).getTime();
          const timeDiff = Math.abs(withdrawalTime - earningTime);

          if (timeDiff < minTimeDiff) {
            minTimeDiff = timeDiff;
            bestMatchIndices = [...candidateIndices];
          }
          break; // Stop adding to this block, check the next starting point
        } else if (sum > target) {
          break; // Overshot, check next starting point
        }
      }
    }

    // Lock in the best, closest match!
    if (bestMatchIndices) {
      bestMatchIndices.forEach(idx => {
        earningsQueue[idx].used = true;
        if (earningsQueue[idx].amount > 0) {
            earningsToWithdrawalMap.set(earningsQueue[idx].earnings_id, {
              withdrawal_id: String(w.withdrawal_id),
              withdrawal_create_time: w.create_time,
              withdrawal_total_amount: target
            });
        }
      });

      if (target === 1010290) {
          const matchedNumbers = bestMatchIndices.map(i => earningsQueue[i].amount);
          console.log(`\n[VICTORY] Matched 1.01M Withdrawal using CLOSEST Earnings:`, matchedNumbers);
      }
    }
  }

  // 4. Build the final output
  const walletTrx = [];

  for (const trx of rawTransactions) {
    const statementInfo = statementToEarningsMap.get(String(trx.statement_id));
    if (!statementInfo || !statementInfo.earnings_id) continue;

    const earningsId = statementInfo.earnings_id;
    const earningsCreateTime = statementInfo.earnings_create_time;

    if (!earningsId) continue;

    const wMatch = earningsToWithdrawalMap.get(earningsId);

    walletTrx.push({
      withdrawal_id: wMatch ? wMatch.withdrawal_id : null,
      withdrawal_create_time: wMatch ? wMatch.withdrawal_create_time : null,
      withdrawal_total_amount: wMatch ? wMatch.withdrawal_total_amount : null,
      earnings_id: earningsId,
      earnings_create_time: earningsCreateTime,
      transaction_id: String(trx.transaction_id),
      order_id: trx.order_id ? String(trx.order_id) : null,
      order_create_time: trx.order_create_time,
      order_total_amount: parseInt(trx.settlement_amount, 10),
      transaction_type: trx.transaction_type
    });
  }

  return walletTrx;
}

const brandTables = {
    "Eileen Grace": "eileen_grace_wallet_trx",
    "Mamaway": "mamaway_wallet_trx",
    "SHRD": "shrd_wallet_trx",
    "Miss Daisy": "miss_daisy_wallet_trx",
    "Polynia": "polynia_wallet_trx",
    "CHESS": "chess_wallet_trx",
    "Cléviant": "cleviant_wallet_trx",
    "Mossèru": "mosseru_wallet_trx",
    "Evoke": "evoke_wallet_trx",
    "Dr Jou": "dr_jou_wallet_trx",
    "Mirae": "mirae_wallet_trx",
    "Swissvita": "swissvita_wallet_trx",
    "G-Belle": "gbelle_wallet_trx",
    "Past Nine": "past_nine_wallet_trx",
    "Nutri & Beyond": "nutri_beyond_wallet_trx",
    "Ivy & Lily": "ivy_lily_wallet_trx",
    "Naruko": "naruko_wallet_trx",
    "Relove": "relove_wallet_trx",
    "Joey & Roo": "joey_roo_wallet_trx",
    "Rocketindo Shop": "pinkrocket_wallet_trx",
    "Enchante": "enchante_wallet_trx"
}

// async function mergeFinanceTiktok(brand, data) {
//     try {
//         const datasetId = "tiktok_api_us";
//         const tableName = brandTables[brand];
//         const bigquery = new BigQuery();

//         console.log(`[${brand}] Processing ${data.length} rows...`);
//         if (data.length === 0) return;

//         // --- STEP 1: GLOBAL CLEANUP VIA CTAS ---
//         console.log(`[${brand}] Rebuilding table to drop old unwithdrawn rows...`);
//         const allTxIds = data.map(row => `'${row.transaction_id}'`).join(",");

//         const ctasQuery = `
//             CREATE OR REPLACE TABLE \`${bigquery.projectId}.${datasetId}.${tableName}\` AS
//             SELECT * FROM \`${bigquery.projectId}.${datasetId}.${tableName}\`
//             WHERE NOT (
//                 transaction_id IN (${allTxIds}) 
//                 AND withdrawal_id IS NULL
//             )
//         `;
        
//         await bigquery.query({ query: ctasQuery });
//         console.log(`[${brand}] Table rebuilt successfully.`);

//         // --- STEP 2: BATCH INSERTS VIA SQL (NO STREAMING API) ---
//         let batchSize = 1000;
//         for(let i=0; i<data.length; i+=batchSize) {
//             const batchData = data.slice(i, i+batchSize);
//             const batchIds = batchData.map(row => `'${row.transaction_id}'`).join(",");

//             const checkQuery = `
//                 SELECT transaction_id, earnings_create_time
//                 FROM  \`${bigquery.projectId}.${datasetId}.${tableName}\`
//                 WHERE transaction_id IN (${batchIds})
//             `;
//             const [existingRows] = await bigquery.query(checkQuery);
            
//             // Map existing rows to check their current earnings_create_time status
//             const existingRowsMap = new Map();
//             existingRows.forEach(row => existingRowsMap.set(row.transaction_id, row.earnings_create_time));
            
//             // 1. Filter out rows we already have completely for INSERTS
//             const dataToInsert = batchData.filter(row => !existingRowsMap.has(row.transaction_id));
            
//             // 2. Find rows that exist in DB, but have a NULL earnings_create_time (NEW)
//             const dataToUpdate = batchData.filter(row => {
//                 // Update ONLY if DB value is null/empty AND our incoming data actually has a value
//                 const dbTime = existingRowsMap.get(row.transaction_id);
//                 return existingRowsMap.has(row.transaction_id) && !dbTime && row.earnings_create_time; 
//             });
            
//             console.log(`[${brand}] Insert: ${dataToInsert.length} | Update: ${dataToUpdate.length}`);

//             if(dataToInsert.length > 0) {
//                 // Construct a raw SQL INSERT INTO query (Unchanged except adding e_time from previous step)
//                 const valuesString = dataToInsert.map(row => {
//                     const w_id = row.withdrawal_id ? `'${row.withdrawal_id}'` : 'NULL';
//                     const w_time = row.withdrawal_create_time ? `'${row.withdrawal_create_time}'` : 'NULL';
//                     const w_amt = row.withdrawal_total_amount !== null ? row.withdrawal_total_amount : 'NULL';
//                     const e_id = row.earnings_id ? `'${row.earnings_id}'` : 'NULL';
//                     const e_time = row.earnings_create_time ? `'${row.earnings_create_time}'` : 'NULL';
//                     const t_id = row.transaction_id ? `'${row.transaction_id}'` : 'NULL';
//                     const o_id = row.order_id ? `'${row.order_id}'` : 'NULL';
//                     const o_time = row.order_create_time ? `'${row.order_create_time}'` : 'NULL';
//                     const o_amt = row.order_total_amount !== null ? row.order_total_amount : 'NULL';
                    
//                     // Escape single quotes in strings to prevent SQL syntax errors
//                     const t_type = row.transaction_type ? `'${row.transaction_type.replace(/'/g, "\\'")}'` : 'NULL';

//                     return `(${w_id}, ${w_time}, ${w_amt}, ${e_id}, ${e_time}, ${t_id}, ${o_id}, ${o_time}, ${o_amt}, ${t_type})`;
//                 }).join(',');

//                 // Push through the Query Engine, bypassing the Streaming API completely
//                 const insertQuery = `
//                     INSERT INTO \`${bigquery.projectId}.${datasetId}.${tableName}\`
//                     (withdrawal_id, withdrawal_create_time, withdrawal_total_amount, earnings_id, earnings_create_time, transaction_id, order_id, order_create_time, order_total_amount, transaction_type)
//                     VALUES ${valuesString}
//                 `;

//                 await bigquery.query({ query: insertQuery });
//                 console.log(`[${brand}] Successfully inserted batch using SQL DML.`);
//             }

//             // ADDED: Handle the updates dynamically using a CASE statement for batch efficiency
//             if (dataToUpdate.length > 0) {
//                 const cases = dataToUpdate.map(row => 
//                     `WHEN '${row.transaction_id}' THEN '${row.earnings_create_time}'`
//                 ).join(' ');
                
//                 const updateIds = dataToUpdate.map(row => `'${row.transaction_id}'`).join(",");

//                 const updateQuery = `
//                     UPDATE \`${bigquery.projectId}.${datasetId}.${tableName}\`
//                     SET earnings_create_time = CASE transaction_id
//                         ${cases}
//                     END
//                     WHERE transaction_id IN (${updateIds})
//                 `;

//                 await bigquery.query({ query: updateQuery });
//                 console.log(`[${brand}] Successfully updated batch for missing earnings_create_time.`);
//             }
//         }
        
//     } catch (e) {
//         console.log("[TIKTOK-FINANCE] Error merging wallet trx tiktok on brand: ", brand);
//         console.log(e.response);
//     }
// }

async function mergeFinanceTiktok(brand, data) {
    try {
        const datasetId = "tiktok_api_us";
        const tableName = brandTables[brand];
        const bigquery = new BigQuery();

        if (data.length === 0) return;

        // Updated. Needs to update in shopee-worker as well.
        const allTxIds = data.map(row => String(row.transaction_id));

        const ctasQuery = `
            CREATE OR REPLACE TABLE \`${bigquery.projectId}.${datasetId}.${tableName}\` AS
            SELECT * FROM \`${bigquery.projectId}.${datasetId}.${tableName}\`
            WHERE NOT (
                transaction_id IN UNNEST(@txIds) 
                AND withdrawal_id IS NULL
            )
        `;
        
        await bigquery.query({ query: ctasQuery, params: { txIds: allTxIds } });
        // End Updated.

        let batchSize = 1000;
        for(let i = 0; i < data.length; i += batchSize) {
            const batchData = data.slice(i, i + batchSize);
            console.log("Processing ", batchData.length, " rows on table: ", tableName);
            const batchIds = batchData.map(row => `'${row.transaction_id}'`).join(",");

            const checkQuery = `
                SELECT transaction_id, earnings_create_time, withdrawal_id
                FROM  \`${bigquery.projectId}.${datasetId}.${tableName}\`
                WHERE transaction_id IN (${batchIds})
            `;
            const [existingRows] = await bigquery.query(checkQuery);
            
            const existingRowsMap = new Map();
            existingRows.forEach(row => existingRowsMap.set(row.transaction_id, row));
            
            const dataToInsert = batchData.filter(row => !existingRowsMap.has(row.transaction_id));
            
            const dataToUpdate = batchData.filter(row => {
                const dbRow = existingRowsMap.get(row.transaction_id);
                if (!dbRow) return false;
                
                const needsTimeUpdate = !dbRow.earnings_create_time && row.earnings_create_time;
                const needsWithdrawalUpdate = dbRow.withdrawal_id !== row.withdrawal_id && row.withdrawal_id !== null;
                
                return needsTimeUpdate || needsWithdrawalUpdate;
            });

            if(dataToInsert.length > 0) {
                const valuesString = dataToInsert.map(row => {
                    const w_id = row.withdrawal_id ? `'${row.withdrawal_id}'` : 'NULL';
                    const w_time = row.withdrawal_create_time ? `'${row.withdrawal_create_time}'` : 'NULL';
                    const w_amt = row.withdrawal_total_amount !== null ? row.withdrawal_total_amount : 'NULL';
                    const e_id = row.earnings_id ? `'${row.earnings_id}'` : 'NULL';
                    const e_time = row.earnings_create_time ? `'${row.earnings_create_time}'` : 'NULL';
                    const t_id = row.transaction_id ? `'${row.transaction_id}'` : 'NULL';
                    const o_id = row.order_id ? `'${row.order_id}'` : 'NULL';
                    const o_time = row.order_create_time ? `'${row.order_create_time}'` : 'NULL';
                    const o_amt = row.order_total_amount !== null ? row.order_total_amount : 'NULL';
                    const t_type = row.transaction_type ? `'${row.transaction_type.replace(/'/g, "\\'")}'` : 'NULL';

                    return `(${w_id}, ${w_time}, ${w_amt}, ${e_id}, ${e_time}, ${t_id}, ${o_id}, ${o_time}, ${o_amt}, ${t_type})`;
                }).join(',');

                const insertQuery = `
                    INSERT INTO \`${bigquery.projectId}.${datasetId}.${tableName}\`
                    (withdrawal_id, withdrawal_create_time, withdrawal_total_amount, earnings_id, earnings_create_time, transaction_id, order_id, order_create_time, order_total_amount, transaction_type)
                    VALUES ${valuesString}
                `;

                await bigquery.query({ query: insertQuery });
            }

            if (dataToUpdate.length > 0) {
                const casesEarningsTime = dataToUpdate.map(row => 
                    `WHEN '${row.transaction_id}' THEN ${row.earnings_create_time ? `'${row.earnings_create_time}'` : 'earnings_create_time'}`
                ).join(' ');

                const casesWithdrawalId = dataToUpdate.map(row => 
                    `WHEN '${row.transaction_id}' THEN ${row.withdrawal_id ? `'${row.withdrawal_id}'` : 'withdrawal_id'}`
                ).join(' ');

                const casesWithdrawalTime = dataToUpdate.map(row => 
                    `WHEN '${row.transaction_id}' THEN ${row.withdrawal_create_time ? `'${row.withdrawal_create_time}'` : 'withdrawal_create_time'}`
                ).join(' ');

                const casesWithdrawalAmount = dataToUpdate.map(row => 
                    `WHEN '${row.transaction_id}' THEN ${row.withdrawal_total_amount !== null ? row.withdrawal_total_amount : 'withdrawal_total_amount'}`
                ).join(' ');
                
                const updateIds = dataToUpdate.map(row => `'${row.transaction_id}'`).join(",");

                const updateQuery = `
                    UPDATE \`${bigquery.projectId}.${datasetId}.${tableName}\`
                    SET 
                        earnings_create_time = CASE transaction_id ${casesEarningsTime} END,
                        withdrawal_id = CASE transaction_id ${casesWithdrawalId} END,
                        withdrawal_create_time = CASE transaction_id ${casesWithdrawalTime} END,
                        withdrawal_total_amount = CASE transaction_id ${casesWithdrawalAmount} END
                    WHERE transaction_id IN (${updateIds})
                `;

                await bigquery.query({ query: updateQuery });
            }
        }
        
    } catch (e) {
        console.log(e);
    }
}

export async function handleFinance(brand, targetMonth) {

    const tokens = await loadTokens(brand);
    let accessToken = tokens.accessToken;
    let refreshToken = tokens.refreshToken;

    await refreshTokens(brand, refreshToken);

    const shopCipher = await getShopCipher(brand, accessToken);
    console.log("Shop cipher: ", shopCipher);

    const dates = generateDateRanges(targetMonth);

    const rawWithdrawals = await getWithdrawals(brand, shopCipher, accessToken, dates.withdrawalMonths);
    console.log("Raw Withdrawals: ");
    console.log(rawWithdrawals.length, " in size");
    // console.log(rawWithdrawals);

    const ghost = rawWithdrawals.find(w => Math.abs(w.amount) === 1010290);
    // console.log("DEBUG - Found 1.01M Withdrawal:", ghost);

    const rawStatements = await getStatements(brand, shopCipher, accessToken, dates.statementMonths);
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
    
    const flattenedLineageData = generateWalletTransactions(rawWithdrawals, rawStatements, allTransactionsPerStatement);

    const settleEventMap = new Map();
    rawWithdrawals.filter(w => w.type === 'SETTLE').forEach(w => {
        settleEventMap.set(w.withdrawal_id, w.create_time);
    });

    // const finalDataToInsert = flattenedLineageData.filter(row => {
    //     if (row.withdrawal_create_time && row.withdrawal_create_time.startsWith(dates.targetMonth)) {
    //         return true;
    //     }
    //     if (!row.withdrawal_id) {
    //         const actualSettleTime = settleEventMap.get(row.earnings_id);
    //         return actualSettleTime && actualSettleTime.startsWith(dates.targetMonth);
    //     }
    //     return false;
    // });

    const finalDataToInsert = flattenedLineageData.filter(row => {
        // ALWAYS check when it settled in the wallet. This is the truest anchor.
        const actualSettleTime = settleEventMap.get(row.earnings_id);
        const settledInTargetMonth = actualSettleTime && actualSettleTime.startsWith(dates.targetMonth);
        
        // Check when it withdrew to the bank
        const withdrewInTargetMonth = row.withdrawal_create_time && row.withdrawal_create_time.startsWith(dates.targetMonth);

        // Keep it if it belongs to this month's ledger in ANY way
        return settledInTargetMonth || withdrewInTargetMonth;
    });

    await mergeFinanceTiktok(brand, finalDataToInsert);
}

export async function mainTiktokFinance() {
    const targetMonth = new Date().toISOString().slice(0, 7);

    console.log("Current month: ", targetMonth);
    await handleFinance("Eileen Grace", targetMonth);
    // await handleFinance("Mamaway", targetMonth);
    // await handleFinance("SHRD", targetMonth);
    // await handleFinance("Miss Daisy", targetMonth);
    // await handleFinance("Polynia", targetMonth);
    // await handleFinance("CHESS", targetMonth);
    // await handleFinance("Cléviant", targetMonth);
    // await handleFinance("Mossèru", targetMonth);
    // await handleFinance("Evoke", targetMonth);
    // await handleFinance("Dr Jou", targetMonth);
    // await handleFinance("Mirae", targetMonth)
    // await handleFinance("Swissvita", targetMonth);
    // await handleFinance("G-Belle", targetMonth);
    // await handleFinance("Past Nine", targetMonth);
    // await handleFinance("Nutri & Beyond", targetMonth);
    // await handleFinance("Ivy & Lily", targetMonth);
    // await handleFinance("Naruko", targetMonth);
    // await handleFinance("Relove", targetMonth);
    // await handleFinance("Joey & Roo", targetMonth);
    // await handleFinance("Rocketindo Shop", targetMonth);
}

// October backfill
await mainTiktokFinance();