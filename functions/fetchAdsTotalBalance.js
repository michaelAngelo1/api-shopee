import axios from 'axios';
import crypto from 'crypto';
import { BigQuery } from '@google-cloud/bigquery';
const bigquery = new BigQuery();

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export async function fetchAdsTotalBalance(brand, partner_id, partner_key, accessToken, shop_id) {
    console.log("Fetch Ads Total Balance of brand: ", brand);
    
    const HOST = "https://partner.shopeemobile.com"
    const PATH = "/api/v2/ads/get_all_cpc_ads_daily_performance";

    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${partner_id}${PATH}${timestamp}${accessToken}${shop_id}`;
    const sign = crypto.createHmac('sha256', partner_key)
        .update(baseString)
        .digest('hex');

    const yesterday = new Date(Date.now() - 86400000 * 1);
    const day = String(yesterday.getDate()).padStart(2, '0');
    const month = String(yesterday.getMonth() + 1).padStart(2, '0'); 
    const year = yesterday.getFullYear();
    const yesterdayString = `${day}-${month}-${year}`;
    
    const params = new URLSearchParams({
        partner_id: partner_id,
        timestamp,
        access_token: accessToken,
        shop_id: shop_id,
        sign,
        start_date: yesterdayString,
        end_date: yesterdayString
    });

    const fullUrl = `${HOST}${PATH}?${params.toString()}`;
    console.log(`Hitting Ads Total Balance for ${brand}: `, fullUrl);

    let totalExpense = [];

    let retries = 3;
    let success = false;

    while(!success && retries > 0) {
        try {
            const response = await axios.get(fullUrl, {
                headers: {
                    'Content-Type': 'application/json'
                }
            });
    
            if(response && response.data.response) {
                // console.log(`${brand} Ads Total Balance: ${response.data.response[0].expense} on ${response.data.response[0].date}`);
                
                success = true;

                let responseList = response.data.response;
                responseList.forEach(r => {
                    if(r.expense > 0) {
                        totalExpense.push(r);
                    }
                })
                // return totalExpense;
                // await submitData(brand, response.data.response[0].expense, response.data.response[0].date);
                await submitData(brand, totalExpense);
            } else {
                console.log("[SHOPEE] response ads does not exist: ", brand);
                
                retries -= 1;
                if(retries > 0) await sleep(5000);
            }
        } catch (e) {
            console.log(`Error fetching total balance for ${brand}: ${e}`);
        }
    }
}

async function submitData(brand, expenses) {
    let tableName = ""
    
    if(brand == "Eileen Grace") {
        tableName = "eileen_grace_ads_spending";
    } else if(brand == "Miss Daisy") {
        tableName = "miss_daisy_ads_spending";
    } else if(brand == "SH-RD") {
        tableName = "shrd_ads_spending";
    } else if(brand == "Cleviant") {
        tableName = "cleviant_ads_spending";
    } else if(brand == "Mosseru") {
        tableName = "mosseru_ads_spending";
    } else if(brand == "Dr.Jou") {
        tableName = "drjou_ads_spending";
    } else if(brand == "G-Belle") {
        tableName = "gbelle_ads_spending";
    } else if(brand == "Ivy & Lily") {
        tableName = "ivylily_ads_spending"
    } else if(brand == "Evoke") {
        tableName = "evoke_ads_spending";
    } else if(brand == "Mamaway") {
        tableName = "mmw_ads_spending";
    } else if(brand == "Chess") {
        tableName = "chess_ads_spending";
    } else if(brand == "Swissvita") {
        tableName = "swissvita_ads_spending";
    } else if(brand == "Past Nine") {
        tableName = "pastnine_ads_spending";
    } else if(brand == "Nutri & Beyond") {
        tableName = "nutribeyond_ads_spending";
    } else if(brand == "Polynia") {
        tableName = "polynia_ads_spending";
    } else if(brand == "Mirae") {
        tableName = "mirae_ads_spending";
    }
 
    const datasetId = 'shopee_api';

    console.log(`[SHOPEE] Ads Total Balance on ${brand}`);

    
    try {

        for(const expense of expenses) {
            const query = `
                SELECT Tanggal_Dibuat
                FROM \`${datasetId}.${tableName}\`
                WHERE Tanggal_Dibuat = @date
            `;
            const options = {
                query,
                params: { 
                    date: expense.date 
                }
            }
            const [rows] = await bigquery.query(options);
    
            if(rows.length > 0) {
                console.log("Row already exists");
                continue;
            }
    
            await bigquery
                .dataset(datasetId)
                .table(tableName)
                .insert({
                    Tanggal_Dibuat: expense.date,
                    Spending: expense.expense,
                });
        }

        console.log(`Successfully written ads spending to ${brand} table.`)
    } catch (e) {
        console.error(`Error inserting ads spending on ${brand}: ${e}`);
    }
}