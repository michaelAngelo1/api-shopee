import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
const secretClient = new SecretManagerServiceClient();

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
        console.error("[MERGE-REALTIME] Error getting service account credentials 1: ", e);
    }
}

async function loadSalesCountCredentials() {
    const secretName = "projects/231801348950/secrets/realtime-service-account-2/versions/latest";

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName
        });
        const data = version.payload.data.toString('UTF-8');
        const creds = JSON.parse(data);

        return creds;
    } catch (e) {
        console.error("[MERGE-REALTIME] Error getting service account credentials 2: ", e);
    }
}

async function loadLastUpdatedCredentials() {
    const secretName = "projects/231801348950/secrets/realtime-service-account-3/versions/latest";

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName
        });
        const data = version.payload.data.toString('UTF-8');
        const creds = JSON.parse(data);

        return creds;
    } catch (e) {
        console.error("[MERGE-REALTIME] Error getting service account credentials 3: ", e);
    }
}

export async function handleMergeRealtime(brand, marketplace, sales_value, orders_count) {

    if(marketplace == "TikTok" || marketplace == "Tokopedia") {
        if(brand == "Cléviant") brand = "Cleviant";
        if(brand == "Mossèru") brand = "Mosseru";
        if(brand == "Nutri & Beyond") brand = "Nutri Beyond";
        if(brand == "Ivy & Lily") brand = "Ivy Lily";
        if(brand == "Joey & Roo") brand = "Joey Roo";
    }

    console.log(`Start merging to sheets for brand: ${brand} from ${marketplace} with ${sales_value} sales today.`);

    try {
    const saCredsRawData     = await loadCredentials();
    const saCredsSalesCount  = await loadSalesCountCredentials();
    const saCredsLastUpdated = await loadLastUpdatedCredentials();

    // All-or-nothing: skip the whole merge if any SA failed to load, and log which one(s)
    if (!saCredsRawData || !saCredsSalesCount || !saCredsLastUpdated) {
        const missing = [
            !saCredsRawData && 'RawData (SA#1)',
            !saCredsSalesCount && 'SalesCount (SA#2)',
            !saCredsLastUpdated && 'LastUpdated (SA#3)',
        ].filter(Boolean).join(', ');
        console.log(`[MERGE-REALTIME] Skipping ${brand}/${marketplace}: failed to load service account credentials for: ${missing}`);
        return;
    }

    // let devSheetId = "1zArzQCqewtCxkka9l03bRpZLAjqOjapV6ngHUp0jluM"
    let prodSheetId = "1RMcbhi0wZgXqvYf_lFma2U8vznJh4ACy_OmsTjplgKI"

    const makeAuth = (creds) => new JWT({
        email: creds.client_email,
        key: creds.private_key,
        scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });

    // One doc (= one service account) per sheet, so each row.save() lands on its own quota bucket
    const docRawData     = new GoogleSpreadsheet(prodSheetId, makeAuth(saCredsRawData));
    const docSalesCount  = new GoogleSpreadsheet(prodSheetId, makeAuth(saCredsSalesCount));
    const docLastUpdated = new GoogleSpreadsheet(prodSheetId, makeAuth(saCredsLastUpdated));

    await docRawData.loadInfo();
    await docSalesCount.loadInfo();
    await docLastUpdated.loadInfo();

    const sheetRawData     = docRawData.sheetsByIndex[0];      // SA#1
    const sheetSalesCount  = docSalesCount.sheetsByIndex[1];   // SA#2
    const sheetLastUpdated = docLastUpdated.sheetsByIndex[2];  // SA#3

    const rowsRawData     = await sheetRawData.getRows();
    const rowsSalesCount  = await sheetSalesCount.getRows();
    const rowsLastUpdated = await sheetLastUpdated.getRows();

    for(const row of rowsRawData) {
        const rowBrand = row.get('Brand');
        const rowPlatform = row.get('Platform');

        if(rowBrand == brand && rowPlatform == marketplace) {
            const now = new Date();
            const utc7Time = new Date(now.getTime() + (7 * 60 * 60 * 1000));
            const formattedTimestamp = utc7Time.toISOString().replace('T', ' ').substring(0, 19);

            row.assign({
                'GMV': sales_value,
                'Timestamp': formattedTimestamp,
            });
    
            if (row._rawData.length > 4) {
                row._rawData = row._rawData.slice(0, 4);
            }
    
            await row.save();

            setTimeout(() => {
            }, 3000)
        }

    }

    for(const row of rowsSalesCount) {
        const rowBrand = row.get('Brand');
        const rowPlatform = row.get('Platform');

        if(rowBrand == brand && rowPlatform == marketplace) {
            const now = new Date();
            const utc7Time = new Date(now.getTime() + (7 * 60 * 60 * 1000));
            const formattedTimestamp = utc7Time.toISOString().replace('T', ' ').substring(0, 19);
            
            row.assign({
                'GMV': sales_value,
                'Orders': orders_count,
                'Timestamp': formattedTimestamp,
            });
    
            if (row._rawData.length > 5) {
                row._rawData = row._rawData.slice(0, 5);
            }
    
            await row.save();

            setTimeout(() => {
            }, 3000)
        }

    }

    for(const row of rowsLastUpdated) {
        const rowBrand = row.get('Brand');
        const rowPlatform = row.get('Platform');

        if(rowBrand == brand && rowPlatform == marketplace) {
            const now = new Date();
            const utc7Time = new Date(now.getTime() + (7 * 60 * 60 * 1000));
            const formattedTimestamp = utc7Time.toISOString().replace('T', ' ').substring(0, 19);
            
            if(sales_value > 0) {
                row.assign({
                    'GMV': sales_value,
                    'Orders': orders_count,
                    'Timestamp': formattedTimestamp,
                });
        
                if (row._rawData.length > 5) {
                    row._rawData = row._rawData.slice(0, 5);
                }
        
                await row.save();

                setTimeout(() => {
                }, 3000)
            }
        }

    }



    } catch (e) {
        console.log(`[MERGE-REALTIME] Error merging to sheets for ${brand}/${marketplace}: `, e.message || e);
    }
}