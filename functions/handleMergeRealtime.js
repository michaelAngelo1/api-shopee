import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
const secretClient = new SecretManagerServiceClient();

const prodSheetId = "1RMcbhi0wZgXqvYf_lFma2U8vznJh4ACy_OmsTjplgKI";
// let devSheetId = "1zArzQCqewtCxkka9l03bRpZLAjqOjapV6ngHUp0jluM"

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

// Load each SA secret ONCE and reuse the JWT across every merge (no per-merge Secret Manager reads).
let cachedAuths = null;
async function getAuthClients() {
    if (cachedAuths) return cachedAuths;

    const [c1, c2, c3] = await Promise.all([
        loadCredentials(),
        loadSalesCountCredentials(),
        loadLastUpdatedCredentials(),
    ]);

    // All-or-nothing: if any SA fails to load, log which and bail.
    if (!c1 || !c2 || !c3) {
        const missing = [
            !c1 && 'RawData (SA#1)',
            !c2 && 'SalesCount (SA#2)',
            !c3 && 'LastUpdated (SA#3)',
        ].filter(Boolean).join(', ');
        console.log(`[MERGE-REALTIME] Failed to load service account credentials for: ${missing}`);
        return null;
    }

    const mk = (c) => new JWT({
        email: c.client_email,
        key: c.private_key,
        scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });

    cachedAuths = [mk(c1), mk(c2), mk(c3)];
    return cachedAuths;
}

// Deterministic brand+marketplace -> SA index, so writes spread evenly across the SAs (round-robin).
function pickAuthIndex(key, count) {
    let h = 0;
    for (let i = 0; i < key.length; i++) h = (h * 31 + key.charCodeAt(i)) >>> 0;
    return h % count;
}

// Column letter (A, B, C...) for a header name on a sheet (sheets here have < 26 columns).
function colLetter(sheet, header) {
    const idx = sheet.headerValues.indexOf(header);
    return idx < 0 ? null : String.fromCharCode(65 + idx);
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
        const auths = await getAuthClients();
        if (!auths) {
            console.log(`[MERGE-REALTIME] Skipping ${brand}/${marketplace}: credentials unavailable`);
            return;
        }

        // One SA handles this whole merge (its single batched write lands on that SA's quota bucket).
        const auth = auths[pickAuthIndex(`${brand}|${marketplace}`, auths.length)];

        const doc = new GoogleSpreadsheet(prodSheetId, auth);
        await doc.loadInfo();

        const sheetRawData = doc.sheetsByIndex[0];
        const sheetSalesCount = doc.sheetsByIndex[1];
        const sheetLastUpdated = doc.sheetsByIndex[2];

        const [rowsRawData, rowsSalesCount, rowsLastUpdated] = await Promise.all([
            sheetRawData.getRows(),
            sheetSalesCount.getRows(),
            sheetLastUpdated.getRows(),
        ]);

        const now = new Date();
        const utc7Time = new Date(now.getTime() + (7 * 60 * 60 * 1000));
        const formattedTimestamp = utc7Time.toISOString().replace('T', ' ').substring(0, 19);

        const findRow = (rows) => rows.find(r => r.get('Brand') == brand && r.get('Platform') == marketplace);
        const rawRow = findRow(rowsRawData);
        const countRow = findRow(rowsSalesCount);
        const updatedRow = findRow(rowsLastUpdated);

        // Collect every cell to write; all three sheets go out in ONE batchUpdate request.
        const data = [];
        const cell = (sheet, row, header, value) => {
            if (!row) return;
            const col = colLetter(sheet, header);
            if (!col) return;
            data.push({ range: `'${sheet.title}'!${col}${row.rowNumber}`, values: [[value]] });
        };

        cell(sheetRawData, rawRow, 'GMV', sales_value);
        cell(sheetRawData, rawRow, 'Timestamp', formattedTimestamp);

        cell(sheetSalesCount, countRow, 'GMV', sales_value);
        cell(sheetSalesCount, countRow, 'Orders', orders_count);
        cell(sheetSalesCount, countRow, 'Timestamp', formattedTimestamp);

        // Last Updated only advances on a real sale (unchanged behaviour).
        if (sales_value > 0) {
            cell(sheetLastUpdated, updatedRow, 'GMV', sales_value);
            cell(sheetLastUpdated, updatedRow, 'Orders', orders_count);
            cell(sheetLastUpdated, updatedRow, 'Timestamp', formattedTimestamp);
        }

        if (data.length === 0) {
            console.log(`[MERGE-REALTIME] No matching rows for ${brand}/${marketplace}, nothing to write.`);
            return;
        }

        await auth.request({
            url: `https://sheets.googleapis.com/v4/spreadsheets/${prodSheetId}/values:batchUpdate`,
            method: 'POST',
            data: {
                valueInputOption: 'USER_ENTERED',
                data,
            },
        });

    } catch (e) {
        const apiMsg = e?.response?.data?.error?.message;
        console.log(`[MERGE-REALTIME] Error merging to sheets for ${brand}/${marketplace}: `, apiMsg || e.message || e);
    }
}
