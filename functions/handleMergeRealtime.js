import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import { GoogleSpreadsheet } from 'google-spreadsheet';
import { JWT } from 'google-auth-library';
const secretClient = new SecretManagerServiceClient();

async function loadCredentials() {
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

export async function handleMergeRealtime(brand, marketplace, sales_value) {
    console.log(`Start merging to sheets for brand: ${brand} from ${marketplace} with ${sales_value} sales today.`);

    const saCreds = await loadCredentials();
    // console.log("SA Creds: ", saCreds);

    const saAuth = new JWT({
        email: saCreds.client_email,
        key: saCreds.private_key,
        scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });

    let devSheetId = "1zArzQCqewtCxkka9l03bRpZLAjqOjapV6ngHUp0jluM"
    let prodSheetId = "1RMcbhi0wZgXqvYf_lFma2U8vznJh4ACy_OmsTjplgKI"

    const doc = new GoogleSpreadsheet(prodSheetId, saAuth);
    await doc.loadInfo();

    const sheet = doc.sheetsByIndex[0];
    
    const rows = await sheet.getRows();

    for(const row of rows) {
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
    
            if (row._rawData.length > 5) {
                row._rawData = row._rawData.slice(0, 5);
            }
    
            await row.save();

            setTimeout(() => {
            }, 3000)
        }

    }
}