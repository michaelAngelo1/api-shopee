import axios from 'axios';
import 'dotenv/config';

let token = null;
let tokenExpiresAt = 0;
export async function getToken() {
    if (token && Date.now() < tokenExpiresAt - 60_000) return token;

    const response = await axios.post(
        `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/oauth/access_token`,
        new URLSearchParams({
            grant_type: 'client_credentials',
            client_id: process.env.SHOPIFY_CLIENT_ID,
            client_secret: process.env.SHOPIFY_CLIENT_SECRET,
        }),
        {
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            timeout: 20000,
        }
    );

    const { access_token, expires_in } = response.data;
    token = access_token;
    tokenExpiresAt = Date.now() + expires_in * 1000;
    return token;
}

export async function graphql(query, variables = {}) {
    const response = await axios.post(
        `https://${process.env.SHOPIFY_SHOP}.myshopify.com/admin/api/2025-01/graphql.json`,
        { query, variables },
        {
            headers: {
                'Content-Type': 'application/json',
                'X-Shopify-Access-Token': await getToken(),
            },
            timeout: 20000,
        }
    );

    const { data, errors } = response.data;
    if (errors?.length) {
        throw new Error(`GraphQL errors: ${JSON.stringify(errors)}`);
    }
    return data;
}
