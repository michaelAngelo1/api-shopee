import axios from 'axios';
import crypto from 'crypto';
import 'dotenv/config';
import { SecretManagerServiceClient } from '@google-cloud/secret-manager';
import { mainRealtime } from '../functions/handleRealtime.js';
import { graphql } from '../auth/shopifyAuth.js';
import { handleMergeRealtime } from '../functions/handleMergeRealtime.js';

const secretClient = new SecretManagerServiceClient();
export const PARTNER_ID = parseInt(process.env.POLY_PARTNER_ID);
export const PARTNER_KEY = process.env.POLY_PARTNER_KEY;
export const SHOP_ID = parseInt(process.env.PAZZO_SHOP_ID);

export let ACCESS_TOKEN;
let REFRESH_TOKEN;
export const HOST = "https://partner.shopeemobile.com";
const REFRESH_ACCESS_TOKEN_URL = "https://partner.shopeemobile.com/api/v2/auth/access_token/get";

export const formatJakartaTime = (isoString) => isoString ? new Date(new Date(isoString).getTime() + 7 * 3600000).toISOString().replace('T', ' ').slice(0, 19) : null;

async function refreshToken() {
    const path = "/api/v2/auth/access_token/get";
    const timestamp = Math.floor(Date.now() / 1000);
    const baseString = `${PARTNER_ID}${path}${timestamp}`;
    const sign = crypto.createHmac('sha256', PARTNER_KEY)
        .update(baseString)
        .digest('hex');

    const fullUrl = `${REFRESH_ACCESS_TOKEN_URL}?partner_id=${PARTNER_ID}&timestamp=${timestamp}&sign=${sign}`;

    const body = {
        refresh_token: REFRESH_TOKEN,
        partner_id: PARTNER_ID,
        shop_id: SHOP_ID
    }

    console.log("Hitting Refresh Token endpoint: ", fullUrl);

    const response = await axios.post(fullUrl, body, {
        headers: {
            'Content-Type': 'application/json'
        }
    });

    const newAccessToken = response.data.access_token;
    const newRefreshToken = response.data.refresh_token;

    if(newAccessToken && newRefreshToken) {
        ACCESS_TOKEN = newAccessToken;
        REFRESH_TOKEN = newRefreshToken;

        // saveTokensToFile({ accessToken: ACCESS_TOKEN, refreshToken: REFRESH_TOKEN });

        await saveTokensToSecret({ accessToken: ACCESS_TOKEN, refreshToken: REFRESH_TOKEN });
    } else {
        console.log("[PAZZO] Tokens dont exist");
    }
}

async function saveTokensToSecret(tokens) {
    const parent = 'projects/231801348950/secrets/pazzo-shopee-tokens';
    const payload = Buffer.from(JSON.stringify(tokens, null, 2), 'UTF-8');

    try {
        const [newVersion] = await secretClient.addSecretVersion({
            parent: parent,
            payload: {
                data: payload,
            }
        });

        console.log("Saved Shopee tokens to Secret Manager");

        // Destroying previous token version
        const [versions] = await secretClient.listSecretVersions({
            parent: parent
        });

        for (const version of versions) {
            if (version.name !== newVersion.name && version.state !== 'DESTROYED') {
                try {
                    await secretClient.destroySecretVersion({
                        name: version.name
                    });
                    console.log(`Destroyed old token version: ${version.name}`);
                } catch (destroyError) {
                    console.error(`Failed to destroy version ${version.name}:`, destroyError);
                }
            }
        }
    } catch (e) {
        console.log("Error saving tokens to Secret Manager: ", e);
    }
}

async function loadTokensFromSecret() {
    const secretName = 'projects/231801348950/secrets/pazzo-shopee-tokens/versions/latest';

    try {
        const [version] = await secretClient.accessSecretVersion({
            name: secretName,
        });
        const data = version.payload.data.toString('UTF-8');
        const tokens = JSON.parse(data);
        console.log("Tokens loaded from Secret Manager: ", tokens);
        return tokens;
    } catch (e) {
        console.log("Error loading tokens from Secret Manager: ", e);
    }
}

export async function getShopifyOrders() {
    try {
        const now = new Date();
        const wib = new Date(now.getTime() + 7 * 3600000);
        const startDate =
            `${wib.getUTCFullYear()}-${String(wib.getUTCMonth() + 1).padStart(2, '0')}-${String(wib.getUTCDate()).padStart(2, '0')}T00:00:00+07:00`;
        const endDate = now.toISOString(); 

        let hasNextPage = true;
        let cursor = null;

        let ordersData = [];
        while(hasNextPage) {
            const query =
                `{ 
                    orders(first: 20, query: "created_at:>=${startDate} created_at:<=${endDate}", after: ${cursor ? `"${cursor}"` : null}) {
                        edges {
                            cursor
                            node {
                                id
                                name
                                email
                                createdAt
                                currencyCode
                                displayFinancialStatus
                                displayFulfillmentStatus
                                paymentGatewayNames
                                shippingAddress {
                                    name
                                    phone
                                    address1
                                    city
                                    zip
                                    country
                                    province
                                }
                                transactions {
                                    id
                                    paymentId
                                    status
                                    createdAt
                                    processedAt
                                }
                                subtotalPriceSet {
                                    shopMoney {
                                        amount
                                    }
                                }
                                totalPriceSet {
                                    shopMoney {
                                        amount
                                    }
                                }
                                totalDiscountsSet {
                                    shopMoney {
                                        amount
                                    }
                                }
                                cancelledAt
                                cancelReason
                                closedAt
                                discountCode
                                shippingLine {
                                    title
                                    originalPriceSet {
                                        shopMoney {
                                            amount
                                        }
                                    }
                                }
                                lineItems(first: 20) {
                                    edges {
                                        cursor
                                        node {
                                            vendor
                                            id
                                            sku
                                            name
                                            quantity
                                            originalUnitPriceSet {
                                                shopMoney {
                                                    amount
                                                }
                                            }
                                            discountedUnitPriceSet {
                                                shopMoney {
                                                    amount
                                                }
                                            }
                                            originalTotalSet {
                                                shopMoney {
                                                    amount
                                                }
                                            }
                                            discountedTotalSet {
                                                shopMoney {
                                                    amount
                                                }
                                            }
                                            variant {
                                                inventoryItem {
                                                    measurement {
                                                        weight {
                                                            value
                                                            unit
                                                        }
                                                    }
                                                }
                                            }
                                        }   
                                    }
                                }
                            }
                        }
                        pageInfo {
                            hasNextPage
                            hasPreviousPage
                            startCursor
                            endCursor
                        }
                    }
                }`;
            const data = await graphql(query);

            if(data.orders.edges.length == 0) return ordersData;
            data.orders.edges.forEach(d => {
                ordersData.push(d);
            })

            if(data.orders.pageInfo.hasNextPage == false) hasNextPage = false;
            cursor = data.orders.pageInfo.endCursor;
        }
        console.log("Orders data: ", ordersData);
        return ordersData;
    } catch (e) {
        console.log("Error querying products: ", e);
    }
}

export async function mainPazzo() {
    let brand = "PAZZO";

    const loadedTokens = await loadTokensFromSecret();
    ACCESS_TOKEN = loadedTokens.accessToken;
    REFRESH_TOKEN = loadedTokens.refreshToken;

    await refreshToken();

    await mainRealtime(brand, PARTNER_ID, PARTNER_KEY, ACCESS_TOKEN, SHOP_ID);
    await mainPazzoWebRealtime();
}

async function mainPazzoWebRealtime() {
    const orders = await getShopifyOrders() ?? [];
    const flatShopifyOrders = orders.flatMap(s => {
        const transactions = s.node.transactions;
        if (transactions.length === 0) return [];
    
        const successfulTrx = transactions.find(t => t.status === "SUCCESS");
        const latestTrx = successfulTrx ?? [...transactions].sort((a, b) =>
            new Date(b.processedAt ?? b.createdAt) - new Date(a.processedAt ?? a.createdAt)
        )[0];
    
        return {
            order_id: s.node.id,
            order_name: s.node.name,
            order_created_at: formatJakartaTime(s.node.createdAt),
            cancelled_at: formatJakartaTime(s.node.cancelledAt),
            cancel_reason: s.node.cancelReason,
            closed_at: formatJakartaTime(s.node.closedAt),
            financial_status: s.node.displayFinancialStatus,
            fulfillment_status: s.node.displayFulfillmentStatus,
            payment_gateway_names: s.node.paymentGatewayNames.join(", "),
            order_payment_id: latestTrx.paymentId,
            order_paid_at: latestTrx.status !== "SUCCESS" ? null : formatJakartaTime(latestTrx.processedAt),
            order_transaction_status: latestTrx.status,
            discount_code: s.node.discountCode,
            total_discount: s.node.totalDiscountsSet.shopMoney.amount,
            shipping_method: s.node.shippingLine?.title ?? null,
            total_shipping: s.node.shippingLine?.originalPriceSet?.shopMoney?.amount ?? null,
            subtotal_price: s.node.subtotalPriceSet?.shopMoney?.amount ?? null,
            total_price: s.node.totalPriceSet.shopMoney.amount,
            currency_code: s.node.currencyCode,
            customer_name: s.node.shippingAddress?.name ?? null,
            customer_email: s.node.email,
            customer_phone: s.node.shippingAddress?.phone ?? null,
            customer_address: s.node.shippingAddress?.address1 ?? null,
            customer_zip: s.node.shippingAddress?.zip ?? null,
            customer_city: s.node.shippingAddress?.city ?? null,
            customer_country: s.node.shippingAddress?.country ?? null,
            customer_province: s.node.shippingAddress?.province ?? null,
        }
    });
    // console.log("Flat orders: ", flatShopifyOrders);
    console.log("Flat orders GMV: ", flatShopifyOrders.reduce((i, o) => { return i + parseInt(o.subtotal_price) }, 0));
    
    const salesValue = flatShopifyOrders.reduce((i, o) => { return i + parseInt(o.subtotal_price)}, 0);
    const ordersCount = flatShopifyOrders.length;
    await handleMergeRealtime("PAZZO", "Website", salesValue, ordersCount);
}

// await mainPazzoWebRealtime();