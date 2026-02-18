import { handleMergeRealtime } from "./handleMergeRealtime";

export async function mainRealtimeTiktok(brand) {
    console.log("Main Realtime tiktok: ", brand);
    
    let totalSalesBrand = 0;
    let marketplace = "Tiktok";
    await handleMergeRealtime(brand, marketplace, totalSalesBrand);
}