import { fetchAndProcessOrders } from "../processor.js";
import { fetchAndProcessOrdersCHESS } from "../workers/chess_processor.js";
import { fetchAndProcessOrdersCLEV } from "../workers/clev_processor.js";
import { fetchAndProcessOrdersDRJOU } from "../workers/drjou_processor.js";
import { fetchAndProcessOrdersEVOKE } from "../workers/evoke_processor.js";
import { fetchAndProcessOrdersGB } from "../workers/gb_processor.js";
import { fetchAndProcessOrdersIL } from "../workers/il_processor.js";
import { mainM2 } from "../workers/m2_processor.js";
import { fetchAndProcessOrdersMD } from "../workers/md_processor.js";
import { fetchAndProcessOrdersMIRAE } from "../workers/mirae_processor.js";
import { fetchAndProcessOrdersMMW } from "../workers/mmw_processor.js";
import { fetchAndProcessOrdersMOSS } from "../workers/moss_processor.js";
import { fetchAndProcessOrdersNB } from "../workers/nb_processor.js";
import { fetchAndProcessOrdersPN } from "../workers/pn_processor.js";
import { fetchAndProcessOrdersPOLY } from "../workers/poly_processor.js";
import { fetchAndProcessOrdersSHRD } from "../workers/shrd_processor.js";
import { fetchAndProcessOrdersSV } from "../workers/sv_processor.js";

async function mainRunner() {
    await fetchAndProcessOrders();
    await fetchAndProcessOrdersMMW();
    await fetchAndProcessOrdersSHRD();
    await fetchAndProcessOrdersMD();
    await fetchAndProcessOrdersPOLY();
    await fetchAndProcessOrdersCHESS();
    await fetchAndProcessOrdersCLEV();
    await fetchAndProcessOrdersMOSS();
    await fetchAndProcessOrdersEVOKE();
    await fetchAndProcessOrdersDRJOU();
    await fetchAndProcessOrdersMIRAE();
    await fetchAndProcessOrdersSV();
    await fetchAndProcessOrdersGB();
    await fetchAndProcessOrdersPN();
    await fetchAndProcessOrdersNB();
    await fetchAndProcessOrdersIL();
    await mainM2();
}

// await mainRunner();