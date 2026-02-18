const RTO_ENABLED_STATES = new Set(["TN", "KA", "KL", "AP", "MH", "TS", "PY"]);

const CATEGORY_KEYS = {
  twoWheeler: "two wheeler",
  privateCar: "private car",
  pcv: "pcv",
  misc: "misc",
  gcv: "gcv",
};

const PRIVATE_CAR_MODEL_MAP = {
  honda: [],
  hyundai: ["Getz"],
  kia: [],
  mahindra: ["Bolero"],
  maruti: ["Eeco", "Omni"],
  tata: ["Indica", "Indigo", "Sumo"],
  toyota: ["Qualis"],
  others: ["Chevrolet", "GM", "Obsolete Models", "Tavera"],
};

const GCV_4W_MODEL_MAP = {
  tata: ["Ace", "Intra", "Magic", "Super Ace", "Xenon", "Tipper", "Truck", "Tanker"],
  "tata motors": ["Ace", "Intra", "Magic", "Super Ace", "Xenon", "Tipper", "Truck", "Tanker"],
  mahindra: ["Mahindra Jeeto", "Supro", "Yodha", "Tractor", "Tipper", "Truck", "Tanker"],
  "mahindra & mahindra": ["Mahindra Jeeto", "Supro", "Yodha", "Tractor", "Tipper", "Truck", "Tanker"],
  maruti: ["Super Carry"],
  "maruti suzuki": ["Super Carry"],
  ashok: ["Tipper", "Truck", "Tanker"],
  others: ["Tipper", "Truck", "Tanker"],
};

const TWO_WHEELER_MAKE_CATEGORY_MAP = {
  ather: "scooter",
  bajaj: "both",
  hero: "bike",
  honda: "scooter",
  piaggio: "scooter",
};

const form = document.getElementById("payout-form");
const submitButton = form.querySelector('button[type="submit"]');

const el = {
  state: document.getElementById("state"),
  rtoNumber: document.getElementById("rto-number"),
  rtoPrefix: document.getElementById("rto-prefix"),
  vehicleCategory: document.getElementById("vehicle-category"),
  vehicleType: document.getElementById("vehicle-type"),
  make: document.getElementById("make"),
  model: document.getElementById("model"),
  fuelType: document.getElementById("fuel-type"),
  ccSlab: document.getElementById("cc-slab"),
  wattSlab: document.getElementById("watt-slab"),
  seatingCapacity: document.getElementById("seating-capacity"),
  trailer: document.getElementById("trailer"),
  vehicleAge: document.getElementById("vehicle-age"),
  gvwSlab: document.getElementById("gvw-slab-select"),
  businessType: document.getElementById("business-type"),
  policyType: document.getElementById("policy-type"),
};

const group = {
  rto: document.getElementById("rto-group"),
  vehicleType: document.getElementById("vehicle-type-group"),
  make: document.getElementById("make-group"),
  model: document.getElementById("model-group"),
  fuel: document.getElementById("fuel-group"),
  cc: document.getElementById("cc-group"),
  watt: document.getElementById("watt-group"),
  seating: document.getElementById("seating-group"),
  trailer: document.getElementById("trailer-group"),
  gvw: document.getElementById("gvw-group"),
  business: document.getElementById("business-group"),
  policy: document.getElementById("policy-group"),
};

const results = {
  section: document.getElementById("results-section"),
  message: document.getElementById("results-message"),
  table: document.getElementById("results-table"),
  body: document.getElementById("results-body"),
};

let selectedStateCode = "";

function norm(v) {
  return (v || "").toString().trim().toLowerCase();
}

function normCategory(v) {
  return norm(v).replace(/[-_]/g, " ");
}

function isCategory(value, categoryKey) {
  const n = normCategory(value);
  if (categoryKey === CATEGORY_KEYS.twoWheeler) return n.includes("two wheeler");
  if (categoryKey === CATEGORY_KEYS.privateCar) return n.includes("private car");
  if (categoryKey === CATEGORY_KEYS.pcv) return n === "pcv" || n.includes("passenger");
  if (categoryKey === CATEGORY_KEYS.misc) return n.includes("misc");
  if (categoryKey === CATEGORY_KEYS.gcv) return n === "gcv" || n.includes("goods");
  return false;
}

function isAutoVehicleType(value) {
  return /^auto(\b|$)/i.test((value || "").toString().trim());
}

function isGcvFourWheeler(value) {
  const n = normCategory(value);
  return n.includes("4 wheeler") || n.includes("4 wheeler goods");
}

function isGcvFlatbed(value) {
  return normCategory(value).includes("flatbed");
}

function isGcvThreeWheeler(value) {
  const n = normCategory(value);
  return n.includes("3 wheeler") || n.includes("3 wheeler goods");
}

function isPcvBusNoMakeModel(value) {
  const n = norm(value);
  return n.startsWith("educational bus") || n.startsWith("staff bus");
}

function isPcvTaxi(value) {
  return norm(value) === "taxi";
}

function createOption(value, label) {
  const option = document.createElement("option");
  option.value = value;
  option.textContent = label;
  return option;
}

function resetSelect(select, placeholderText) {
  select.innerHTML = "";
  select.appendChild(createOption("", placeholderText));
}

function getSelectableOptions(select) {
  return Array.from(select.options || []).filter(
    (opt) => !!opt.value && !opt.disabled && !opt.hidden
  );
}

function hasOptionValue(select, value) {
  return Array.from(select.options || []).some(
    (opt) => !opt.hidden && !opt.disabled && opt.value === value
  );
}

function restoreOrAutoSelect(select, previousValue = "") {
  if (previousValue && hasOptionValue(select, previousValue)) {
    select.value = previousValue;
    return false;
  }
  if (select.value) return false;
  const selectable = getSelectableOptions(select);
  if (selectable.length === 1) {
    select.value = selectable[0].value;
    return true;
  }
  return false;
}

function setGroupVisible(groupEl, visible) {
  groupEl.style.display = visible ? "flex" : "none";
}

function applyVehicleDetailFieldOrder() {
  const category = el.vehicleCategory.value;
  const isTW = isCategory(category, CATEGORY_KEYS.twoWheeler);
  const sectionBody = group.make?.parentElement;
  if (!sectionBody) return;

  // Always keep Make before Model.
  if (group.make && group.model && group.make.nextElementSibling !== group.model) {
    sectionBody.insertBefore(group.make, group.model);
  }

  // Two Wheeler: Fuel Type first, then Make, then Model.
  if (isTW) {
    if (group.fuel && group.make) {
      sectionBody.insertBefore(group.fuel, group.make);
    }
    return;
  }

  // Default order for other categories: Make, Model, then Fuel Type.
  if (group.model && group.fuel) {
    sectionBody.insertBefore(group.model, group.fuel);
  }
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return await res.json();
}

function appendDistinctOptions(select, values, includeOthers = false) {
  const seen = new Set();
  values.forEach((value) => {
    const raw = (value || "").toString().trim();
    let s = raw;
    if (
      select === el.vehicleType &&
      /^digger\s*&\s*boring machine$/i.test(raw)
    ) {
      s = "Digger and Boring machine";
    }
    if (
      select === el.vehicleType &&
      /^educational bus under school name$/i.test(raw)
    ) {
      s = "Educational Bus";
    }
    if (!s) return;
    const key = s.toLowerCase();
    if (key.startsWith("except ") || key.startsWith("declined ")) return;
    if (seen.has(key)) return;
    seen.add(key);
    let label = s;
    select.appendChild(createOption(s, label));
  });

  if (includeOthers && !seen.has("others")) {
    select.appendChild(createOption("Others", "Others"));
  }
}

function parseCcSlabForSort(value) {
  const s = (value || "").toString().trim().toLowerCase();
  const num = (v) => Number(v);
  let m = s.match(/^(?:up\s*to|upto|below)\s*(\d+(?:\.\d+)?)/i);
  if (m) return { min: 0, max: num(m[1]) };
  m = s.match(/^above\s*(\d+(?:\.\d+)?)/i);
  if (m) return { min: num(m[1]), max: Number.POSITIVE_INFINITY };
  m = s.match(/(\d+(?:\.\d+)?)\s*(?:to|-)\s*(\d+(?:\.\d+)?)/i);
  if (m) return { min: num(m[1]), max: num(m[2]) };
  m = s.match(/(\d+(?:\.\d+)?)/);
  if (m) return { min: num(m[1]), max: num(m[1]) };
  return { min: Number.POSITIVE_INFINITY, max: Number.POSITIVE_INFINITY };
}

function sortCcSlabsAscending(values) {
  return [...(values || [])].sort((a, b) => {
    const pa = parseCcSlabForSort(a);
    const pb = parseCcSlabForSort(b);
    if (pa.min !== pb.min) return pa.min - pb.min;
    if (pa.max !== pb.max) return pa.max - pb.max;
    return String(a).localeCompare(String(b));
  });
}

function applyBusinessTypeDisplayLabels() {}

function isBundlePolicyType(value) {
  const n = norm(value).replace(/\s+/g, "");
  return n === "bundle(1+3)" || n === "bundle(1+5)" || n === "bundle(5+5)";
}

function isSatpPolicyType(value) {
  return norm(value).replace(/\s+/g, "") === "satp";
}

function isVehicleAgeNew(value) {
  const v = (value || "").toString().trim().toLowerCase();
  return v === "new";
}

function normalizeMakeKey(value) {
  return norm(value).replace(/\s+/g, " ").trim();
}

function setSelectLocked(selectEl, locked) {
  if (!selectEl) return;
  selectEl.dataset.locked = locked ? "true" : "false";
  selectEl.style.pointerEvents = locked ? "none" : "auto";
  selectEl.classList.toggle("select-locked", !!locked);
  if (locked) {
    selectEl.setAttribute("aria-readonly", "true");
  } else {
    selectEl.removeAttribute("aria-readonly");
  }
}

function applyBusinessTypeRules(trigger = "auto") {
  const initialAge = (el.vehicleAge.value || "").toString().trim();
  const selectedBusinessNorm = norm(el.businessType.value || "");
  const initialPolicy = el.policyType.value || "";
  let bundleSelected = isBundlePolicyType(initialPolicy);

  // User can start from Business Type or Policy Type:
  // If business is New OR policy is Bundle, force Vehicle Age to New (or 1 fallback).
  if (trigger !== "age" && (selectedBusinessNorm === "new" || bundleSelected) && !isVehicleAgeNew(initialAge)) {
    const hasNewAge = Array.from(el.vehicleAge.options || []).some((o) => norm(o.value) === "new");
    el.vehicleAge.value = hasNewAge ? "New" : "1";
  }

  const finalAge = (el.vehicleAge.value || "").toString().trim();
  const ageIsNew = isVehicleAgeNew(finalAge);
  const policy = el.policyType.value || "";
  bundleSelected = isBundlePolicyType(policy);
  const ageNum = /^\d+$/.test(finalAge) ? Number(finalAge) : null;
  const age16Plus = ageNum !== null && ageNum >= 16;

  // Age rule: if age is not New Vehicle, hide all Bundle policy options.
  const policyOptions = Array.from(el.policyType.options || []);
  policyOptions.forEach((opt) => {
    if (!opt.value) return;
    const isBundle = isBundlePolicyType(opt.value);
    const isSatp = isSatpPolicyType(opt.value);
    const hideBundle = !ageIsNew && isBundle;
    const hideByAge16Rule = age16Plus && !isSatp;
    const hide = hideBundle || hideByAge16Rule;
    opt.hidden = hide;
    opt.disabled = hide;
  });
  const selectedOption = Array.from(el.policyType.options || []).find(
    (opt) => opt.value === el.policyType.value
  );
  if (selectedOption && (selectedOption.hidden || selectedOption.disabled)) {
    el.policyType.value = "";
  }
  if (age16Plus) {
    const satpOption = Array.from(el.policyType.options || []).find(
      (opt) => !opt.hidden && !opt.disabled && isSatpPolicyType(opt.value)
    );
    el.policyType.value = satpOption ? satpOption.value : "";
  }
  bundleSelected = isBundlePolicyType(el.policyType.value || "");

  // Business rule:
  // - Age = New Vehicle -> Business locked to New
  // - Age = any numeric year -> Business locked to Old
  // - Bundle selected -> Business locked to New
  const ageSelected = finalAge !== "";
  let forcedBusiness = null;
  if (bundleSelected || ageIsNew) {
    forcedBusiness = "new";
  } else if (ageSelected) {
    forcedBusiness = "old";
  }

  let forcedOptionValue = "";
  Array.from(el.businessType.options).forEach((opt) => {
    const valueNorm = norm(opt.value);
    if (!opt.value) {
      opt.disabled = !!forcedBusiness;
      return;
    }
    if (forcedBusiness) {
      const keep = valueNorm === forcedBusiness;
      opt.hidden = !keep;
      opt.disabled = !keep;
      if (keep) forcedOptionValue = opt.value;
    } else {
      opt.hidden = false;
      opt.disabled = false;
    }
  });

  if (forcedBusiness) {
    el.businessType.value = forcedOptionValue || "";
  }

  // Lock rules:
  // 1) Age selected => Business locked to forced value.
  // 2) Bundle policy => both Age and Business are locked.
  const lockBusiness = !!forcedBusiness;
  const lockAge = bundleSelected;
  setSelectLocked(el.businessType, lockBusiness);
  setSelectLocked(el.vehicleAge, lockAge);
}

function appendRtoOptions(select, rtoOptions) {
  const seen = new Set();
  (rtoOptions || []).forEach((opt) => {
    const code = (opt?.code || "").toString().trim();
    if (!code) return;
    if (norm(code) === "others") return;
    const key = code.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);

    const labelRaw = (opt?.label || "").toString().trim();
    const name = (opt?.name || "").toString().trim();
    const label = labelRaw || (name ? `${code} - ${name}` : code);
    select.appendChild(createOption(code, label));
  });
}

function showNoData(message) {
  results.message.textContent = message;
  results.message.style.display = "block";
  results.table.style.display = "none";
  results.section.style.display = "block";
}

function showRows(payouts) {
  results.message.style.display = "none";
  results.body.innerHTML = "";
  const grouped = [];
  const groupMap = new Map();

  payouts.forEach((payout, idx) => {
    const payoutValueRaw =
      payout.payout_percentage ?? payout.final_payout ?? payout.payout ?? 0;
    const payoutValue = Number(payoutValueRaw);
    const payoutText = Number.isFinite(payoutValue)
      ? payoutValue.toFixed(2)
      : String(payoutValueRaw || "");
    const rawCondition = (payout.conditions || "").toString().trim();
    let condition = rawCondition;
    const conditionReplacements = [
      [/\b2nd\s+year\s+from\s+bundle\b/gi, "2nd year from the bundle"],
      [/\b3rd\s+year\s+from\s+bundle\b/gi, "3rd year from the bundle"],
      [/\b4th\s+year\s+from\s+bundle\b/gi, "4th year from the bundle"],
      [/\b5th\s+year\s+from\s+bundle\b/gi, "5th year from the bundle"],
      [/\b50%\s*commission\s*in\s*first\s*year,\s*50%\s*in\s*second\s*year\b/gi, "50% commission in Year 1 and 50% in Year 2"],
      [/\b50%\s*first\s*year,\s*50%\s*second\s*year\b/gi, "50% in Year 1 and 50% in Year 2"],
      [/\bAbove\s+2000\s+Watt\b/gi, "Above 2000 W"],
      [/\bUpto\s+2000\s+Watt\b/gi, "Up to 2000 W"],
      [/\bAdditional\s+payout\s+2\.5%\b/gi, "Additional payout: 2.5%"],
      [/\bAge\s+upto\s+15\s+years\b/gi, "Vehicle age up to 15 years"],
      [/\bupto\s+15\s+years\b/gi, "Up to 15 years"],
      [/\bAs\s+per\s+Approved\s+RTO,\s*Discount\s+below\s+85%\s*,\s*NCB\s+is\s+there\b/gi, "As per approved RTO, discount below 85%, With NCB"],
      [/\bAs\s+per\s+Approved\s+RTO,\s*NO\s+NCB\b/gi, "As per approved RTO, Without NCB"],
      [/\bAs\s+per\s+Approved\s+RTO\b/gi, "As per approved RTO"],
      [/\bAs\s+per\s+System\s+RTO\b/gi, "As per system RTO"],
      [/\bBelow\s+30\s+days\s*&\s*live\s+case\b/gi, "Break-in below 30 days (live case)"],
      [/\bBelow\s+30\s+days\s+and\s+live\s+case\b/gi, "Break-in below 30 days (live case)"],
      [/\bBelow\s+30\s+days\s+and\s+live\s+cases\b/gi, "Break-in below 30 days (live cases)"],
      [/\bBreak\s+in\s+30\s+days\b/gi, "Break-in within 30 days"],
      [/\bBreak\s+in\s+Above\s+30\s+days\b/gi, "Break-in above 30 days"],
      [/\bChennai\s+ID,\s*Commission\s+on\s+1st\s+year\s+premium\b/gi, "Chennai ID, commission on 1st-year premium only"],
      [/\bCommission\s+for\s+Chennai\s+ID,\s*Only\s+brand\s+new\s+Comprehensive\(1\+1\)\b/gi, "Chennai ID, only for brand-new Comprehensive (1+1)"],
      [/\bcommission\s+for\s+Chennai\s+ID,\s*Commission\s+on\s+TP\b/gi, "Chennai ID, commission on TP only"],
      [/\bChennai\s+RTO\s+Only\b/gi, "Chennai RTO only"],
      [/\bfor\s+Chennai\s+ID\b/gi, "For Chennai ID only"],
      [/\bCommission\s+on\s+Chennai\s+ID\b/gi, "Commission applicable for Chennai ID"],
      [/\bcommission\s+for\s+Chennai\s+ID\b/gi, "Commission applicable for Chennai ID"],
      [/\bChennai\s+ID\b/gi, "Chennai ID only"],
      [/\bCoimbatore\s*,\s*Madurai\s*,\s*Trichy\b/gi, "Coimbatore, Madurai, Trichy"],
      [/\bCommission\s+on\s+First\s+Year\s+Premium\b/gi, "Commission on 1st-year premium only"],
      [/\bcommission\s+on\s+Kerala\s+ID\b/gi, "Commission applicable for Kerala ID"],
      [/\bKerala\s+ID\b/gi, "Kerala ID only"],
      [/\bcommission\s+only\s+for\s+first\s+year\s+premium;\s*on\s+OD\b/gi, "Commission on OD, 1st-year premium only"],
      [/\bcommission\s+only\s+for\s+first\s+year\s+premium;\s*on\s+TP\b/gi, "Commission on TP, 1st-year premium only"],
      [/\bCommission\s+on\s+OD\s*,\s*NCB\s+is\s+there\b/gi, "Commission on OD, With NCB"],
      [/\bCommission\s+on\s+OD\s*,\s*NO\s+NCB\b/gi, "Commission on OD, Without NCB"],
      [/\bCommission\s+on\s+OD,\s*No\s+Zero\s+Depreciation,\s*Upto\s+90%\s+Discount\b/gi, "Commission on OD, no Zero Depreciation, discount up to 90%"],
      [/\bCommission\s+on\s+OD\s+only\b/gi, "Commission on OD"],
      [/\bCommission\s+on\s+TP\s+only\b/gi, "Commission on TP"],
      [/\bcommission\s+on\s+OD\b/gi, "Commission on OD"],
      [/\bcommission\s+on\s+TP\b/gi, "Commission on TP"],
      [/\bCPA\s+is\s+not\s+mandatory\s+here\b/gi, "CPA is not mandatory"],
      [/\bCPA\s+mandatory\s+with\s+NO\s+Zero\s+depreciation\b/gi, "CPA is mandatory with no Zero Depreciation"],
      [/\bCPA\s+mandatory\b/gi, "CPA is mandatory"],
      [/\bDiscount\s+40%\b/gi, "Discount: 40%"],
      [/\bDiscount\s+45%\b/gi, "Discount: 45%"],
      [/\bDiscount\s+50%\b/gi, "Discount: 50%"],
      [/\bNCB\s*>\s*25%\s*,\s*Commission\s+on\s+OD\b/gi, "NCB above 25%, commission on OD"],
      [/\bNCB\s+is\s+there\b/gi, "With NCB"],
      [/\bNCB\s+applicable\b/gi, "With NCB"],
      [/\bNCB\s+not\s+applicable\b/gi, "Without NCB"],
      [/\bNO\s+NCB\b/gi, "Without NCB"],
      [/\bWith\s+Zero\s+depreciation\b/gi, "With Zero Depreciation"],
      [/\bwith\s+No\s+Zero\s+depreciation\b/gi, "With no Zero Depreciation"],
      [/\bNo\s+Zero\s+Depreciation\s*\(76%\s*to\s*80%\)\b/gi, "No Zero Depreciation (76% to 80% discount)"],
      [/\bNo\s+Zero\s+Depreciation\s*\(81%\s*to\s*85%\)\b/gi, "No Zero Depreciation (81% to 85% discount)"],
      [/\bNo\s+Zero\s+Depreciation\s*\(86%\s*to\s*90%\)\b/gi, "No Zero Depreciation (86% to 90% discount)"],
      [/\bNo\s+Zero\s+Depreciation\s*\(upto\s*75%\)\b/gi, "No Zero Depreciation (up to 75% discount)"],
      [/\bNo\s+Zero\s+Depreciation\s*\(upto\s*80%\)\b/gi, "No Zero Depreciation (up to 80% discount)"],
      [/\bNo\s+Zero\s+Depreciation\s*,\s*Commission\s+on\s+OD\b/gi, "No Zero Depreciation, commission on OD"],
      [/\bNo\s+Zero\s+Depreciation\s*,\s*Upto\s+90%\s+Discount\b/gi, "No Zero Depreciation, discount up to 90%"],
      [/\bOnly\s+System\s+discount\b/gi, "Only system discount applicable"],
      [/\bupto\s+85%\s+Discount\b/gi, "Discount up to 85%"],
      [/\bupto\s+90%\s+Discount\b/gi, "Discount up to 90%"],
      [/\bwith\s+NCB\s+and\s+CPA\b/gi, "With NCB and CPA"],
      [/\bwithout\s+NCB\s+and\s+CPA\b/gi, "Without NCB and CPA"],
      [/\bwith\s+NCB\s+upto\s+15\s+years\b/gi, "With NCB, up to 15 years"],
      [/\bwithout\s+NCB\s+upto\s+15\s+years\b/gi, "Without NCB, vehicle age up to 15 years"],
      [/\bwith\s+Nil\s+Deep\b/gi, "With Zero Depreciation"],
      [/\bwithout\s+addon\s+coverage,\s*Commission\s+on\s+Net\b/gi, "Without add-on cover, commission on net premium"],
      [/\bWithout\s+any\s+addon\s+coverage,\s*Commission\s+on\s+Net\b/gi, "Without any add-on cover, commission on net premium"],
    ];
    conditionReplacements.forEach(([pattern, replacement]) => {
      condition = condition.replace(pattern, replacement);
    });
    const company = payout.company_name || payout.company || "Unknown";
    const rank = payout.rank || idx + 1;
    const key = `${rank}__${company.toLowerCase()}`;
    if (!groupMap.has(key)) {
      const entry = {
        rank,
        company,
        lines: [],
      };
      grouped.push(entry);
      groupMap.set(key, entry);
    }
    const entry = groupMap.get(key);
    entry.lines.push({
      condition,
      payoutText,
    });
  });

  grouped.forEach((entry) => {
    const conditionedLines = entry.lines.filter((line) => !!line.condition);
    const displayLines = conditionedLines.length > 0 ? conditionedLines : entry.lines;
    const hasConditionBlock = conditionedLines.length > 0;

    const conditionLines = displayLines
      .map((line) => `<div class="company-note">${line.condition}</div>`)
      .join("");
    const payoutLines = displayLines
      .map((line) => `<div class="payout-line">${line.payoutText}</div>`)
      .join("");
    const companyHtml = hasConditionBlock
      ? `<div class="company-header"><strong>${entry.company}</strong></div><div class="company-lines">${conditionLines}</div>`
      : `<div class="company-header"><strong>${entry.company}</strong></div>`;
    const payoutHtml = hasConditionBlock
      ? `<div class="payout-header-spacer"></div><div class="payout-lines">${payoutLines}</div>`
      : `<div class="payout-lines">${payoutLines}</div>`;

    const row = document.createElement("tr");
    row.innerHTML = `
      <td class="table-rank">${entry.rank}</td>
      <td class="table-company">${companyHtml}</td>
      <td class="table-payout">${payoutHtml}</td>
    `;
    results.body.appendChild(row);
  });
  results.table.style.display = "table";
  results.section.style.display = "block";
}

async function populateStates() {
  const data = await fetchJSON("/api/states");
  resetSelect(el.state, "-- Choose State --");
  appendDistinctOptions(el.state, data.states || [], false);
  const hasOthers = Array.from(el.state.options).some(
    (opt) => norm(opt.value) === "others"
  );
  if (!hasOthers) el.state.appendChild(createOption("Others", "Others"));
}

async function populateCategories() {
  const data = await fetchJSON("/api/vehicle-categories");
  resetSelect(el.vehicleCategory, "-- Choose Category --");
  appendDistinctOptions(el.vehicleCategory, data.categories || []);
}

async function populateVehicleAges() {
  const data = await fetchJSON("/api/vehicle-ages");
  resetSelect(el.vehicleAge, "-- Choose Vehicle Age --");
  const rawAges = (data.ages || [])
    .map((ageVal) => (ageVal || "").toString().trim())
    .filter(Boolean);

  const hasNew = rawAges.some((v) => v.toLowerCase() === "new");
  if (hasNew) {
    el.vehicleAge.appendChild(createOption("New", "New Vehicle"));
  }

  // UI display rule:
  // Show years in descending order (currentYear-1, currentYear-2 ... currentYear-50),
  // while keeping option values as numeric ages (1..50) for backend matching.
  const currentYear = new Date().getFullYear();
  const numericAges = rawAges
    .filter((v) => /^\d+$/.test(v))
    .map((v) => Number(v))
    .filter((n) => Number.isFinite(n) && n >= 1 && n <= 50)
    .sort((a, b) => a - b);

  numericAges.forEach((age) => {
    const labelYear = String(currentYear - age);
    el.vehicleAge.appendChild(createOption(String(age), labelYear));
  });
}

async function onStateChange() {
  const stateVal = el.state.value;
  const prevRto = el.rtoNumber.value;
  selectedStateCode = "";
  el.rtoPrefix.textContent = "XX";
  resetSelect(el.rtoNumber, "-- Choose RTO --");

  if (!stateVal) {
    setGroupVisible(group.rto, false);
    return;
  }

  const stateCodeRes = await fetchJSON(
    `/api/state-code/${encodeURIComponent(stateVal)}`
  );
  selectedStateCode = (stateCodeRes.code || "").toString().trim().toUpperCase();
  el.rtoPrefix.textContent = selectedStateCode || "XX";

  if (!RTO_ENABLED_STATES.has(selectedStateCode)) {
    setGroupVisible(group.rto, false);
    el.rtoNumber.value = "";
    return;
  }

  setGroupVisible(group.rto, true);
  const rtoData = await fetchJSON(`/api/rtos/${encodeURIComponent(selectedStateCode)}`);
  resetSelect(el.rtoNumber, "-- Choose RTO --");
  if (Array.isArray(rtoData.rto_options) && rtoData.rto_options.length) {
    appendRtoOptions(el.rtoNumber, rtoData.rto_options);
  } else {
    appendDistinctOptions(el.rtoNumber, rtoData.rtos || [], false);
  }
  Array.from(el.rtoNumber.options).forEach((opt) => {
    if (norm(opt.value) === "others") opt.remove();
  });
  restoreOrAutoSelect(el.rtoNumber, prevRto);
}

async function reloadBusinessAndPolicy() {
  const prevBusiness = el.businessType.value;
  const prevPolicy = el.policyType.value;
  const vehicleType = el.vehicleType.value || "";
  const fuelType = el.fuelType.value || "";
  const category = el.vehicleCategory.value || "";

  const [businessData, policyData] = await Promise.all([
    fetchJSON(
      `/api/business-types?vehicle_type=${encodeURIComponent(
        vehicleType
      )}&fuel_type=${encodeURIComponent(fuelType)}&category=${encodeURIComponent(category)}`
    ),
    fetchJSON(
      `/api/policy-types?vehicle_type=${encodeURIComponent(
        vehicleType
      )}&fuel_type=${encodeURIComponent(fuelType)}&category=${encodeURIComponent(category)}`
    ),
  ]);

  resetSelect(el.businessType, "-- Choose Business Type --");
  appendDistinctOptions(el.businessType, businessData.business_types || []);
  applyBusinessTypeDisplayLabels();
  restoreOrAutoSelect(el.businessType, prevBusiness);

  resetSelect(el.policyType, "-- Choose Policy Type --");
  appendDistinctOptions(el.policyType, policyData.policies || []);
  restoreOrAutoSelect(el.policyType, prevPolicy);
  applyBusinessTypeRules();
  if (!el.businessType.value) restoreOrAutoSelect(el.businessType);
  if (!el.policyType.value) restoreOrAutoSelect(el.policyType);
}

async function reloadMakesAndModels() {
  const prevMake = el.make.value;
  const prevModel = el.model.value;
  const category = el.vehicleCategory.value;
  const vehicleType = el.vehicleType.value;
  const fuelType = el.fuelType.value || "";
  const isTwoWheeler = isCategory(category, CATEGORY_KEYS.twoWheeler);
  const fuelSelected = !!(el.fuelType.value || "").trim();
  const showMakeModel =
    isCategory(category, CATEGORY_KEYS.twoWheeler) ||
    isCategory(category, CATEGORY_KEYS.privateCar) ||
    isCategory(category, CATEGORY_KEYS.pcv) ||
    isCategory(category, CATEGORY_KEYS.gcv);

  if (!showMakeModel) {
    resetSelect(el.make, "-- Choose Make --");
    resetSelect(el.model, "-- Choose Model --");
    return;
  }

  // Two Wheeler rule: Fuel type must be selected before showing/populating Make.
  if (isTwoWheeler && !fuelSelected) {
    resetSelect(el.make, "-- Choose Make --");
    resetSelect(el.model, "-- Choose Model --");
    return;
  }

  const makeData = await fetchJSON(
    `/api/makes?vehicle_type=${encodeURIComponent(vehicleType || "")}&category=${encodeURIComponent(category || "")}&fuel_type=${encodeURIComponent(fuelType)}`
  );
  let makeValues = makeData.makes || [];

  // Two Wheeler fallback:
  // if subtype-specific makes are too narrow (e.g., only Others), fetch category+fuel makes.
  if (isTwoWheeler) {
    const nonOther = makeValues.filter((m) => norm(m) !== "others");
    if (nonOther.length === 0) {
      const broadMakeData = await fetchJSON(
        `/api/makes?category=${encodeURIComponent(category || "")}&fuel_type=${encodeURIComponent(fuelType)}`
      );
      makeValues = broadMakeData.makes || makeValues;
    }
    // Two Wheeler EV make-category mapping:
    // Bike -> BIKE/BOTH makes, Scooter -> SCOOTER/BOTH makes.
    if (norm(fuelType) === "ev") {
      const vt = norm(vehicleType);
      if (vt === "bike" || vt === "scooter") {
        makeValues = makeValues.filter((m) => {
          const key = norm(m);
          if (key === "others") return true;
          const mkCat = TWO_WHEELER_MAKE_CATEGORY_MAP[key];
          if (!mkCat) return false;
          return mkCat === "both" || mkCat === vt;
        });
      }
    }
  }

  resetSelect(el.make, "-- Choose Make --");
  appendDistinctOptions(el.make, makeValues, true);
  const makeChanged = restoreOrAutoSelect(el.make, prevMake);
  if (makeChanged && el.make.value) {
    await reloadModelsByMake();
    return;
  }

  resetSelect(el.model, "-- Choose Model --");
  const modelData = await fetchJSON(
    `/api/models?vehicle_type=${encodeURIComponent(
      vehicleType || ""
    )}&category=${encodeURIComponent(category || "")}`
  );
  appendDistinctOptions(el.model, modelData.models || [], true);
  restoreOrAutoSelect(el.model, prevModel);
  if (!el.model.value) restoreOrAutoSelect(el.model);
}

async function reloadModelsByMake() {
  const prevModel = el.model.value;
  const category = el.vehicleCategory.value;
  const isPrivateCar = isCategory(category, CATEGORY_KEYS.privateCar);
  const fuelType = el.fuelType.value || "";
  const isGcv4W = isCategory(category, CATEGORY_KEYS.gcv) && isGcvFourWheeler(el.vehicleType.value);
  const showMakeModel =
    isCategory(category, CATEGORY_KEYS.twoWheeler) ||
    isCategory(category, CATEGORY_KEYS.privateCar) ||
    isCategory(category, CATEGORY_KEYS.pcv) ||
    isCategory(category, CATEGORY_KEYS.gcv);
  if (!showMakeModel || isAutoVehicleType(el.vehicleType.value)) return;

  const make = el.make.value;
  const vehicleType = el.vehicleType.value || "";
  resetSelect(el.model, "-- Choose Model --");

  if (isGcv4W) {
    const makeKey = normalizeMakeKey(make || "others");
    const mapped = GCV_4W_MODEL_MAP[makeKey] || [];
    if (mapped.length > 0) {
      appendDistinctOptions(el.model, mapped, true);
      restoreOrAutoSelect(el.model, prevModel);
      if (!el.model.value) restoreOrAutoSelect(el.model);
      return;
    }
    appendDistinctOptions(el.model, [], true);
    restoreOrAutoSelect(el.model, prevModel);
    if (!el.model.value) restoreOrAutoSelect(el.model);
    return;
  }

  if (isPrivateCar) {
    const makeKey = norm(make || "others");
    const exactModels = PRIVATE_CAR_MODEL_MAP[makeKey] || [];
    if (exactModels.length > 0) {
      appendDistinctOptions(el.model, exactModels, true);
      restoreOrAutoSelect(el.model, prevModel);
      if (!el.model.value) restoreOrAutoSelect(el.model);
      return;
    }
    // If make has no dedicated model list (Honda/Kia/Mahindra currently),
    // keep only 'Others' to avoid showing unrelated models.
    appendDistinctOptions(el.model, [], true);
    restoreOrAutoSelect(el.model, prevModel);
    if (!el.model.value) restoreOrAutoSelect(el.model);
    return;
  }

  if (!make || norm(make) === "others") {
    const allModelData = await fetchJSON(
      `/api/models?vehicle_type=${encodeURIComponent(
        vehicleType
      )}&category=${encodeURIComponent(category || "")}`
    );
    appendDistinctOptions(el.model, allModelData.models || [], true);
    restoreOrAutoSelect(el.model, prevModel);
    if (!el.model.value) restoreOrAutoSelect(el.model);
    return;
  }

  const data = await fetchJSON(
    `/api/models?make=${encodeURIComponent(make)}&vehicle_type=${encodeURIComponent(
      vehicleType
    )}&category=${encodeURIComponent(category || "")}`
  );
  const models = data.models || [];
  if (models.length > 0) {
    appendDistinctOptions(el.model, models, true);
    restoreOrAutoSelect(el.model, prevModel);
    if (!el.model.value) restoreOrAutoSelect(el.model);
    return;
  }

  // Fallback for sparse/unclean private-car model data:
  // 1) try all models under category, 2) if still empty, use make list as model choices.
  const allModelData = await fetchJSON(
    `/api/models?vehicle_type=${encodeURIComponent(
      vehicleType
    )}&category=${encodeURIComponent(category || "")}`
  );
  const allModels = allModelData.models || [];
  if (allModels.length > 0) {
    appendDistinctOptions(el.model, allModels, true);
    restoreOrAutoSelect(el.model, prevModel);
    if (!el.model.value) restoreOrAutoSelect(el.model);
    return;
  }
  if (isPrivateCar) {
    const makeData = await fetchJSON(
      `/api/makes?vehicle_type=${encodeURIComponent(vehicleType || "")}&category=${encodeURIComponent(category || "")}&fuel_type=${encodeURIComponent(fuelType)}`
    );
    appendDistinctOptions(el.model, makeData.makes || [], true);
    restoreOrAutoSelect(el.model, prevModel);
    if (!el.model.value) restoreOrAutoSelect(el.model);
    return;
  }
  appendDistinctOptions(el.model, [], true);
  restoreOrAutoSelect(el.model, prevModel);
  if (!el.model.value) restoreOrAutoSelect(el.model);
}

async function reloadFuelTypes() {
  const prevFuel = el.fuelType.value;
  const category = el.vehicleCategory.value;
  const isTwoWheeler = isCategory(category, CATEGORY_KEYS.twoWheeler);
  const needsFuel =
    isCategory(category, CATEGORY_KEYS.twoWheeler) ||
    isCategory(category, CATEGORY_KEYS.privateCar) ||
    isCategory(category, CATEGORY_KEYS.pcv);

  resetSelect(el.fuelType, "-- Choose Fuel Type --");
  if (!needsFuel) return;

  const data = await fetchJSON(
    `/api/fuel-types?vehicle_type=${encodeURIComponent(
      el.vehicleType.value || ""
    )}&category=${encodeURIComponent(category || "")}`
  );
  appendDistinctOptions(el.fuelType, data.fuels || [], false);
  Array.from(el.fuelType.options).forEach((opt) => {
    if (norm(opt.value) === "others") opt.remove();
  });
  if (isTwoWheeler) {
    Array.from(el.fuelType.options).forEach((opt) => {
      if (!opt.value) return;
      const fv = norm(opt.value);
      if (fv !== "petrol" && fv !== "ev") opt.remove();
    });
  }
  restoreOrAutoSelect(el.fuelType, prevFuel);
  if (!el.fuelType.value) restoreOrAutoSelect(el.fuelType);
}

async function reloadSlabs() {
  const prevCc = el.ccSlab.value;
  const prevWatt = el.wattSlab.value;
  const category = el.vehicleCategory.value;
  const fuel = el.fuelType.value || "";
  const isEV = norm(fuel) === "ev";
  const isAuto = isAutoVehicleType(el.vehicleType.value);
  const isTwoWheeler = isCategory(category, CATEGORY_KEYS.twoWheeler);
  const needsSlab =
    isCategory(category, CATEGORY_KEYS.twoWheeler) ||
    isCategory(category, CATEGORY_KEYS.privateCar) ||
    isCategory(category, CATEGORY_KEYS.pcv);

  setGroupVisible(group.cc, false);
  setGroupVisible(group.watt, false);
  resetSelect(el.ccSlab, "-- Choose CC Slab --");
  resetSelect(el.wattSlab, "-- Choose Watt Slab --");

  // Auto rule: no CC/Watt slab in UI; Fuel Type alone decides payout branch.
  if (isAuto) return;
  // Two Wheeler rule: for EV, no slab input should be shown.
  if (isTwoWheeler && isEV) return;
  if (!needsSlab || !fuel) return;

  const vehicleType = encodeURIComponent(el.vehicleType.value || "");
  const fuelType = encodeURIComponent(fuel);

  if (isEV) {
    setGroupVisible(group.watt, true);
    const data = await fetchJSON(
      `/api/watt-slabs?vehicle_type=${vehicleType}&fuel_type=${fuelType}&category=${encodeURIComponent(category || "")}`
    );
    appendDistinctOptions(el.wattSlab, data.watt_slabs || [], false);
    restoreOrAutoSelect(el.wattSlab, prevWatt);
    if (!el.wattSlab.value) restoreOrAutoSelect(el.wattSlab);
    return;
  }

  setGroupVisible(group.cc, true);
  const data = await fetchJSON(
    `/api/cc-slabs?vehicle_type=${vehicleType}&fuel_type=${fuelType}&category=${encodeURIComponent(category || "")}`
  );
  appendDistinctOptions(el.ccSlab, sortCcSlabsAscending(data.cc_slabs || []), false);
  restoreOrAutoSelect(el.ccSlab, prevCc);
  if (!el.ccSlab.value) restoreOrAutoSelect(el.ccSlab);
}

async function reloadSeating() {
  // Seating is not an explicit UI input now. For PCV, seating will be shown in output condition text.
  setGroupVisible(group.seating, false);
  resetSelect(el.seatingCapacity, "-- Choose Seating Capacity --");
}

async function reloadTrailer() {
  const prevTrailer = el.trailer.value;
  const category = el.vehicleCategory.value;
  const isMisc = isCategory(category, CATEGORY_KEYS.misc);
  const isTractor = norm(el.vehicleType.value) === "tractor";
  const show = isMisc && isTractor;
  setGroupVisible(group.trailer, show);
  resetSelect(el.trailer, "-- Choose Trailer --");
  if (!show) return;

  const data = await fetchJSON(
    `/api/trailers?vehicle_type=${encodeURIComponent(el.vehicleType.value || "")}`
  );
  appendDistinctOptions(el.trailer, data.trailers || [], false);
  restoreOrAutoSelect(el.trailer, prevTrailer);
  if (!el.trailer.value) restoreOrAutoSelect(el.trailer);
}

function applyCategoryVisibility() {
  applyVehicleDetailFieldOrder();
  const category = el.vehicleCategory.value;
  const isTW = isCategory(category, CATEGORY_KEYS.twoWheeler);
  const isPC = isCategory(category, CATEGORY_KEYS.privateCar);
  const isPCV = isCategory(category, CATEGORY_KEYS.pcv);
  const isMisc = isCategory(category, CATEGORY_KEYS.misc);
  const isGCV = isCategory(category, CATEGORY_KEYS.gcv);
  const isFuelPicked = !!(el.fuelType.value || "").trim();
  const isAuto = isAutoVehicleType(el.vehicleType.value);
  const isPcvBusType = isPCV && isPcvBusNoMakeModel(el.vehicleType.value);
  const isPcvTaxiType = isPCV && isPcvTaxi(el.vehicleType.value);
  const hidePcvFuel = isPcvBusType;
  const isGcvFlatbedType = isGCV && isGcvFlatbed(el.vehicleType.value);
  const isGcvThreeWheelerType = isGCV && isGcvThreeWheeler(el.vehicleType.value);
  const hideGcvMakeModel = isGcvFlatbedType || isGcvThreeWheelerType;
  const hideMakeModel = hideGcvMakeModel || isPcvBusType;
  const hideOnlyMake = isPcvTaxiType;
  const showMake = ((isTW ? isFuelPicked : true) && (isTW || isPC || isPCV || isGCV)) && !hideMakeModel && !hideOnlyMake;
  const showModel = (isPC || isPCV || isGCV) && !isAuto && !hideMakeModel;
  const showGvw = isGCV && isGcvFourWheeler(el.vehicleType.value);

  setGroupVisible(group.vehicleType, isTW || isPCV || isMisc || isGCV);
  setGroupVisible(group.make, showMake);
  setGroupVisible(group.model, showModel);
  setGroupVisible(group.fuel, (isTW || isPC || isPCV) && !hidePcvFuel);
  setGroupVisible(group.cc, false);
  setGroupVisible(group.watt, false);
  setGroupVisible(group.seating, false);
  setGroupVisible(group.trailer, isMisc && norm(el.vehicleType.value) === "tractor");
  setGroupVisible(group.gvw, showGvw);
  if (isMisc) {
    const hasType = !!el.vehicleType.value;
    setGroupVisible(group.business, hasType);
    setGroupVisible(group.policy, hasType);
  } else {
    setGroupVisible(group.business, !!category);
    setGroupVisible(group.policy, !!category);
  }

  if (isPC) {
    // Private Car has no separate vehicle type selection in UI flow.
    el.vehicleType.value = "";
  }

  if (isAuto) {
    el.model.value = "";
    el.seatingCapacity.value = "";
    el.ccSlab.value = "";
    el.wattSlab.value = "";
    setGroupVisible(group.cc, false);
    setGroupVisible(group.watt, false);
  }
  if (hidePcvFuel) {
    el.fuelType.value = "";
    el.ccSlab.value = "";
    el.wattSlab.value = "";
    setGroupVisible(group.cc, false);
    setGroupVisible(group.watt, false);
  }
  if (!showMake && !isPcvTaxiType) {
    el.make.value = "";
  }
  if (!showModel) {
    el.model.value = "";
  }
  if (!showGvw) {
    el.gvwSlab.value = "";
  }
  if (!showMake || !showModel) {
    if (!showMake) el.make.value = "";
    if (!showModel) el.model.value = "";
  }
  if (isGCV || isMisc) {
    setGroupVisible(group.cc, false);
    setGroupVisible(group.watt, false);
    el.ccSlab.value = "";
    el.wattSlab.value = "";
  }
}

async function onCategoryChange() {
  applyCategoryVisibility();
  resetSelect(el.vehicleType, "-- Choose Vehicle Type --");
  resetSelect(el.make, "-- Choose Make --");
  resetSelect(el.model, "-- Choose Model --");
  resetSelect(el.fuelType, "-- Choose Fuel Type --");
  resetSelect(el.ccSlab, "-- Choose CC Slab --");
  resetSelect(el.wattSlab, "-- Choose Watt Slab --");
  resetSelect(el.seatingCapacity, "-- Choose Seating Capacity --");
  resetSelect(el.trailer, "-- Choose Trailer --");
  if (el.gvwSlab) el.gvwSlab.value = "";

  if (!el.vehicleCategory.value) return;

  const isPC = isCategory(el.vehicleCategory.value, CATEGORY_KEYS.privateCar);

  if (!isPC) {
    const typeData = await fetchJSON(
      `/api/vehicle-types?category=${encodeURIComponent(el.vehicleCategory.value)}`
    );
    appendDistinctOptions(el.vehicleType, typeData.types || [], false);
    if (restoreOrAutoSelect(el.vehicleType)) {
      await onVehicleTypeChange();
    }
  } else {
    // still load make/fuel/policy options for private car even without sub-category
    await reloadFuelTypes();
    await reloadMakesAndModels();
    await reloadBusinessAndPolicy();
    await reloadSlabs();
  }
}

async function onVehicleTypeChange() {
  applyCategoryVisibility();
  await reloadFuelTypes();
  await reloadMakesAndModels();
  await reloadBusinessAndPolicy();
  await reloadSlabs();
  await reloadSeating();
  await reloadTrailer();
}

async function onFuelTypeChange() {
  const category = el.vehicleCategory.value;
  const isTW = isCategory(category, CATEGORY_KEYS.twoWheeler);
  const prevMake = el.make.value;
  const prevModel = el.model.value;

  applyCategoryVisibility();
  const tasks = [reloadBusinessAndPolicy(), reloadSlabs(), reloadSeating()];
  if (isTW) {
    tasks.push(reloadMakesAndModels());
  }
  await Promise.all(tasks);

  // For non-two-wheeler flows (PCV/Private Car/GCV/Misc), keep user make/model selection
  // when fuel changes, as long as those options still exist in the dropdown.
  if (!isTW) {
    const makeExists = Array.from(el.make.options).some((o) => o.value === prevMake);
    const modelExists = Array.from(el.model.options).some((o) => o.value === prevModel);
    if (makeExists) el.make.value = prevMake;
    if (modelExists) el.model.value = prevModel;
  }
}

function validateForm() {
  const missing = [];

  const require = (condition, label) => {
    if (!condition) missing.push(label);
  };

  require(!!el.state.value, "State");
  if (group.rto.style.display !== "none") {
    require(!!el.rtoNumber.value, "RTO Code");
  }
  require(!!el.vehicleCategory.value, "Vehicle Category");
  require(!!el.vehicleAge.value, "Vehicle Age");
  require(!!el.businessType.value, "Business Type");
  require(!!el.policyType.value, "Policy Type");

  const category = el.vehicleCategory.value;
  const isTW = isCategory(category, CATEGORY_KEYS.twoWheeler);
  const isPC = isCategory(category, CATEGORY_KEYS.privateCar);
  const isPCV = isCategory(category, CATEGORY_KEYS.pcv);
  const isMisc = isCategory(category, CATEGORY_KEYS.misc);
  const isGCV = isCategory(category, CATEGORY_KEYS.gcv);
  const isAuto = isAutoVehicleType(el.vehicleType.value);
  const isPcvBusType = isPCV && isPcvBusNoMakeModel(el.vehicleType.value);
  const isPcvTaxiType = isPCV && isPcvTaxi(el.vehicleType.value);
  const hidePcvFuel = isPcvBusType;
  const isGcvFlatbedType = isGCV && isGcvFlatbed(el.vehicleType.value);
  const isGcvThreeWheelerType = isGCV && isGcvThreeWheeler(el.vehicleType.value);
  const hideGcvMakeModel = isGcvFlatbedType || isGcvThreeWheelerType;
  const hideMakeModel = hideGcvMakeModel || isPcvBusType;
  const hideOnlyMake = isPcvTaxiType;

  if (isTW || isPCV || isMisc || isGCV) require(!!el.vehicleType.value, "Vehicle Type");
  if ((isTW || isPC || isPCV || isGCV) && !hideMakeModel && !hideOnlyMake) require(!!el.make.value, "Make");
  if ((isPC || isPCV) && !isAuto && group.model.style.display !== "none") {
    require(!!el.model.value, "Model");
  }
  if (isGCV && group.model.style.display !== "none") require(!!el.model.value, "Model");
  if ((isTW || isPC || isPCV) && !hidePcvFuel) require(!!el.fuelType.value, "Fuel Type");
  if (group.gvw.style.display !== "none") require(!!el.gvwSlab.value, "GVW Slab (Ton)");
  if (group.cc.style.display !== "none") require(!!el.ccSlab.value, "CC Slab");
  if (group.watt.style.display !== "none") require(!!el.wattSlab.value, "Watt Slab");
  if (group.seating.style.display !== "none") require(!!el.seatingCapacity.value, "Seating Capacity");
  if (isMisc && group.trailer.style.display !== "none") require(!!el.trailer.value, "Trailer");

  const age = (el.vehicleAge.value || "").toString().trim();
  const policy = el.policyType.value || "";
  const business = el.businessType.value || "";
  const ageIsNew = isVehicleAgeNew(age);
  if (ageIsNew && norm(business) !== "new") {
    missing.push("Business Type must be New when Vehicle Age is New Vehicle");
  }
  if (norm(business) === "new" && !ageIsNew) {
    missing.push("Vehicle Age must be New Vehicle when Business Type is New");
  }
  if (isBundlePolicyType(policy)) {
    if (!ageIsNew) missing.push("Vehicle Age must be New Vehicle for Bundle policy");
    if (norm(business) !== "new") missing.push("Business Type must be New for Bundle policy");
  }

  return missing;
}

async function onSubmit(event) {
  event.preventDefault();
  const missing = validateForm();
  if (missing.length) {
    alert(`Please fill required fields:\n\n- ${missing.join("\n- ")}`);
    return;
  }

  submitButton.disabled = true;
  submitButton.textContent = "Calculating...";
  try {
    const formData = new FormData(form);
    const response = await fetch("/check-payout", { method: "POST", body: formData });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    const payouts = data.top_5_payouts || data.top_3_payouts || [];
    if (data.status === "success" && payouts.length > 0) {
      showRows(payouts);
      results.section.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    showNoData(data.message || "No payout data found.");
  } catch (error) {
    showNoData(`Error: ${error.message}`);
  } finally {
    submitButton.disabled = false;
    submitButton.textContent = "Calculate Payout";
  }
}

async function init() {
  setGroupVisible(group.rto, false);
  setGroupVisible(group.vehicleType, false);
  setGroupVisible(group.make, false);
  setGroupVisible(group.model, false);
  setGroupVisible(group.fuel, false);
  setGroupVisible(group.cc, false);
  setGroupVisible(group.watt, false);
  setGroupVisible(group.seating, false);
  setGroupVisible(group.trailer, false);
  setGroupVisible(group.gvw, false);
  setGroupVisible(group.business, false);
  setGroupVisible(group.policy, false);

  await Promise.all([populateStates(), populateCategories(), populateVehicleAges()]);

  el.state.addEventListener("change", async () => {
    await onStateChange();
  });

  el.vehicleCategory.addEventListener("change", async () => {
    await onCategoryChange();
  });

  el.vehicleType.addEventListener("change", async () => {
    await onVehicleTypeChange();
  });

  el.make.addEventListener("change", async () => {
    await reloadModelsByMake();
  });

  el.fuelType.addEventListener("change", async () => {
    await onFuelTypeChange();
  });

  el.vehicleAge.addEventListener("change", () => {
    applyBusinessTypeRules("age");
  });

  [el.vehicleAge, el.businessType].forEach((sel) => {
    sel.addEventListener("keydown", (e) => {
      if (sel.dataset.locked === "true") e.preventDefault();
    });
  });

  el.policyType.addEventListener("change", () => {
    applyBusinessTypeRules("policy");
  });
  el.businessType.addEventListener("change", () => {
    applyBusinessTypeRules("business");
  });

  form.addEventListener("submit", onSubmit);
}

document.addEventListener("DOMContentLoaded", () => {
  init().catch((error) => {
    showNoData(`Initialization error: ${error.message}`);
  });
});
