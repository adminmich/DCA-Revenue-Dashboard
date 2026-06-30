// Build subscription tenure report from an NMI .xls transactions export.
// Reads transactions, groups by customer, computes how long each member stays
// on the monthly subscription, and writes public/subscription-tenure-data.json
// for the static report page.
//
// Usage:  node build-tenure-report.js [path/to/transactions.xls]
// Default source: C:/Users/User/Downloads/transactions (3).xls

const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');

const SRC = process.argv[2] || 'C:/Users/User/Downloads/transactions (3).xls';
const OUT = path.join(__dirname, 'public', 'subscription-tenure-data.json');
const DAY_MS = 24 * 60 * 60 * 1000;

// A customer is "active" if their most recent successful charge is within ACTIVE_WINDOW_DAYS.
// 35 days covers a monthly cycle plus a few days of retry/decline slack.
const ACTIVE_WINDOW_DAYS = 35;

// A monthly subscription payment covers ~30 days forward, so when computing
// tenure for churned members we add 30 days to (lastPaid - firstPaid).
const CYCLE_DAYS = 30;

console.log('Reading', SRC);
const wb = XLSX.readFile(SRC);
const sheet = wb.Sheets[wb.SheetNames[0]];
const rows = XLSX.utils.sheet_to_json(sheet, { raw: false, defval: '' });
console.log('Loaded', rows.length, 'rows from sheet', wb.SheetNames[0]);

// Keep only completed sales (each sale has a matching "settle" row — we dedupe by counting sales only).
const sales = rows.filter(r => r.type === 'sale' && r.status === 'complete');
console.log('Completed sales:', sales.length);

// Group by customer (email primary, name+card-suffix fallback)
const byCust = {};
sales.forEach(r => {
  const email = (r.email || '').toLowerCase().trim();
  const key = email || `${r.first_name}|${r.last_name}|${r.account || ''}`.toLowerCase();
  if (!key) return;
  if (!byCust[key]) {
    byCust[key] = {
      key,
      email,
      firstName: r.first_name || '',
      lastName: r.last_name || '',
      product: r.orderdescription || '',
      amount: parseFloat(r.amount || 0),
      txs: [],
    };
  }
  const t = new Date(r.time);
  if (!isNaN(t)) byCust[key].txs.push(t.getTime());
});
const customers = Object.values(byCust);
customers.forEach(c => c.txs.sort((a, b) => a - b));
console.log('Unique customers:', customers.length);

// Use latest date across the dataset as "now" so the report is stable
// regardless of when we re-run it. Add a small buffer (3 days) so charges
// landed right at the edge still count as active.
const dataMaxTs = Math.max(...customers.map(c => c.txs[c.txs.length - 1]));
const asOf = new Date(dataMaxTs);
console.log('As-of date (latest tx in data):', asOf.toISOString().slice(0, 10));

const ACTIVE_CUTOFF = dataMaxTs - ACTIVE_WINDOW_DAYS * DAY_MS;

const memberRows = customers.map(c => {
  const first = c.txs[0];
  const last = c.txs[c.txs.length - 1];
  const isActive = last >= ACTIVE_CUTOFF;
  // Tenure: from first paid to last paid + one cycle (the last payment covers the next month).
  // For currently-active members, "tenure so far" is from first paid to as-of date.
  const tenureDays = isActive
    ? Math.round((dataMaxTs - first) / DAY_MS)
    : Math.round((last - first) / DAY_MS) + CYCLE_DAYS;
  return {
    email: c.email,
    name: `${c.firstName} ${c.lastName}`.trim() || c.email || 'Unknown',
    product: c.product,
    monthlyAmount: c.amount,
    payments: c.txs.length,
    firstPaidAt: new Date(first).toISOString(),
    lastPaidAt: new Date(last).toISOString(),
    status: isActive ? 'active' : 'churned',
    tenureDays,
    tenureMonths: Math.round(tenureDays / 30.44 * 10) / 10,
    totalPaid: Math.round(c.txs.length * c.amount * 100) / 100,
  };
});

const churned = memberRows.filter(m => m.status === 'churned');
const active = memberRows.filter(m => m.status === 'active');

const sum = arr => arr.reduce((s, m) => s + m.tenureDays, 0);
const avg = arr => arr.length ? sum(arr) / arr.length : 0;
const median = arr => {
  if (!arr.length) return 0;
  const s = arr.map(m => m.tenureDays).sort((a, b) => a - b);
  const mid = Math.floor((s.length - 1) / 2);
  return s.length % 2 ? s[mid] : (s[mid] + s[mid + 1]) / 2;
};
const pct = (arr, p) => {
  if (!arr.length) return 0;
  const s = arr.map(m => m.tenureDays).sort((a, b) => a - b);
  const i = Math.min(s.length - 1, Math.max(0, Math.round((p / 100) * (s.length - 1))));
  return s[i];
};

const BUCKETS = [
  { label: '< 1 month',     min: 0,    max: 30.44 },
  { label: '1–2 months',    min: 30.44, max: 60.88 },
  { label: '2–3 months',    min: 60.88, max: 91.32 },
  { label: '3–6 months',    min: 91.32, max: 182.64 },
  { label: '6–12 months',   min: 182.64, max: 365.25 },
  { label: '12+ months',    min: 365.25, max: Infinity },
];
const bucketize = arr => {
  const counts = BUCKETS.map(b => ({ ...b, count: 0 }));
  arr.forEach(m => {
    for (const b of counts) {
      if (m.tenureDays >= b.min && m.tenureDays < b.max) { b.count++; break; }
    }
  });
  return counts.map(b => ({ bucket: b.label, count: b.count }));
};

// Cohort: signup month → tenure stats
const byCohort = {};
memberRows.forEach(m => {
  const d = new Date(m.firstPaidAt);
  const k = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  if (!byCohort[k]) byCohort[k] = { cohort: k, members: [] };
  byCohort[k].members.push(m);
});
const cohortTable = Object.values(byCohort).map(g => {
  const total = g.members.length;
  const ch = g.members.filter(m => m.status === 'churned');
  const ac = g.members.filter(m => m.status === 'active');
  return {
    cohort: g.cohort,
    total,
    churned: ch.length,
    active: ac.length,
    churnRate: total ? Math.round(ch.length / total * 1000) / 10 : 0,
    avgChurnedDays: Math.round(avg(ch)),
    avgChurnedMonths: Math.round(avg(ch) / 30.44 * 10) / 10,
    medianChurnedDays: Math.round(median(ch)),
    avgAllDays: Math.round(avg(g.members)),
  };
}).sort((a, b) => a.cohort.localeCompare(b.cohort));

// Distribution of payment counts (how many monthly charges before they churned)
const paymentBuckets = [
  { label: '1 payment',      min: 1, max: 2 },
  { label: '2 payments',     min: 2, max: 3 },
  { label: '3 payments',     min: 3, max: 4 },
  { label: '4–6 payments',   min: 4, max: 7 },
  { label: '7–12 payments',  min: 7, max: 13 },
  { label: '13+ payments',   min: 13, max: Infinity },
];
const paymentDist = paymentBuckets.map(b => {
  const list = churned.filter(m => m.payments >= b.min && m.payments < b.max);
  return { bucket: b.label, count: list.length };
});

// Year-over-year churn comparison (cohorts grouped by signup year)
const byCohortYear = {};
memberRows.forEach(m => {
  const y = new Date(m.firstPaidAt).getFullYear();
  if (!byCohortYear[y]) byCohortYear[y] = [];
  byCohortYear[y].push(m);
});
const yearTable = Object.entries(byCohortYear).map(([y, arr]) => {
  const ch = arr.filter(m => m.status === 'churned');
  return {
    year: Number(y),
    total: arr.length,
    churned: ch.length,
    active: arr.length - ch.length,
    avgChurnedDays: Math.round(avg(ch)),
    avgChurnedMonths: Math.round(avg(ch) / 30.44 * 10) / 10,
    medianChurnedDays: Math.round(median(ch)),
    avgPayments: ch.length ? Math.round(ch.reduce((s, m) => s + m.payments, 0) / ch.length * 10) / 10 : 0,
  };
}).sort((a, b) => a.year - b.year);

// Summary
const churnedDays = churned.map(m => m.tenureDays);
const summary = {
  asOf: asOf.toISOString().slice(0, 10),
  dataRangeStart: new Date(Math.min(...customers.map(c => c.txs[0]))).toISOString().slice(0, 10),
  dataRangeEnd: asOf.toISOString().slice(0, 10),
  monthlyPrice: customers[0]?.amount || 0,
  product: customers[0]?.product || '',
  totalCustomers: memberRows.length,
  activeCustomers: active.length,
  churnedCustomers: churned.length,
  overallChurnRate: memberRows.length ? Math.round(churned.length / memberRows.length * 1000) / 10 : 0,
  avgTenureChurnedDays: Math.round(avg(churned)),
  avgTenureChurnedMonths: Math.round(avg(churned) / 30.44 * 100) / 100,
  medianTenureChurnedDays: Math.round(median(churned)),
  medianTenureChurnedMonths: Math.round(median(churned) / 30.44 * 100) / 100,
  p25TenureDays: pct(churned, 25),
  p75TenureDays: pct(churned, 75),
  p90TenureDays: pct(churned, 90),
  avgTenureActiveDays: Math.round(avg(active)),
  avgTenureActiveMonths: Math.round(avg(active) / 30.44 * 100) / 100,
  avgTenureAllDays: Math.round(avg(memberRows)),
  avgTenureAllMonths: Math.round(avg(memberRows) / 30.44 * 100) / 100,
  avgPaymentsChurned: churned.length ? Math.round(churned.reduce((s, m) => s + m.payments, 0) / churned.length * 100) / 100 : 0,
  avgPaymentsActive: active.length ? Math.round(active.reduce((s, m) => s + m.payments, 0) / active.length * 100) / 100 : 0,
  avgLifetimeValueChurned: churned.length ? Math.round(churned.reduce((s, m) => s + m.totalPaid, 0) / churned.length * 100) / 100 : 0,
  avgLifetimeValueAll: memberRows.length ? Math.round(memberRows.reduce((s, m) => s + m.totalPaid, 0) / memberRows.length * 100) / 100 : 0,
  totalRevenueProcessed: Math.round(memberRows.reduce((s, m) => s + m.totalPaid, 0) * 100) / 100,
};

const data = {
  summary,
  distribution: {
    tenureChurned: bucketize(churned),
    tenureActive: bucketize(active),
    tenureAll: bucketize(memberRows),
    paymentsChurned: paymentDist,
  },
  cohorts: cohortTable,
  byYear: yearTable,
  // Trim per-member rows for the response: top by tenure plus most-recent churners
  members: {
    longestTenure: [...memberRows].sort((a, b) => b.tenureDays - a.tenureDays).slice(0, 100),
    shortestChurned: [...churned].sort((a, b) => a.tenureDays - b.tenureDays).slice(0, 50),
    recentChurners: [...churned].sort((a, b) => new Date(b.lastPaidAt) - new Date(a.lastPaidAt)).slice(0, 50),
    activeMembers: [...active].sort((a, b) => b.tenureDays - a.tenureDays),
    allCount: memberRows.length,
  },
  generatedAt: new Date().toISOString(),
};

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(data, null, 2));
console.log('Wrote', OUT, `(${(fs.statSync(OUT).size / 1024).toFixed(1)} KB)`);

console.log('\nSummary:');
console.log('  Total customers:', summary.totalCustomers);
console.log('  Active:', summary.activeCustomers, '/ Churned:', summary.churnedCustomers, `(churn ${summary.overallChurnRate}%)`);
console.log('  Avg churned tenure:', summary.avgTenureChurnedDays, 'days =', summary.avgTenureChurnedMonths, 'months');
console.log('  Median churned tenure:', summary.medianTenureChurnedDays, 'days =', summary.medianTenureChurnedMonths, 'months');
console.log('  Avg payments per churned:', summary.avgPaymentsChurned);
console.log('  Avg LTV (churned):', '$' + summary.avgLifetimeValueChurned);
