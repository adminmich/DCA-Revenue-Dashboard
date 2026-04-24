require('dotenv').config();
const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

const WHOP_KEY = process.env.WHOP_API_KEY;
const WHOP_CID = process.env.WHOP_COMPANY_ID;
const NMI_KEY = process.env.NMI_API_KEY;
const WHOP_BASE = 'https://api.whop.com/api/v1';
const NMI_BASE = 'https://secure.networkmerchants.com/api/query.php';

// ── CACHE ──
let dashboardCache = null;
let cacheTime = 0;
const CACHE_TTL = 30 * 60 * 1000; // 30 minutes

const whopHeaders = { 'Authorization': `Bearer ${WHOP_KEY}` };

// Helper: paginate through all WHOP results
async function whopFetchAll(endpoint, params = {}) {
  const all = [];
  let cursor = null;
  let pages = 0;
  do {
    const p = new URLSearchParams({ per: '100', company_id: WHOP_CID, ...params });
    if (cursor) p.set('after', cursor);
    const resp = await fetch(`${WHOP_BASE}${endpoint}?${p}`, { headers: whopHeaders });
    const data = await resp.json();
    if (data.data) all.push(...data.data);
    cursor = data.page_info?.has_next_page ? data.page_info.end_cursor : null;
    pages++;
    if (pages > 20) break; // safety limit
  } while (cursor);
  return all;
}

// ── MAIN DASHBOARD ENDPOINT ──
app.get('/api/dashboard', async (req, res) => {
  // Return cache if fresh
  if (dashboardCache && (Date.now() - cacheTime) < CACHE_TTL) {
    return res.json(dashboardCache);
  }

  try {
    console.log('Fetching fresh data from WHOP + NMI...');
    // Fetch WHOP data in parallel
    const [payments, memberships, plans, products] = await Promise.all([
      whopFetchAll('/payments', { status: 'paid' }),
      whopFetchAll('/memberships', { status: 'active' }),
      whopFetchAll('/plans'),
      whopFetchAll('/products'),
    ]);

    // Process payments by month — only 2025 and 2026
    const now = new Date();
    const paymentsByMonth = {};
    let totalRevenue = 0;
    let thisMonthRevenue = 0;
    let last30Revenue = 0;
    const thirtyDaysAgo = new Date(now - 30 * 24 * 60 * 60 * 1000);

    payments.forEach(p => {
      const date = new Date(p.paid_at || p.created_at);
      if (date.getFullYear() < 2025) return;
      const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (!paymentsByMonth[monthKey]) paymentsByMonth[monthKey] = { revenue: 0, count: 0, payments: [] };
      const amount = (p.usd_total || p.total || 0);
      paymentsByMonth[monthKey].revenue += amount;
      paymentsByMonth[monthKey].count++;
      paymentsByMonth[monthKey].payments.push({
        id: p.id,
        amount,
        date: p.paid_at || p.created_at,
        user: p.user?.name || p.user?.username || 'Unknown',
        email: p.user?.email || '',
        product: p.product?.title || '',
        plan: p.plan?.internal_notes || '',
        method: p.payment_method_type || '',
        billing_reason: p.billing_reason || '',
      });
      totalRevenue += amount;
      if (date >= thirtyDaysAgo) last30Revenue += amount;
      if (date.getMonth() === now.getMonth() && date.getFullYear() === now.getFullYear()) {
        thisMonthRevenue += amount;
      }
    });

    // Process memberships
    const activeMemberships = memberships.filter(m => m.status === 'active');
    const cancelingMemberships = memberships.filter(m => m.cancel_at_period_end === true);

    // Process plans - group by product and billing
    const activePlans = plans.filter(p => p.visibility !== 'archived');
    const planSummary = {};
    activePlans.forEach(p => {
      const product = p.product?.title || 'Other';
      const price = (p.initial_price || 0);
      const period = p.billing_period || 'one_time';
      const key = `${product}_${price}_${period}`;
      if (!planSummary[key]) planSummary[key] = { product, price, period, type: p.plan_type, count: 0 };
      planSummary[key].count++;
    });

    // NMI transactions — fetch from Jan 1 of current year to now
    let nmiData = { transactions: [], total: 0, count: 0 };
    try {
      const janFirst = `${now.getFullYear()}0101`;
      const nmiParams = new URLSearchParams({
        username: 'api_key',
        password: NMI_KEY,
        start_date: janFirst,
        end_date: formatNMIDate(now),
      });
      const nmiResp = await fetch(`${NMI_BASE}?${nmiParams}`);
      const nmiXml = await nmiResp.text();
      nmiData = parseNMIXml(nmiXml);
    } catch (e) {
      console.error('NMI fetch error:', e.message);
    }

    // ── WHOP MRR (exclude Stripe-uploaded payments) ──
    const isNativeWhop = p => {
      const method = (p.payment_method_type || '').toLowerCase();
      // Exclude Stripe-uploaded/migrated payments — only count native WHOP billing
      return !method.includes('stripe');
    };
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;

    // WHOP MRR — current month
    const whopRecurringThisMonth = payments.filter(p => {
      const date = new Date(p.paid_at || p.created_at);
      const mk = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      return mk === currentMonth && p.billing_reason === 'subscription_cycle' && isNativeWhop(p);
    });
    const whopMrr = whopRecurringThisMonth.reduce((s, p) => s + (p.usd_total || p.total || 0), 0);

    // WHOP MRR by month (for trend) — 2025+ only
    const whopMrrByMonth = {};
    payments.forEach(p => {
      if (p.billing_reason !== 'subscription_cycle') return;
      if (!isNativeWhop(p)) return;
      const date = new Date(p.paid_at || p.created_at);
      if (date.getFullYear() < 2025) return;
      const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      const amount = p.usd_total || p.total || 0;
      if (!whopMrrByMonth[monthKey]) whopMrrByMonth[monthKey] = 0;
      whopMrrByMonth[monthKey] += amount;
    });

    // ── NMI MRR (live from full year fetch) ──
    const nmiMrrByMonth = {};
    nmiData.transactions.forEach(t => {
      if (t.condition !== 'complete' && t.condition !== 'pendingsettlement') return;
      const ds = String(t.date || '');
      if (ds.length < 6) return;
      const monthKey = `${ds.substring(0, 4)}-${ds.substring(4, 6)}`;
      if (!nmiMrrByMonth[monthKey]) nmiMrrByMonth[monthKey] = 0;
      nmiMrrByMonth[monthKey] += t.amount;
    });
    const nmiMrr = nmiMrrByMonth[currentMonth] || 0;

    // ── Combined MRR ──
    const mrr = whopMrr + nmiMrr;

    // ── MRR by year with WHOP/NMI/Combined breakdown ──
    const allMrrMonths = new Set([...Object.keys(whopMrrByMonth), ...Object.keys(nmiMrrByMonth)]);
    const mrrByYear = {};
    const years = [...new Set([...allMrrMonths].map(m => parseInt(m.split('-')[0])))].sort();
    years.forEach(year => {
      const yearMonths = [...allMrrMonths].filter(m => m.startsWith(year + '-')).sort();
      const lastMonth = yearMonths[yearMonths.length - 1];
      const monthly = yearMonths.map(m => ({
        month: m,
        whopMrr: whopMrrByMonth[m] || 0,
        nmiMrr: nmiMrrByMonth[m] || 0,
        combined: (whopMrrByMonth[m] || 0) + (nmiMrrByMonth[m] || 0),
      }));
      const totalWhop = monthly.reduce((s, m) => s + m.whopMrr, 0);
      const totalNmi = monthly.reduce((s, m) => s + m.nmiMrr, 0);
      const totalCombined = monthly.reduce((s, m) => s + m.combined, 0);
      const lastData = monthly[monthly.length - 1] || { whopMrr: 0, nmiMrr: 0, combined: 0 };
      mrrByYear[year] = {
        lastMonthMrr: lastData.combined,
        lastMonthWhopMrr: lastData.whopMrr,
        lastMonthNmiMrr: lastData.nmiMrr,
        avgMrr: Math.round(totalCombined / yearMonths.length * 100) / 100,
        avgWhopMrr: Math.round(totalWhop / yearMonths.length * 100) / 100,
        avgNmiMrr: Math.round(totalNmi / yearMonths.length * 100) / 100,
        monthCount: yearMonths.length,
        monthly,
      };
    });

    // Trim paymentsByMonth — keep only last 50 payments per month
    const trimmedPaymentsByMonth = {};
    Object.entries(paymentsByMonth).forEach(([month, data]) => {
      trimmedPaymentsByMonth[month] = {
        revenue: data.revenue,
        count: data.count,
        payments: data.payments.slice(-50),
      };
    });

    // Trim NMI — keep only last 30 transactions for display
    const nmiTrimmed = {
      total: nmiData.total,
      count: nmiData.count,
      transactions: nmiData.transactions.slice(-30),
    };

    dashboardCache = {
      summary: {
        totalRevenue,
        thisMonthRevenue: thisMonthRevenue + nmiMrr,
        last30Revenue,
        mrr,
        whopMrr,
        nmiMrr,
        activeMembers: activeMemberships.length,
        cancelingMembers: cancelingMemberships.length,
        totalPayments: payments.length,
        nmiReserve: nmiData.total,
      },
      mrrByYear,
      paymentsByMonth: trimmedPaymentsByMonth,
      memberships: {
        active: activeMemberships.length,
        canceling: cancelingMemberships.length,
      },
      plans: Object.values(planSummary),
      products: products.map(p => ({ id: p.id, title: p.title, members: p.member_count })),
      nmi: nmiTrimmed,
      fetchedAt: new Date().toISOString(),
    };
    cacheTime = Date.now();
    console.log('Cache refreshed: ' + payments.length + ' payments loaded');
    res.json(dashboardCache);
  } catch (err) {
    console.error('Dashboard error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── RAW WHOP ENDPOINTS ──
app.get('/api/whop/payments', async (req, res) => {
  try {
    const data = await whopFetchAll('/payments', { status: req.query.status || 'paid' });
    res.json({ data, count: data.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/whop/memberships', async (req, res) => {
  try {
    const data = await whopFetchAll('/memberships', { status: req.query.status || 'active' });
    res.json({ data, count: data.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── NMI ENDPOINT ──
app.get('/api/nmi/transactions', async (req, res) => {
  try {
    const { start_date, end_date } = req.query;
    const params = new URLSearchParams({ username: 'api_key', password: NMI_KEY });
    if (start_date) params.set('start_date', start_date);
    if (end_date) params.set('end_date', end_date);
    const resp = await fetch(`${NMI_BASE}?${params}`);
    const text = await resp.text();
    res.set('Content-Type', 'text/xml');
    res.send(text);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── STEVEN ESSA LEADS (live from Google Sheet) ──
const ESSA_SHEET_ID = '11NDRuct-mPGI4KQiGpYF3zaQuDN-BbvpF20rlt-pF4Y';
const ESSA_GID = '1930928329';
let essaCache = null;
let essaCacheTime = 0;

async function fetchEssaData() {
  // Return cache if fresh (5 min)
  if (essaCache && (Date.now() - essaCacheTime) < CACHE_TTL) return essaCache;
  try {
    const url = `https://docs.google.com/spreadsheets/d/${ESSA_SHEET_ID}/gviz/tq?tqx=out:csv&gid=${ESSA_GID}`;
    const resp = await fetch(url);
    const csv = await resp.text();

    // Parse key values from CSV
    let totalPipeline = 93000, totalCollected = 0, workshopRevenue = 17892;
    const lines = csv.split('\n');

    for (const line of lines) {
      // Find the "Total" row with 3 dollar amounts: pipeline, collected, outstanding
      if (line.includes('Total')) {
        const dollars = [...line.matchAll(/\$([\d,]+(?:\.\d{2})?)/g)].map(m => parseFloat(m[1].replace(/,/g, '')));
        if (dollars.length >= 3 && dollars[0] >= 50000) {
          totalPipeline = dollars[0];
          totalCollected = dollars[1];
        }
      }
      // Workshop Ticket Revenue — grab the largest dollar amount on that line
      if (line.includes('Workshop Ticket Revenue')) {
        const wDollars = [...line.matchAll(/\$([\d,]+(?:\.\d{2})?)/g)].map(m => parseFloat(m[1].replace(/,/g, '')));
        if (wDollars.length > 0) workshopRevenue = Math.max(...wDollars);
      }
    }

    const outstanding = totalPipeline - totalCollected;
    essaCache = { totalPipeline, totalCollected, outstanding, workshopRevenue, fetchedAt: new Date().toISOString() };
    essaCacheTime = Date.now();
    console.log(`Steven Essa data: Pipeline $${totalPipeline} | Collected $${totalCollected} | Outstanding $${outstanding}`);
    return essaCache;
  } catch (e) {
    console.error('Essa fetch error:', e.message);
    return essaCache || { totalPipeline: 93000, totalCollected: 41480, outstanding: 51520, workshopRevenue: 17892, fetchedAt: null };
  }
}

app.get('/api/essa', async (req, res) => {
  try {
    const data = await fetchEssaData();
    res.json(data);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── HELPERS ──
function formatNMIDate(d) {
  return d.toISOString().split('T')[0].replace(/-/g, '');
}

function parseNMIXml(xml) {
  const transactions = [];
  let total = 0;
  const txnBlocks = xml.split('<transaction>').slice(1);
  txnBlocks.forEach(block => {
    const get = tag => { const m = block.match(new RegExp(`<${tag}>(.*?)</${tag}>`)); return m ? m[1] : ''; };
    const amount = parseFloat(get('amount')) || 0;
    transactions.push({
      id: get('transaction_id'),
      amount,
      date: get('date'),
      first_name: get('first_name'),
      last_name: get('last_name'),
      email: get('email'),
      condition: get('condition'),
    });
    if (get('condition') === 'complete' || get('condition') === 'pendingsettlement') {
      total += amount;
    }
  });
  return { transactions, total, count: transactions.length };
}

function groupBy(arr, fn) {
  const groups = {};
  arr.forEach(item => {
    const key = fn(item);
    if (!groups[key]) groups[key] = [];
    groups[key].push(item);
  });
  const result = {};
  Object.keys(groups).forEach(k => { result[k] = groups[k].length; });
  return result;
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`DCA Dashboard API running on http://localhost:${PORT}`);
  console.log(`Company: ${WHOP_CID}`);
  // Pre-warm cache on startup
  const http = require('http');
  setTimeout(() => {
    http.get(`http://localhost:${PORT}/api/dashboard`, () => {
      console.log('Cache pre-warmed');
    }).on('error', () => {});
  }, 1000);
});
