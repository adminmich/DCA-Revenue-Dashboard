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
  try {
    // Fetch WHOP data in parallel
    const [payments, memberships, plans, products] = await Promise.all([
      whopFetchAll('/payments', { status: 'paid' }),
      whopFetchAll('/memberships', { status: 'active' }),
      whopFetchAll('/plans'),
      whopFetchAll('/products'),
    ]);

    // Process payments by month
    const now = new Date();
    const paymentsByMonth = {};
    let totalRevenue = 0;
    let thisMonthRevenue = 0;
    let last30Revenue = 0;
    const thirtyDaysAgo = new Date(now - 30 * 24 * 60 * 60 * 1000);

    payments.forEach(p => {
      const date = new Date(p.paid_at || p.created_at);
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

    // NMI transactions (last 30 days)
    let nmiData = { transactions: [], total: 0, count: 0 };
    try {
      const nmiParams = new URLSearchParams({
        username: 'api_key',
        password: NMI_KEY,
        start_date: formatNMIDate(thirtyDaysAgo),
        end_date: formatNMIDate(now),
      });
      const nmiResp = await fetch(`${NMI_BASE}?${nmiParams}`);
      const nmiXml = await nmiResp.text();
      nmiData = parseNMIXml(nmiXml);
    } catch (e) {
      console.error('NMI fetch error:', e.message);
    }

    // Calculate MRR from recurring payments — current month
    const currentMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const recurringThisMonth = payments.filter(p => {
      const date = new Date(p.paid_at || p.created_at);
      const mk = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      return mk === currentMonth && p.billing_reason === 'subscription_cycle';
    });
    const mrr = recurringThisMonth.reduce((s, p) => s + (p.usd_total || p.total || 0), 0);

    // Calculate MRR by year — use last month of each year with data
    const mrrByYear = {};
    const mrrByMonth = {};
    payments.forEach(p => {
      if (p.billing_reason !== 'subscription_cycle') return;
      const date = new Date(p.paid_at || p.created_at);
      const year = date.getFullYear();
      const monthKey = `${year}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      const amount = p.usd_total || p.total || 0;
      if (!mrrByMonth[monthKey]) mrrByMonth[monthKey] = 0;
      mrrByMonth[monthKey] += amount;
    });
    // For each year, get the average monthly recurring and the last month's MRR
    const years = [...new Set(Object.keys(mrrByMonth).map(m => parseInt(m.split('-')[0])))].sort();
    years.forEach(year => {
      const yearMonths = Object.keys(mrrByMonth).filter(m => m.startsWith(year + '-')).sort();
      const lastMonth = yearMonths[yearMonths.length - 1];
      const avgMrr = yearMonths.reduce((s, m) => s + mrrByMonth[m], 0) / yearMonths.length;
      mrrByYear[year] = {
        lastMonthMrr: mrrByMonth[lastMonth] || 0,
        avgMrr: Math.round(avgMrr * 100) / 100,
        monthCount: yearMonths.length,
        monthly: yearMonths.map(m => ({ month: m, mrr: mrrByMonth[m] })),
      };
    });

    res.json({
      summary: {
        totalRevenue,
        thisMonthRevenue,
        last30Revenue,
        mrr,
        activeMembers: activeMemberships.length,
        cancelingMembers: cancelingMemberships.length,
        totalPayments: payments.length,
        nmiReserve: 13109.28,
      },
      mrrByYear,
      paymentsByMonth,
      recentPayments: payments.slice(0, 50).map(p => ({
        id: p.id,
        amount: p.usd_total || p.total || 0,
        date: p.paid_at || p.created_at,
        user: p.user?.name || p.user?.username || 'Unknown',
        email: p.user?.email || '',
        product: p.product?.title || '',
        method: p.payment_method_type || '',
        billing_reason: p.billing_reason || '',
      })),
      memberships: {
        active: activeMemberships.length,
        canceling: cancelingMemberships.length,
        byProduct: groupBy(activeMemberships, m => m.product?.title || 'Unknown'),
      },
      plans: Object.values(planSummary),
      products: products.map(p => ({ id: p.id, title: p.title, members: p.member_count })),
      nmi: nmiData,
      fetchedAt: new Date().toISOString(),
    });
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
});
