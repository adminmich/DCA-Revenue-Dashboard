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

// Helper: paginate through all WHOP results. opts.shouldStop(page) lets the caller bail early.
async function whopFetchAll(endpoint, params = {}, opts = {}) {
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
    if (opts.shouldStop && data.data && opts.shouldStop(data.data)) break;
  } while (cursor);
  return all;
}

// We only render 2025+ data — bail once a full page is pre-2025
const isPre2025 = p => {
  const d = new Date(p.paid_at || p.created_at);
  return d.getFullYear() < 2025;
};

// ── MAIN DASHBOARD ENDPOINT ──
app.get('/api/dashboard', async (req, res) => {
  const force = req.query.force === '1' || req.query.force === 'true';
  if (force) {
    res.set('Cache-Control', 'no-store');
  } else {
    // Edge CDN cache: 30 min fresh, 1 h stale-while-revalidate
    res.set('Cache-Control', 'public, s-maxage=1800, stale-while-revalidate=3600');
    // Return in-memory cache if fresh (warm container)
    if (dashboardCache && (Date.now() - cacheTime) < CACHE_TTL) {
      return res.json(dashboardCache);
    }
  }

  try {
    console.log('Fetching fresh data from WHOP + NMI...');
    // Fetch WHOP data in parallel. /payments returns paid/open/void records
    // regardless of the status filter — we classify per record below.
    const [payments, memberships, plans, products] = await Promise.all([
      whopFetchAll('/payments', { status: 'paid' }, {
        shouldStop: page => page.length > 0 && page.every(isPre2025),
      }),
      whopFetchAll('/memberships'), // all statuses, so we can compute active %
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
    const cancelingMemberships = activeMemberships.filter(m => m.cancel_at_period_end === true);

    // Build a clean list of canceling members (active subs flagged cancel_at_period_end).
    // These are live members who manually cancelled — still active until period end.
    const cancelingMembersList = cancelingMemberships
      .map(m => ({
        name: m.user?.name || m.user?.username || 'Unknown',
        email: m.user?.email || '',
        plan: m.plan?.title || m.product?.title || m.plan_name || '',
        product: m.product?.title || '',
        memberSince: m.created_at || null,
        renewsAt: m.renewal_period_end || m.expires_at || null,
        expiresAt: m.expires_at || m.renewal_period_end || null,
        amount: m.plan?.initial_price || m.plan?.renewal_price || 0,
      }))
      .sort((a, b) => {
        const da = new Date(a.expiresAt || 0).getTime();
        const db = new Date(b.expiresAt || 0).getTime();
        return da - db;
      });

    // ── Members per product, with active % ──
    const totalByProductId = {};
    const activeByProductId = {};
    memberships.forEach(m => {
      const pid = m.product?.id;
      if (!pid) return;
      totalByProductId[pid] = (totalByProductId[pid] || 0) + 1;
      if (m.status === 'active') activeByProductId[pid] = (activeByProductId[pid] || 0) + 1;
    });
    const productMembership = products.map(p => {
      const total = totalByProductId[p.id] || p.member_count || 0;
      const active = activeByProductId[p.id] || 0;
      return {
        product: p.title,
        total,
        active,
        percentActive: total > 0 ? (active / total * 100) : 0,
      };
    }).filter(p => p.total > 0).sort((a, b) => b.total - a.total);

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

    // Steven Essa attendees (workshop tickets) — used to classify payments
    let essaAttendees = [];
    try {
      const essa = await fetchEssaData(force);
      essaAttendees = essa.attendees || [];
    } catch (e) {
      console.error('Essa attendees fetch error:', e.message);
    }

    // NMI transactions — current-year fetch (multi-year was rejected by NMI API)
    let nmiData = { transactions: [], total: 0, count: 0 };
    try {
      const nmiParams = new URLSearchParams({
        username: 'api_key',
        password: NMI_KEY,
        start_date: `${now.getFullYear()}0101`,
        end_date: formatNMIDate(now),
      });
      const nmiResp = await fetch(`${NMI_BASE}?${nmiParams}`);
      nmiData = parseNMIXml(await nmiResp.text());
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

    // Diagnostic: count subscription_cycle records by status to see what WHOP returns.
    const _scStatusCounts = { paid: 0, open: 0, void: 0, other: 0 };
    const _scStatusSums   = { paid: 0, open: 0, void: 0, other: 0 };
    payments.forEach(p => {
      if (p.billing_reason !== 'subscription_cycle') return;
      if (!isNativeWhop(p)) return;
      const st = String(p.status || '').toLowerCase();
      const amt = p.usd_total || p.total || 0;
      if (st === 'paid' || st === 'open' || st === 'void') {
        _scStatusCounts[st]++; _scStatusSums[st] += amt;
      } else {
        _scStatusCounts.other++; _scStatusSums.other += amt;
      }
    });
    console.log('subscription_cycle status mix:', _scStatusCounts);
    console.log('subscription_cycle $ by status:', Object.fromEntries(Object.entries(_scStatusSums).map(([k,v])=>[k,Math.round(v)])));

    // WHOP MRR — current month (paid only, excludes failed/open/void)
    const whopRecurringThisMonth = payments.filter(p => {
      if (String(p.status || '').toLowerCase() !== 'paid') return false;
      const date = new Date(p.paid_at || p.created_at);
      const mk = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      return mk === currentMonth && p.billing_reason === 'subscription_cycle' && isNativeWhop(p);
    });
    const whopMrr = whopRecurringThisMonth.reduce((s, p) => s + (p.usd_total || p.total || 0), 0);

    // WHOP MRR by month (for trend) — paid only, 2025+ only.
    // Also collect contributing rows by month (2026 only, to keep payload small)
    const whopMrrByMonth = {};
    const whopMrrDetailsByMonth = {};
    payments.forEach(p => {
      if (p.billing_reason !== 'subscription_cycle') return;
      if (!isNativeWhop(p)) return;
      if (String(p.status || '').toLowerCase() !== 'paid') return;
      const date = new Date(p.paid_at || p.created_at);
      if (date.getFullYear() < 2025) return;
      const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      const amount = p.usd_total || p.total || 0;
      if (!whopMrrByMonth[monthKey]) whopMrrByMonth[monthKey] = 0;
      whopMrrByMonth[monthKey] += amount;
      if (date.getFullYear() === 2026) {
        if (!whopMrrDetailsByMonth[monthKey]) whopMrrDetailsByMonth[monthKey] = [];
        whopMrrDetailsByMonth[monthKey].push({
          source: 'WHOP',
          date: p.paid_at || p.created_at,
          amount,
          method: p.payment_method_type || '',
          name: p.user?.name || p.user?.username || '',
          email: p.user?.email || '',
        });
      }
    });

    // ── WHOP New Sales by month — initial signups + one-time purchases (anything NOT subscription_cycle) ──
    const whopNewSalesByMonth = {};
    payments.forEach(p => {
      if (p.billing_reason === 'subscription_cycle') return;
      if (String(p.status || '').toLowerCase() !== 'paid') return;
      if (!isNativeWhop(p)) return;
      const date = new Date(p.paid_at || p.created_at);
      if (date.getFullYear() < 2025) return;
      const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      const amount = p.usd_total || p.total || 0;
      if (!whopNewSalesByMonth[monthKey]) whopNewSalesByMonth[monthKey] = 0;
      whopNewSalesByMonth[monthKey] += amount;
    });

    // ── NMI MRR (live from full year fetch) ──
    const nmiMrrByMonth = {};
    const nmiMrrDetailsByMonth = {};
    nmiData.transactions.forEach(t => {
      if (t.condition !== 'complete' && t.condition !== 'pendingsettlement') return;
      const ds = String(t.date || '');
      if (ds.length < 6) return;
      const monthKey = `${ds.substring(0, 4)}-${ds.substring(4, 6)}`;
      if (!nmiMrrByMonth[monthKey]) nmiMrrByMonth[monthKey] = 0;
      nmiMrrByMonth[monthKey] += t.amount;
      if (ds.substring(0, 4) === '2026' && ds.length >= 8) {
        if (!nmiMrrDetailsByMonth[monthKey]) nmiMrrDetailsByMonth[monthKey] = [];
        nmiMrrDetailsByMonth[monthKey].push({
          source: 'NMI',
          date: `${ds.substring(0,4)}-${ds.substring(4,6)}-${ds.substring(6,8)}`,
          amount: t.amount,
          method: 'card',
          name: `${t.first_name||''} ${t.last_name||''}`.trim() || '',
          email: t.email || '',
        });
      }
    });
    const nmiMrr = nmiMrrByMonth[currentMonth] || 0;

    // ── Combined MRR ──
    const mrr = whopMrr + nmiMrr;

    // ── Run-Rate MRR (forward-looking) ──
    // WHOP: sum each active membership's plan renewal_price normalized to a monthly figure.
    // NMI: 3-month trailing average of captured NMI MRR (no native subscription concept on NMI).
    const planById = {};
    plans.forEach(p => { planById[p.id] = p; });

    // Per-price fallout rate from trailing 3 complete months of WHOP subscription_cycle payments.
    // Used to net-down the gross run-rate by realistic payment failure expectations.
    const falloutByPriceAccum = {};
    const threeMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 3, 1);
    payments.forEach(p => {
      if (p.billing_reason !== 'subscription_cycle') return;
      const d = new Date(p.paid_at || p.created_at);
      if (isNaN(d) || d < threeMonthsAgo) return;
      const mk = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      if (mk === currentMonth) return; // exclude in-progress month
      const amt = Math.round(p.usd_total || p.total || 0);
      if (!amt) return;
      const status = String(p.status || '').toLowerCase();
      const isPaid = status === 'paid';
      const isFailed = status === 'open' || status === 'failed' || status === 'declined';
      if (!isPaid && !isFailed) return;
      if (!falloutByPriceAccum[amt]) falloutByPriceAccum[amt] = { paid: 0, failed: 0 };
      if (isPaid) falloutByPriceAccum[amt].paid++; else falloutByPriceAccum[amt].failed++;
    });
    // Only the $97 tier has enough signal AND a meaningful failure rate to apply downstream.
    // Other tiers ($247, $497, $997, $970 annual, etc.) are treated as 0% fallout so the
    // run-rate / forecast / projected-net numbers don't double-discount them.
    const falloutByPrice = {};
    const _p97 = falloutByPriceAccum[97];
    if (_p97 && (_p97.paid + _p97.failed) >= 3) {
      falloutByPrice[97] = _p97.failed / (_p97.paid + _p97.failed);
    }

    // ── Cohort-aware $97 fallout: first-month conversions vs month-2+ recurring ──
    // Step 1: Build chronological list of paid $97 subscription_cycle charges per email.
    const paid97DatesByEmail = {};
    payments.forEach(p => {
      if (p.billing_reason !== 'subscription_cycle') return;
      if (Math.round(p.usd_total || p.total || 0) !== 97) return;
      if (String(p.status || '').toLowerCase() !== 'paid') return;
      if (!isNativeWhop(p)) return;
      const email = String(p.user?.email || '').toLowerCase();
      if (!email) return;
      const d = new Date(p.paid_at || p.created_at);
      if (isNaN(d)) return;
      if (!paid97DatesByEmail[email]) paid97DatesByEmail[email] = [];
      paid97DatesByEmail[email].push(d.getTime());
    });
    Object.values(paid97DatesByEmail).forEach(arr => arr.sort((a, b) => a - b));

    // Step 2: For each $97 subscription_cycle attempt in trailing 3 complete months,
    // bucket as first-month (0 prior $97 paid) or recurring (1+).
    const fallout97CohortAccum = {
      firstMonth: { paid: 0, failed: 0 },
      recurring:  { paid: 0, failed: 0 },
    };
    payments.forEach(p => {
      if (p.billing_reason !== 'subscription_cycle') return;
      if (Math.round(p.usd_total || p.total || 0) !== 97) return;
      if (!isNativeWhop(p)) return;
      const d = new Date(p.paid_at || p.created_at);
      if (isNaN(d) || d < threeMonthsAgo) return;
      const mk = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
      if (mk === currentMonth) return;
      const status = String(p.status || '').toLowerCase();
      const isPaid = status === 'paid';
      const isFailed = status === 'open' || status === 'failed' || status === 'declined';
      if (!isPaid && !isFailed) return;
      const email = String(p.user?.email || '').toLowerCase();
      const t = d.getTime();
      const prior = (paid97DatesByEmail[email] || []).filter(td => td < t).length;
      const cohort = prior === 0 ? 'firstMonth' : 'recurring';
      if (isPaid) fallout97CohortAccum[cohort].paid++;
      else        fallout97CohortAccum[cohort].failed++;
    });

    // Step 3: Convert to rates. Require ≥3 samples per cohort; otherwise fall back to the
    // blended $97 rate (falloutByPrice[97]) so we don't get noisy single-sample rates.
    const fallout97ByCohort = {};
    ['firstMonth', 'recurring'].forEach(c => {
      const v = fallout97CohortAccum[c];
      const total = v.paid + v.failed;
      fallout97ByCohort[c] = {
        rate: total >= 3 ? v.failed / total : (falloutByPrice[97] || 0),
        paid: v.paid,
        failed: v.failed,
        total,
        usedBlended: total < 3,
      };
    });

    // Helper: classify an upcoming/forecast $97 rebill by the email's CURRENT prior-paid count.
    // Returns the cohort name and rate. For non-$97 prices, falls back to falloutByPrice or 0.
    const rateFor = (email, price, asOfTime) => {
      if (Math.round(price) !== 97) return { rate: falloutByPrice[Math.round(price)] || 0, cohort: null };
      const e = String(email || '').toLowerCase();
      const prior = (paid97DatesByEmail[e] || []).filter(td => !asOfTime || td < asOfTime).length;
      const cohort = prior === 0 ? 'firstMonth' : 'recurring';
      return { rate: fallout97ByCohort[cohort].rate, cohort };
    };

    const planBreakdown = {}; // tier breakdown for the UI: { '$97 monthly': { count, monthly, sum } }
    let whopRunRateMrr = 0;
    let whopRunRateMrrNet = 0;
    let whopRunRateCount = 0;
    activeMemberships.forEach(m => {
      const pl = planById[m.plan?.id];
      if (!pl) return;
      const type = (pl.plan_type || '').toLowerCase();
      if (type === 'one_time') return;
      const period = Number(pl.billing_period || 0);   // WHOP billing_period is in days
      const price = Number(pl.renewal_price ?? pl.initial_price ?? 0);
      if (!period || !price) return;
      const monthly = price * 30 / period;
      const fo = falloutByPrice[Math.round(price)] || 0;
      const monthlyNet = monthly * (1 - fo);
      whopRunRateMrr += monthly;
      whopRunRateMrrNet += monthlyNet;
      whopRunRateCount++;
      const tierKey = `$${price} / ${period}d`;
      if (!planBreakdown[tierKey]) {
        planBreakdown[tierKey] = { price, period, count: 0, monthlyEach: monthly, monthlyEachNet: monthlyNet, falloutRate: fo, sum: 0, sumNet: 0 };
      }
      planBreakdown[tierKey].count++;
      planBreakdown[tierKey].sum += monthly;
      planBreakdown[tierKey].sumNet += monthlyNet;
    });

    // NMI run-rate proxy: trailing 3 complete months' average
    const nmiMonthKeys = Object.keys(nmiMrrByMonth).sort();
    const currentMk = currentMonth;
    const completeNmiKeys = nmiMonthKeys.filter(k => k !== currentMk).slice(-3);
    const nmiRunRateMrr = completeNmiKeys.length
      ? completeNmiKeys.reduce((s, k) => s + nmiMrrByMonth[k], 0) / completeNmiKeys.length
      : 0;

    const runRateMrr = whopRunRateMrr + nmiRunRateMrr;
    // NMI run-rate already reflects actual captured revenue (failed attempts never landed in nmiMrrByMonth),
    // so its net == gross. Only WHOP needs fallout discounting.
    const runRateMrrNet = whopRunRateMrrNet + nmiRunRateMrr;

    // ── Upcoming WHOP rebills this month (renewal_period_end in current month, future-dated) ──
    const upcomingWhopByMonth = {};
    activeMemberships.forEach(m => {
      if (!m.renewal_period_end) return;
      if (m.cancel_at_period_end === true) return; // they've cancelled, won't rebill
      const dt = new Date(m.renewal_period_end);
      if (isNaN(dt)) return;
      if (dt < now) return; // already past
      const mk = `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}`;
      if (mk !== currentMonth) return;
      const pl = planById[m.plan?.id];
      if (!pl) return;
      const type = (pl.plan_type || '').toLowerCase();
      if (type === 'one_time') return;
      const price = Number(pl.renewal_price ?? pl.initial_price ?? 0);
      if (!price) return;
      if (!upcomingWhopByMonth[mk]) upcomingWhopByMonth[mk] = [];
      const { rate: foRate, cohort } = rateFor(m.user?.email, price);
      upcomingWhopByMonth[mk].push({
        source: 'WHOP',
        status: 'upcoming',
        date: m.renewal_period_end,
        amount: price,
        method: 'scheduled',
        name: m.user?.name || m.user?.username || '',
        email: m.user?.email || '',
        cohort,
        falloutRate: foRate,
      });
    });
    const upcomingWhopMrr = (upcomingWhopByMonth[currentMonth] || []).reduce((s, r) => s + r.amount, 0);

    // ── Upcoming NMI rebills (predicted from per-customer cadence) ──
    // NMI exposes only transactions, so infer recurrence: customers with >=2 charges
    // at the same amount within sensible cadence (7–60 days) are projected forward.
    const nmiByEmail = {};
    nmiData.transactions.forEach(t => {
      if (t.condition !== 'complete' && t.condition !== 'pendingsettlement') return;
      const email = String(t.email || '').toLowerCase();
      if (!email) return;
      const ds = String(t.date || '');
      if (ds.length < 8) return;
      const dt = new Date(`${ds.substring(0,4)}-${ds.substring(4,6)}-${ds.substring(6,8)}`);
      if (isNaN(dt)) return;
      if (!nmiByEmail[email]) nmiByEmail[email] = [];
      nmiByEmail[email].push({ date: dt, amount: t.amount, first_name: t.first_name, last_name: t.last_name });
    });
    const upcomingNmiByMonth = {};
    const DAY_MS = 24 * 60 * 60 * 1000;
    Object.entries(nmiByEmail).forEach(([email, txs]) => {
      if (txs.length < 2) return;
      txs.sort((a, b) => a.date - b.date);
      const last = txs[txs.length - 1];
      const prev = txs[txs.length - 2];
      // Require recurring amount match (same plan price across last two charges)
      if (Math.round(last.amount) !== Math.round(prev.amount)) return;
      const cadenceDays = Math.round((last.date - prev.date) / DAY_MS);
      if (cadenceDays < 7 || cadenceDays > 60) return;
      const predicted = new Date(last.date.getTime() + cadenceDays * DAY_MS);
      if (predicted < now) return;
      const mk = `${predicted.getFullYear()}-${String(predicted.getMonth() + 1).padStart(2, '0')}`;
      if (mk !== currentMonth) return;
      // Skip if a charge has already landed this month for them
      const paidThisMonth = txs.some(t => {
        const tmk = `${t.date.getFullYear()}-${String(t.date.getMonth() + 1).padStart(2, '0')}`;
        return tmk === currentMonth;
      });
      if (paidThisMonth) return;
      if (!upcomingNmiByMonth[mk]) upcomingNmiByMonth[mk] = [];
      upcomingNmiByMonth[mk].push({
        source: 'NMI',
        status: 'upcoming',
        date: predicted.toISOString(),
        amount: last.amount,
        method: 'predicted',
        name: `${last.first_name || ''} ${last.last_name || ''}`.trim(),
        email,
      });
    });
    const upcomingNmiMrr = (upcomingNmiByMonth[currentMonth] || []).reduce((s, r) => s + r.amount, 0);

    // ── MRR Forecast: simulate each active sub's billings through end of current year ──
    // For each WHOP active membership, project next-renewal forward by billing_period until year end.
    // For each NMI customer with inferred cadence, project next charge forward by cadenceDays.
    // Apply per-price fallout to each projected charge for the net figure.
    const mrrForecast = {};
    const FORECAST_END = new Date(now.getFullYear(), 11, 31, 23, 59, 59);
    const bumpForecast = (mk, price, source, rate) => {
      const fo = rate ?? (falloutByPrice[Math.round(price)] || 0);
      if (!mrrForecast[mk]) {
        mrrForecast[mk] = { gross: 0, net: 0, count: 0, whopGross: 0, nmiGross: 0, firstMonthCount: 0, recurringCount: 0 };
      }
      mrrForecast[mk].gross += price;
      mrrForecast[mk].net   += price * (1 - fo);
      mrrForecast[mk].count++;
      if (source === 'WHOP') mrrForecast[mk].whopGross += price;
      else mrrForecast[mk].nmiGross += price;
    };

    // WHOP projection — cohort-aware: the FIRST projected charge per membership uses the
    // member's current "firstMonth" vs "recurring" status; every subsequent projected charge
    // is by definition month-2+, so always "recurring".
    activeMemberships.forEach(m => {
      if (m.cancel_at_period_end === true) return;
      const pl = planById[m.plan?.id];
      if (!pl) return;
      const type = (pl.plan_type || '').toLowerCase();
      if (type === 'one_time') return;
      const period = Number(pl.billing_period || 0);
      const price = Number(pl.renewal_price ?? pl.initial_price ?? 0);
      if (!period || !price) return;
      let nextDate = m.renewal_period_end ? new Date(m.renewal_period_end) : null;
      if (!nextDate || isNaN(nextDate) || nextDate < now) {
        nextDate = new Date(now.getTime() + period * DAY_MS);
      }
      const isP97 = Math.round(price) === 97;
      const firstClassification = isP97 ? rateFor(m.user?.email, price) : null;
      const recurringRate = isP97 ? fallout97ByCohort.recurring.rate : (falloutByPrice[Math.round(price)] || 0);
      let chargeIdx = 0;
      let guard = 0;
      while (nextDate <= FORECAST_END && guard++ < 60) {
        const mk = `${nextDate.getFullYear()}-${String(nextDate.getMonth() + 1).padStart(2, '0')}`;
        let rate;
        let cohort = null;
        if (isP97) {
          if (chargeIdx === 0 && firstClassification) { rate = firstClassification.rate; cohort = firstClassification.cohort; }
          else { rate = recurringRate; cohort = 'recurring'; }
        } else {
          rate = falloutByPrice[Math.round(price)] || 0;
        }
        bumpForecast(mk, price, 'WHOP', rate);
        if (cohort === 'firstMonth') mrrForecast[mk].firstMonthCount++;
        else if (cohort === 'recurring') mrrForecast[mk].recurringCount++;
        nextDate = new Date(nextDate.getTime() + period * DAY_MS);
        chargeIdx++;
      }
    });

    // NMI projection (re-use nmiByEmail built earlier for upcoming-this-month logic)
    Object.entries(nmiByEmail).forEach(([email, txs]) => {
      if (txs.length < 2) return;
      const sorted = [...txs].sort((a, b) => a.date - b.date);
      const last = sorted[sorted.length - 1];
      const prev = sorted[sorted.length - 2];
      if (Math.round(last.amount) !== Math.round(prev.amount)) return;
      const cadenceDays = Math.round((last.date - prev.date) / DAY_MS);
      if (cadenceDays < 7 || cadenceDays > 60) return;
      let nextDate = new Date(last.date.getTime() + cadenceDays * DAY_MS);
      while (nextDate < now) nextDate = new Date(nextDate.getTime() + cadenceDays * DAY_MS);
      let guard = 0;
      while (nextDate <= FORECAST_END && guard++ < 60) {
        const mk = `${nextDate.getFullYear()}-${String(nextDate.getMonth() + 1).padStart(2, '0')}`;
        // NMI uses blended rate for $97 (we don't have native subscription state to identify trial conversions).
        const rate = falloutByPrice[Math.round(last.amount)] || 0;
        bumpForecast(mk, last.amount, 'NMI', rate);
        nextDate = new Date(nextDate.getTime() + cadenceDays * DAY_MS);
      }
    });

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
        whopNewSales: whopNewSalesByMonth[m] || 0,
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

    // ── $997 Recurring members — full untrimmed list (WHOP recurring + NMI $997) ──
    // WHOP /payments returns paid + open (failed) + void (refunded) records mixed,
    // so we explicitly require status === 'paid' to exclude refunded/voided charges.
    const whop997 = payments
      .filter(p => p.billing_reason === 'subscription_cycle'
        && (p.usd_total === 997 || p.total === 997)
        && String(p.status || '').toLowerCase() === 'paid')
      .map(p => ({
        source: 'WHOP',
        amount: p.usd_total || p.total || 0,
        date: p.paid_at || p.created_at,
        user: p.user?.name || p.user?.username || 'Unknown',
        email: p.user?.email || '',
        userId: p.user?.id || '',
        method: p.payment_method_type || '',
      }));

    const nmi997 = (nmiData.transactions || [])
      .filter(t => Math.round(t.amount) === 997 && (t.condition === 'complete' || t.condition === 'pendingsettlement'))
      .map(t => {
        const ds = String(t.date || '');
        const isoDate = ds.length >= 8
          ? `${ds.substring(0,4)}-${ds.substring(4,6)}-${ds.substring(6,8)}`
          : '';
        return {
          source: 'NMI',
          amount: t.amount,
          date: isoDate,
          user: `${t.first_name||''} ${t.last_name||''}`.trim() || 'Unknown',
          email: t.email || '',
          userId: '',
          method: 'card',
          condition: t.condition,
        };
      });

    const recurring997 = [...whop997, ...nmi997]
      .filter(r => r.date)
      .sort((a, b) => new Date(b.date) - new Date(a.date));

    // ── Deduped per-member view with WHOP membership status + membership-since date ──
    // Build lookups of WHOP memberships by user.id / user_id / email (latest wins)
    const membershipByKey = {};
    const setLatest = (key, m) => {
      if (!key) return;
      const existing = membershipByKey[key];
      if (!existing || new Date(m.created_at || 0) > new Date(existing.created_at || 0)) {
        membershipByKey[key] = m;
      }
    };
    memberships.forEach(m => {
      setLatest(m.user?.id, m);
      setLatest(m.user_id, m);
      const email = (m.user?.email || m.email || '').toLowerCase();
      if (email) setLatest(`email:${email}`, m);
    });
    const findMembership = (userId, email) => {
      const e = (email || '').toLowerCase();
      return membershipByKey[userId] || (e && membershipByKey[`email:${e}`]) || {};
    };

    const memberAgg = {};
    recurring997.forEach(r => {
      const key = (r.email || r.user || '').toLowerCase() + '|' + r.source;
      if (!memberAgg[key]) {
        let status = 'unknown';
        let membershipDate = r.date;
        if (r.source === 'WHOP') {
          const m = findMembership(r.userId, r.email);
          const rawStatus = m.status || 'unknown';
          status = m.cancel_at_period_end ? 'canceling' : rawStatus;
          if (m.created_at) membershipDate = m.created_at;
        } else {
          status = r.condition === 'complete' ? 'active' : 'pending';
        }
        memberAgg[key] = {
          source: r.source,
          user: r.user,
          email: r.email,
          amount: r.amount,
          lastPaymentDate: r.date,
          membershipDate,
          status,
          paymentCount: 0,
          totalPaid: 0,
        };
      }
      const agg = memberAgg[key];
      agg.paymentCount++;
      agg.totalPaid += r.amount;
      if (new Date(r.date) > new Date(agg.lastPaymentDate)) {
        agg.lastPaymentDate = r.date;
        agg.amount = r.amount;
      }
      if (r.source === 'NMI' && new Date(r.date) < new Date(agg.membershipDate)) {
        agg.membershipDate = r.date;
      }
    });
    const recurring997Members = Object.values(memberAgg)
      .sort((a, b) => new Date(b.lastPaymentDate) - new Date(a.lastPaymentDate));

    // ── DeFi Inner Circle ($97) — Failed Payment Attempts, aggregated per member ──
    // WHOP marks failed subscription rebills as status='open' (also 'failed'/'declined').
    // We scope to subscription_cycle attempts at $97 on the Inner Circle product.
    const ic97FailedAgg = {};
    payments.forEach(p => {
      if (p.billing_reason !== 'subscription_cycle') return;
      const amt = Math.round(p.usd_total || p.total || 0);
      if (amt !== 97) return;
      const status = String(p.status || '').toLowerCase();
      if (status !== 'open' && status !== 'failed' && status !== 'declined') return;
      const product = p.product?.title || '';
      // Belt-and-braces: only Inner Circle (skip any other $97-priced product).
      if (product && !product.toLowerCase().includes('inner circle')) return;
      const email = (p.user?.email || '').toLowerCase();
      const key = email || (p.user?.id || p.user?.username || 'unknown');
      const dateStr = p.paid_at || p.created_at;
      if (!ic97FailedAgg[key]) {
        ic97FailedAgg[key] = {
          user: p.user?.name || p.user?.username || 'Unknown',
          email: p.user?.email || '',
          product: product || 'DeFi Inner Circle',
          attempts: 0,
          totalFailed: 0,
          lastAttemptDate: dateStr,
          firstAttemptDate: dateStr,
          lastStatus: status,
          method: p.payment_method_type || '',
        };
      }
      const row = ic97FailedAgg[key];
      row.attempts++;
      row.totalFailed += (p.usd_total || p.total || 0);
      if (dateStr && (!row.lastAttemptDate || new Date(dateStr) > new Date(row.lastAttemptDate))) {
        row.lastAttemptDate = dateStr;
        row.lastStatus = status;
        row.method = p.payment_method_type || row.method;
      }
      if (dateStr && (!row.firstAttemptDate || new Date(dateStr) < new Date(row.firstAttemptDate))) {
        row.firstAttemptDate = dateStr;
      }
    });
    // Enrich with WHOP membership status so user can see if member is still active / canceled.
    Object.values(ic97FailedAgg).forEach(r => {
      const m = findMembership(null, r.email);
      r.memberStatus = m.cancel_at_period_end ? 'canceling' : (m.status || 'unknown');
    });
    const innerCircle97Failed = Object.values(ic97FailedAgg)
      .sort((a, b) => new Date(b.lastAttemptDate) - new Date(a.lastAttemptDate));

    // ── Active NMI members ──
    // NMI has no "membership status" — proxy by unique payers whose latest captured
    // transaction is within the last 35 days (covers monthly cycle + a few days slack).
    const activeWindowMs = 35 * 24 * 60 * 60 * 1000;
    const activeCutoff = Date.now() - activeWindowMs;
    const activeNmiEmails = new Set();
    (nmiData.transactions || []).forEach(t => {
      if (t.condition !== 'complete' && t.condition !== 'pendingsettlement') return;
      const ds = String(t.date || '');
      if (ds.length < 8) return;
      const iso = `${ds.substring(0,4)}-${ds.substring(4,6)}-${ds.substring(6,8)}`;
      const ts = new Date(iso).getTime();
      if (isNaN(ts) || ts < activeCutoff) return;
      const key = (t.email || `${t.first_name||''} ${t.last_name||''}`.trim()).toLowerCase();
      if (key) activeNmiEmails.add(key);
    });
    const activeMembersNmi = activeNmiEmails.size;
    const activeMembersWhop = activeMemberships.length;

    // ── Failed payments by product, bucketed by month (2025+) ──
    // Per-user calculation: dedupe by user within each month-product so that
    // a user's repeated attempts only count once toward paid/failed.
    // status='open' is treated as a failed billing attempt; status='paid' as success.
    const failureByMonthSets = {};
    payments.forEach(p => {
      const date = new Date(p.paid_at || p.created_at);
      if (isNaN(date) || date.getFullYear() < 2025) return;
      const status = String(p.status || '').toLowerCase();
      const isPaid = status === 'paid';
      const isFailed = status === 'open' || status === 'failed' || status === 'declined';
      if (!isPaid && !isFailed) return;
      const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      const product = p.product?.title || 'Other';
      const userKey = p.user?.id || (p.user?.email || '').toLowerCase() || `pid:${p.id}`;
      if (!failureByMonthSets[monthKey]) failureByMonthSets[monthKey] = {};
      if (!failureByMonthSets[monthKey][product]) failureByMonthSets[monthKey][product] = { paidUsers: new Set(), failedUsers: new Set() };
      const bucket = failureByMonthSets[monthKey][product];
      if (isPaid) bucket.paidUsers.add(userKey);
      else bucket.failedUsers.add(userKey);
    });
    // Serialize Sets to arrays for the JSON response
    const failureByMonth = {};
    Object.entries(failureByMonthSets).forEach(([m, prods]) => {
      failureByMonth[m] = {};
      Object.entries(prods).forEach(([prod, s]) => {
        failureByMonth[m][prod] = {
          paidUsers: [...s.paidUsers],
          failedUsers: [...s.failedUsers],
        };
      });
    });

    // ── $97 subscription fallout rate (rebill attempts that failed) ──
    // status='paid' is success, status='open'/'failed'/'declined' is fallout, 'void' (refunds) excluded.
    const fallout97ByMonth = {};
    payments.forEach(p => {
      if (p.billing_reason !== 'subscription_cycle') return;
      const amt = p.usd_total || p.total || 0;
      if (Math.round(amt) !== 97) return;
      const date = new Date(p.paid_at || p.created_at);
      if (isNaN(date)) return;
      const status = String(p.status || '').toLowerCase();
      const isPaid = status === 'paid';
      const isFailed = status === 'open' || status === 'failed' || status === 'declined';
      if (!isPaid && !isFailed) return;
      const mk = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
      if (!fallout97ByMonth[mk]) fallout97ByMonth[mk] = { paid: 0, failed: 0 };
      if (isPaid) fallout97ByMonth[mk].paid++; else fallout97ByMonth[mk].failed++;
    });
    const fallout97Months = Object.keys(fallout97ByMonth).sort();
    const lastCompleteMonthKey = fallout97Months.filter(m => m !== currentMonth).pop() || null;
    const lastComplete = lastCompleteMonthKey ? fallout97ByMonth[lastCompleteMonthKey] : null;
    const last3Keys = fallout97Months.slice(-3);
    let r3paid = 0, r3failed = 0;
    last3Keys.forEach(k => { r3paid += fallout97ByMonth[k].paid; r3failed += fallout97ByMonth[k].failed; });
    const mtdKey = currentMonth;
    const mtd = fallout97ByMonth[mtdKey] || null;
    const pct = (f, total) => total > 0 ? Math.round(f / total * 1000) / 10 : null;
    const fallout97 = {
      byMonth: fallout97ByMonth,
      lastCompleteMonth: lastCompleteMonthKey,
      lastCompleteRate: lastComplete ? pct(lastComplete.failed, lastComplete.paid + lastComplete.failed) : null,
      lastCompletePaid: lastComplete?.paid || 0,
      lastCompleteFailed: lastComplete?.failed || 0,
      rolling3MonthRate: pct(r3failed, r3paid + r3failed),
      rolling3MonthPaid: r3paid,
      rolling3MonthFailed: r3failed,
      mtdRate: mtd ? pct(mtd.failed, mtd.paid + mtd.failed) : null,
      mtdPaid: mtd?.paid || 0,
      mtdFailed: mtd?.failed || 0,
    };

    // Trim paymentsByMonth — keep the 50 MOST RECENT payments per month.
    // (WHOP returns newest-first, but sort explicitly so we don't depend on API ordering.)
    const trimmedPaymentsByMonth = {};
    Object.entries(paymentsByMonth).forEach(([month, data]) => {
      const sorted = [...data.payments].sort((a, b) => new Date(b.date) - new Date(a.date));
      trimmedPaymentsByMonth[month] = {
        revenue: data.revenue,
        count: data.count,
        payments: sorted.slice(0, 50),
      };
    });

    // Successful NMI captures across all years (for the recent-payments table)
    const nmiCaptures = (nmiData.transactions || []).filter(
      t => t.condition === 'complete' || t.condition === 'pendingsettlement'
    );
    // Current-year reserve preserves the original "this year" semantics for the KPI
    const currentYearStr = String(now.getFullYear());
    const nmiTotalThisYear = nmiCaptures
      .filter(t => String(t.date || '').startsWith(currentYearStr))
      .reduce((s, t) => s + t.amount, 0);
    const nmiTrimmed = {
      total: nmiTotalThisYear,
      count: nmiCaptures.length,
      transactions: nmiCaptures,
    };

    // ── Enrich Steven Essa attendees with WHOP membership status + total paid ──
    // Match by email (primary) or by full-name token match (fallback).
    if (essaAttendees.length) {
      const tokenize = s => (s || '').toLowerCase().split(/[^a-z']+/).filter(Boolean);
      const membershipsWithKeys = memberships.map(m => ({
        m,
        email: (m.user?.email || '').toLowerCase(),
        nameTokens: tokenize(m.user?.name || m.user?.username || ''),
      }));
      const paidWhopRows = payments
        .filter(p => String(p.status || '').toLowerCase() === 'paid')
        .map(p => ({
          email: (p.user?.email || '').toLowerCase(),
          nameTokens: tokenize(p.user?.name || p.user?.username || ''),
          amount: p.usd_total || p.total || 0,
        }));

      essaAttendees = essaAttendees.map(a => {
        const aEmail = (a.email || '').toLowerCase();
        const aFirst = (a.first || '').toLowerCase();
        const aLast = (a.last || '').toLowerCase();
        const matches = (email, nameTokens) => {
          if (aEmail && email && email === aEmail) return true;
          if (aFirst && aLast && nameTokens.includes(aFirst) && nameTokens.includes(aLast)) return true;
          return false;
        };

        // Pick latest matching membership
        let membership = null;
        for (const mw of membershipsWithKeys) {
          if (matches(mw.email, mw.nameTokens)) {
            if (!membership || new Date(mw.m.created_at || 0) > new Date(membership.created_at || 0)) {
              membership = mw.m;
            }
          }
        }
        const whopStatus = membership
          ? (membership.cancel_at_period_end ? 'canceling' : (membership.status || ''))
          : '';

        // Sum WHOP paid amounts
        let whopPaid = 0;
        for (const r of paidWhopRows) {
          if (matches(r.email, r.nameTokens)) whopPaid += r.amount;
        }

        return { ...a, whopStatus, whopPaid };
      });
    }

    dashboardCache = {
      summary: {
        totalRevenue,
        thisMonthRevenue: thisMonthRevenue + nmiMrr,
        last30Revenue,
        mrr,
        whopMrr,
        nmiMrr,
        runRateMrr,
        runRateMrrNet,
        whopRunRateMrr,
        whopRunRateMrrNet,
        nmiRunRateMrr,
        whopRunRateCount,
        nmiRunRateMonths: completeNmiKeys.length,
        upcomingWhopMrr,
        upcomingWhopCount: (upcomingWhopByMonth[currentMonth] || []).length,
        upcomingNmiMrr,
        upcomingNmiCount: (upcomingNmiByMonth[currentMonth] || []).length,
        activeMembers: activeMemberships.length,
        activeMembersWhop,
        activeMembersNmi,
        cancelingMembers: cancelingMemberships.length,
        totalPayments: payments.length,
        nmiReserve: nmiTotalThisYear,
        fallout97Rate: fallout97.lastCompleteRate,
        fallout97Rolling3: fallout97.rolling3MonthRate,
        fallout97MTD: fallout97.mtdRate,
        fallout97LastMonth: fallout97.lastCompleteMonth,
      },
      mrrByYear,
      mrrDetails: { whop: whopMrrDetailsByMonth, nmi: nmiMrrDetailsByMonth },
      mrrUpcoming: { whop: upcomingWhopByMonth, nmi: upcomingNmiByMonth },
      mrrForecast,    // { '2026-06': { gross, net, count, whopGross, nmiGross, firstMonthCount, recurringCount }, ... } through Dec
      falloutByPrice, // { 97: 0.36 } — blended failure rate per price tier (used as fallback)
      fallout97ByCohort, // { firstMonth: { rate, paid, failed, total }, recurring: { rate, ... } }
      _diagnostic: {
        subscriptionCycleByStatus: _scStatusCounts,
        subscriptionCycleSumsByStatus: Object.fromEntries(
          Object.entries(_scStatusSums).map(([k, v]) => [k, Math.round(v * 100) / 100])
        ),
      },
      runRateBreakdown: Object.values(planBreakdown).sort((a,b) => b.sum - a.sum),
      paymentsByMonth: trimmedPaymentsByMonth,
      memberships: {
        active: activeMemberships.length,
        canceling: cancelingMemberships.length,
      },
      plans: Object.values(planSummary),
      products: products.map(p => ({ id: p.id, title: p.title, members: p.member_count })),
      nmi: nmiTrimmed,
      recurring997,
      recurring997Members,
      innerCircle97Failed,
      cancelingMembersList,
      essaAttendees,
      failureByMonth,
      fallout97,
      productMembership,
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
// Short TTL — the attendees list is polled live by the dashboard.
// Cache is just to dedupe rapid concurrent requests, not to gate freshness.
const ESSA_CACHE_TTL = 15 * 1000; // 15s
let essaCache = null;
let essaCacheTime = 0;

// Naive CSV row splitter that respects double-quoted fields (handles commas inside quotes).
function splitCsvRow(line) {
  const out = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i+1] === '"') { cur += '"'; i++; }
      else if (ch === '"') inQuotes = false;
      else cur += ch;
    } else {
      if (ch === '"') inQuotes = true;
      else if (ch === ',') { out.push(cur); cur = ''; }
      else cur += ch;
    }
  }
  out.push(cur);
  return out.map(s => s.trim());
}

async function fetchEssaData(force = false) {
  // Return cache if fresh (and not forced)
  if (!force && essaCache && (Date.now() - essaCacheTime) < ESSA_CACHE_TTL) return essaCache;
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

    // Workshop attendees — column layout (header is mis-labeled in source sheet):
    //   0 first name | 1 last name | 2 email | 3 currency | 4 ticket amount |
    //   5 order date | 6 FP/PP | 7 session/plan tier | 8 plan amount |
    //   9 total paid | 10 balance to pay | 11 notes | 12 follow-up date
    // Skip header ("Customer name") and the trailing "Total" / revenue rows.
    const dollarToNum = s => {
      const n = parseFloat(String(s || '').replace(/[^0-9.\-]/g, ''));
      return isNaN(n) ? 0 : n;
    };
    const attendees = [];
    for (const line of lines) {
      if (!line.trim() || line.includes('Workshop Ticket Revenue')) continue;
      const cells = splitCsvRow(line);
      const first = (cells[0] || '').trim();
      const last = (cells[1] || '').trim();
      const email = (cells[2] || '').trim();
      // Skip header row (label varies: "Customer name", "First name", etc.)
      if (/^(customer|first)\s*name$/i.test(first) || /^last\s*name$/i.test(last) || /^customer\s*(email|phone)$/i.test(email)) continue;
      if (!first || /total/i.test(first) || /total/i.test(last)) continue;
      if (!last && !email) continue;
      attendees.push({
        first,
        last,
        email,
        fullName: `${first} ${last}`.trim(),
        ticketAmount: dollarToNum(cells[4]),
        orderDate: (cells[5] || '').trim(),
        session: (cells[7] || '').trim(),
        planAmount: dollarToNum(cells[8]),
        totalPaid: dollarToNum(cells[9]),
        balance: dollarToNum(cells[10]),
        notes: (cells[11] || '').trim(),
        followUpDate: (cells[12] || '').trim(),
      });
    }

    const outstanding = totalPipeline - totalCollected;
    essaCache = { totalPipeline, totalCollected, outstanding, workshopRevenue, attendees, fetchedAt: new Date().toISOString() };
    essaCacheTime = Date.now();
    console.log(`Steven Essa data: Pipeline $${totalPipeline} | Collected $${totalCollected} | Outstanding $${outstanding} | Attendees ${attendees.length}`);
    return essaCache;
  } catch (e) {
    console.error('Essa fetch error:', e.message);
    return essaCache || { totalPipeline: 93000, totalCollected: 41480, outstanding: 51520, workshopRevenue: 17892, attendees: [], fetchedAt: null };
  }
}

app.get('/api/essa', async (req, res) => {
  // Always live — the dashboard polls this on a short interval.
  res.set('Cache-Control', 'no-store');
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
