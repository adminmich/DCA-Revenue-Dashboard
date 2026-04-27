require('dotenv').config();
const fetch = require('node-fetch');
const XLSX = require('xlsx');
const path = require('path');

const WHOP_KEY = process.env.WHOP_API_KEY;
const WHOP_CID = process.env.WHOP_COMPANY_ID;
const WHOP_BASE = 'https://api.whop.com/api/v1';
const headers = { Authorization: `Bearer ${WHOP_KEY}` };

async function fetchAll(endpoint, params = {}) {
  const all = [];
  let cursor = null, pages = 0;
  do {
    const p = new URLSearchParams({ per: '100', company_id: WHOP_CID, ...params });
    if (cursor) p.set('after', cursor);
    const resp = await fetch(`${WHOP_BASE}${endpoint}?${p}`, { headers });
    const data = await resp.json();
    if (data.data) all.push(...data.data);
    cursor = data.page_info?.has_next_page ? data.page_info.end_cursor : null;
    if (++pages > 30) break;
  } while (cursor);
  return all;
}

(async () => {
  const products = await fetchAll('/products');
  const ic = products.filter(p => /inner.?circle/i.test(p.title || ''));
  if (!ic.length) {
    console.log('No "Inner Circle" product found. Available products:');
    products.forEach(p => console.log(`  ${p.id}  ${p.title}`));
    process.exit(1);
  }
  console.log('Inner Circle product(s):');
  ic.forEach(p => console.log(`  ${p.id}  ${p.title}`));

  const memberships = await fetchAll('/memberships', { status: 'active' });
  const icIds = new Set(ic.map(p => p.id));
  const active = memberships.filter(m => icIds.has(m.product?.id));

  const fmt = v => {
    if (v == null || v === '') return '';
    const n = Number(v);
    const d = !Number.isNaN(n) && n > 1e9 ? new Date(n < 1e12 ? n * 1000 : n) : new Date(v);
    return Number.isNaN(d.getTime()) ? String(v) : d.toISOString().slice(0, 10);
  };
  const rows = active.map(m => ({
    Name: m.user?.name || '',
    Username: m.user?.username || '',
    Email: m.user?.email || '',
    Product: m.product?.title || '',
    Plan: m.plan?.internal_notes || m.plan?.id || '',
    Status: m.status || '',
    Canceling: m.cancel_at_period_end ? 'yes' : '',
    RenewalPeriodEnd: fmt(m.renewal_period_end),
    ExpiresAt: fmt(m.expires_at),
    CreatedAt: fmt(m.created_at),
    MembershipId: m.id,
  })).sort((a, b) => (a.Name || a.Email).localeCompare(b.Name || b.Email));

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(rows), 'Active Inner Circle');
  const out = path.join(__dirname, 'inner-circle-active.xlsx');
  XLSX.writeFile(wb, out);
  console.log(`\nWrote ${rows.length} active members to ${out}`);
})().catch(e => { console.error(e); process.exit(1); });
