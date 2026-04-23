require('dotenv').config();
const fetch = require('node-fetch');
const XLSX = require('xlsx');
const NMI_KEY = process.env.NMI_API_KEY;
const WHOP_KEY = process.env.WHOP_API_KEY;
const WHOP_CID = process.env.WHOP_COMPANY_ID;
const NMI_BASE = 'https://secure.networkmerchants.com/api/query.php';
const WHOP_BASE = 'https://api.whop.com/api/v1';

async function getNMIMonth(start, end) {
  const params = new URLSearchParams({ username: 'api_key', password: NMI_KEY, start_date: start, end_date: end });
  const resp = await fetch(NMI_BASE + '?' + params);
  const xml = await resp.text();
  const txns = [];
  xml.split('<transaction>').slice(1).forEach(block => {
    const get = tag => { const m = block.match(new RegExp('<' + tag + '>(.*?)</' + tag + '>')); return m ? m[1] : ''; };
    txns.push({
      date: get('date'), amount: parseFloat(get('amount')) || 0,
      first_name: get('first_name'), last_name: get('last_name'),
      email: get('email'), condition: get('condition'), transaction_id: get('transaction_id'),
    });
  });
  return txns;
}

async function whopFetchAll(endpoint, params = {}) {
  const all = [];
  let cursor = null, pages = 0;
  do {
    const p = new URLSearchParams({ per: '100', company_id: WHOP_CID, ...params });
    if (cursor) p.set('after', cursor);
    const resp = await fetch(WHOP_BASE + endpoint + '?' + p, { headers: { 'Authorization': 'Bearer ' + WHOP_KEY } });
    const data = await resp.json();
    if (data.data) all.push(...data.data);
    cursor = data.page_info?.has_next_page ? data.page_info.end_cursor : null;
    pages++;
    if (pages > 20) break;
  } while (cursor);
  return all;
}

async function run() {
  console.log('Fetching NMI data (Jan-Apr 2026)...');
  const [jan, feb, mar, apr] = await Promise.all([
    getNMIMonth('20260101', '20260131'),
    getNMIMonth('20260201', '20260228'),
    getNMIMonth('20260301', '20260331'),
    getNMIMonth('20260401', '20260423'),
  ]);

  const allNMI = [
    ...jan.map(t => ({ ...t, month: 'Jan 2026' })),
    ...feb.map(t => ({ ...t, month: 'Feb 2026' })),
    ...mar.map(t => ({ ...t, month: 'Mar 2026' })),
    ...apr.map(t => ({ ...t, month: 'Apr 2026' })),
  ];

  const paid = t => t.condition === 'complete' || t.condition === 'pendingsettlement';

  // Find churned: paid in Jan but NOT in Mar or Apr
  const janEmails = new Set(jan.filter(paid).map(t => t.email.toLowerCase()));
  const febEmails = new Set(feb.filter(paid).map(t => t.email.toLowerCase()));
  const marAprEmails = new Set([...mar, ...apr].filter(paid).map(t => t.email.toLowerCase()));

  const churned = [];
  janEmails.forEach(email => {
    if (!marAprEmails.has(email)) {
      const orig = jan.find(t => t.email.toLowerCase() === email);
      churned.push({ email, first_name: orig?.first_name || '', last_name: orig?.last_name || '', last_payment: orig?.amount || 0, last_seen: 'January 2026', status: 'Churned' });
    }
  });
  febEmails.forEach(email => {
    if (!marAprEmails.has(email) && !janEmails.has(email)) {
      const orig = feb.find(t => t.email.toLowerCase() === email);
      churned.push({ email, first_name: orig?.first_name || '', last_name: orig?.last_name || '', last_payment: orig?.amount || 0, last_seen: 'February 2026', status: 'Churned' });
    }
  });

  console.log('Fetching WHOP payments...');
  const whopPayments = await whopFetchAll('/payments', { status: 'paid' });
  console.log('Fetching WHOP memberships...');
  const whopMemberships = await whopFetchAll('/memberships', { status: 'active' });

  // WHOP monthly summary
  const whopByMonth = {};
  whopPayments.forEach(p => {
    const d = new Date(p.paid_at || p.created_at);
    const key = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0');
    if (!whopByMonth[key]) whopByMonth[key] = { revenue: 0, count: 0, recurring: 0, newSubs: 0, other: 0 };
    const amt = p.usd_total || p.total || 0;
    whopByMonth[key].revenue += amt;
    whopByMonth[key].count++;
    if (p.billing_reason === 'subscription_cycle') whopByMonth[key].recurring += amt;
    else if (p.billing_reason === 'subscription_create') whopByMonth[key].newSubs += amt;
    else whopByMonth[key].other += amt;
  });

  // Build Excel
  const wb = XLSX.utils.book_new();

  // Sheet 1: NMI Monthly Summary
  const nmiMonths = [
    { Month: 'Jan 2026', Transactions: jan.filter(paid).length, Revenue: jan.filter(paid).reduce((s, t) => s + t.amount, 0) },
    { Month: 'Feb 2026', Transactions: feb.filter(paid).length, Revenue: feb.filter(paid).reduce((s, t) => s + t.amount, 0) },
    { Month: 'Mar 2026', Transactions: mar.filter(paid).length, Revenue: mar.filter(paid).reduce((s, t) => s + t.amount, 0) },
    { Month: 'Apr 2026 (to 23rd)', Transactions: apr.filter(paid).length, Revenue: apr.filter(paid).reduce((s, t) => s + t.amount, 0) },
  ];
  const nmiTotal = { Month: 'TOTAL', Transactions: nmiMonths.reduce((s, r) => s + r.Transactions, 0), Revenue: nmiMonths.reduce((s, r) => s + r.Revenue, 0) };
  nmiMonths.push(nmiTotal);
  // Add MoM change
  nmiMonths.forEach((r, i) => {
    if (i > 0 && i < 4) r['MoM_Change'] = ((r.Revenue - nmiMonths[i - 1].Revenue) / nmiMonths[i - 1].Revenue * 100).toFixed(1) + '%';
    else r['MoM_Change'] = '';
  });
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(nmiMonths), 'NMI Monthly Summary');

  // Sheet 2: NMI All Transactions
  const nmiAll = allNMI.filter(paid).map(t => ({
    Month: t.month, Date: t.date, Amount: t.amount,
    First_Name: t.first_name, Last_Name: t.last_name, Email: t.email,
    Status: t.condition, Transaction_ID: t.transaction_id,
  }));
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(nmiAll), 'NMI All Transactions');

  // Sheet 3: NMI Churned Subscribers
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(churned.sort((a, b) => a.last_seen.localeCompare(b.last_seen)).map(c => ({
    First_Name: c.first_name, Last_Name: c.last_name, Email: c.email,
    Last_Payment: c.last_payment, Last_Active: c.last_seen, Status: c.status,
  }))), 'NMI Churned Subscribers');

  // Sheet 4: WHOP Monthly Summary
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(Object.keys(whopByMonth).sort().map(k => ({
    Month: k, Total_Revenue: Math.round(whopByMonth[k].revenue * 100) / 100,
    Payments: whopByMonth[k].count,
    Recurring: Math.round(whopByMonth[k].recurring * 100) / 100,
    New_Subscriptions: Math.round(whopByMonth[k].newSubs * 100) / 100,
    Other: Math.round(whopByMonth[k].other * 100) / 100,
  }))), 'WHOP Monthly Summary');

  // Sheet 5: WHOP Recent Payments
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(whopPayments.slice(0, 200).map(p => ({
    Date: (p.paid_at || p.created_at || '').split('T')[0],
    Amount: p.usd_total || p.total || 0,
    Customer: p.user?.name || p.user?.username || '',
    Email: p.user?.email || '',
    Product: p.product?.title || '',
    Plan: p.plan?.internal_notes || '',
    Type: p.billing_reason || '',
    Method: p.payment_method_type || '',
  }))), 'WHOP Recent Payments');

  // Sheet 6: WHOP Active Members
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(whopMemberships.map(m => ({
    Name: m.user?.name || m.user?.username || '',
    Email: m.user?.email || '',
    Product: m.product?.title || '',
    Status: m.status,
    Joined: (m.joined_at || '').split('T')[0],
    Canceling: m.cancel_at_period_end ? 'YES' : 'No',
    Renewal_End: (m.renewal_period_end || '').split('T')[0],
  }))), 'WHOP Active Members');

  // Save
  const filePath = 'C:/Users/User/OneDrive/Desktop/DCA_Financial_Report_2026.xlsx';
  XLSX.writeFile(wb, filePath);
  console.log('\nExcel saved to: ' + filePath);
  console.log('Churned NMI subscribers: ' + churned.length);
  console.log('WHOP payments: ' + whopPayments.length);
  console.log('WHOP active members: ' + whopMemberships.length);
}

run().catch(e => console.error(e));
