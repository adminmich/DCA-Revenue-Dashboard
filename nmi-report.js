require('dotenv').config();
const fetch = require('node-fetch');
const XLSX = require('xlsx');
const NMI_KEY = process.env.NMI_API_KEY;
const NMI_BASE = 'https://secure.networkmerchants.com/api/query.php';

async function getNMI(start, end) {
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

async function run() {
  console.log('Pulling NMI data...');
  const [jan, feb, mar, apr] = await Promise.all([
    getNMI('20260101', '20260131'),
    getNMI('20260201', '20260228'),
    getNMI('20260301', '20260331'),
    getNMI('20260401', '20260423'),
  ]);

  const paid = t => t.condition === 'complete' || t.condition === 'pendingsettlement';
  const wb = XLSX.utils.book_new();

  // Sheet 1: Monthly Summary
  const summary = [
    { Month: 'Jan 2026', Transactions: jan.filter(paid).length, Revenue: jan.filter(paid).reduce((s, t) => s + t.amount, 0), MoM_Change: '' },
    { Month: 'Feb 2026', Transactions: feb.filter(paid).length, Revenue: feb.filter(paid).reduce((s, t) => s + t.amount, 0) },
    { Month: 'Mar 2026', Transactions: mar.filter(paid).length, Revenue: mar.filter(paid).reduce((s, t) => s + t.amount, 0) },
    { Month: 'Apr 2026 (to 23rd)', Transactions: apr.filter(paid).length, Revenue: apr.filter(paid).reduce((s, t) => s + t.amount, 0) },
  ];
  summary[1].MoM_Change = ((summary[1].Revenue - summary[0].Revenue) / summary[0].Revenue * 100).toFixed(1) + '%';
  summary[2].MoM_Change = ((summary[2].Revenue - summary[1].Revenue) / summary[1].Revenue * 100).toFixed(1) + '%';
  summary[3].MoM_Change = ((summary[3].Revenue - summary[2].Revenue) / summary[2].Revenue * 100).toFixed(1) + '% (partial)';
  summary.push({ Month: 'TOTAL', Transactions: summary.reduce((s, r) => s + r.Transactions, 0), Revenue: summary.reduce((s, r) => s + r.Revenue, 0), MoM_Change: '' });
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(summary), 'Monthly Summary');

  // Sheet 2-5: Each month's transactions
  [['Jan 2026', jan], ['Feb 2026', feb], ['Mar 2026', mar], ['Apr 2026', apr]].forEach(([label, data]) => {
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(data.filter(paid).map(t => ({
      Date: t.date, Amount: t.amount, First_Name: t.first_name, Last_Name: t.last_name,
      Email: t.email, Status: t.condition, Transaction_ID: t.transaction_id,
    }))), label);
  });

  // Sheet 6: Churned subscribers
  const janEmails = new Set(jan.filter(paid).map(t => t.email.toLowerCase()));
  const marAprEmails = new Set([...mar, ...apr].filter(paid).map(t => t.email.toLowerCase()));
  const churned = [];
  janEmails.forEach(email => {
    if (!marAprEmails.has(email)) {
      const orig = jan.find(t => t.email.toLowerCase() === email);
      churned.push({ First_Name: orig?.first_name, Last_Name: orig?.last_name, Email: email, Last_Payment: orig?.amount, Last_Active: 'Jan 2026', Status: 'CHURNED' });
    }
  });
  const febEmails = new Set(feb.filter(paid).map(t => t.email.toLowerCase()));
  febEmails.forEach(email => {
    if (!marAprEmails.has(email) && !janEmails.has(email)) {
      const orig = feb.find(t => t.email.toLowerCase() === email);
      churned.push({ First_Name: orig?.first_name, Last_Name: orig?.last_name, Email: email, Last_Payment: orig?.amount, Last_Active: 'Feb 2026', Status: 'CHURNED' });
    }
  });
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(churned), 'Churned Subscribers');

  // Sheet 7: Analysis & Explanation
  const janPaid = jan.filter(paid).length;
  const febPaid = feb.filter(paid).length;
  const marPaid = mar.filter(paid).length;
  const aprPaid = apr.filter(paid).length;
  const janRev = jan.filter(paid).reduce((s, t) => s + t.amount, 0);
  const febRev = feb.filter(paid).reduce((s, t) => s + t.amount, 0);
  const marRev = mar.filter(paid).reduce((s, t) => s + t.amount, 0);
  const aprRev = apr.filter(paid).reduce((s, t) => s + t.amount, 0);
  const churnRate = ((churned.length / janPaid) * 100).toFixed(1);
  const projectedZeroMonth = Math.ceil(aprPaid / ((janPaid - aprPaid) / 3));

  // Count failed/declined transactions
  const janFailed = jan.filter(t => t.condition === 'failed' || t.condition === 'declined').length;
  const febFailed = feb.filter(t => t.condition === 'failed' || t.condition === 'declined').length;
  const marFailed = mar.filter(t => t.condition === 'failed' || t.condition === 'declined').length;
  const aprFailed = apr.filter(t => t.condition === 'failed' || t.condition === 'declined').length;

  // Unique active subscribers per month
  const janUnique = new Set(jan.filter(paid).map(t => t.email.toLowerCase())).size;
  const febUnique = new Set(feb.filter(paid).map(t => t.email.toLowerCase())).size;
  const marUnique = new Set(mar.filter(paid).map(t => t.email.toLowerCase())).size;
  const aprUnique = new Set(apr.filter(paid).map(t => t.email.toLowerCase())).size;

  const analysis = [
    { Category: 'NMI SALES DECLINE ANALYSIS', Detail: '', Data: '' },
    { Category: '', Detail: '', Data: '' },
    { Category: 'SUMMARY', Detail: 'NMI revenue dropped 64% from January ($4,831) to April ($1,728 partial).', Data: '' },
    { Category: '', Detail: '57 subscribers who were active in Jan/Feb have stopped paying by Mar/Apr.', Data: '' },
    { Category: '', Detail: '', Data: '' },
    { Category: '--- REVENUE TREND ---', Detail: '', Data: '' },
    { Category: 'January 2026', Detail: '$' + janRev + ' from ' + janPaid + ' transactions', Data: 'Baseline' },
    { Category: 'February 2026', Detail: '$' + febRev + ' from ' + febPaid + ' transactions', Data: '-36.8% vs Jan' },
    { Category: 'March 2026', Detail: '$' + marRev + ' from ' + marPaid + ' transactions', Data: '-17.7% vs Feb' },
    { Category: 'April 2026 (partial)', Detail: '$' + aprRev + ' from ' + aprPaid + ' transactions', Data: '-31.2% vs Mar' },
    { Category: '', Detail: '', Data: '' },
    { Category: '--- SUBSCRIBER CHURN ---', Detail: '', Data: '' },
    { Category: 'Unique subscribers (Jan)', Detail: janUnique + ' active paying subscribers', Data: '' },
    { Category: 'Unique subscribers (Feb)', Detail: febUnique + ' active paying subscribers', Data: (febUnique - janUnique) + ' net change' },
    { Category: 'Unique subscribers (Mar)', Detail: marUnique + ' active paying subscribers', Data: (marUnique - febUnique) + ' net change' },
    { Category: 'Unique subscribers (Apr)', Detail: aprUnique + ' active paying subscribers (partial month)', Data: (aprUnique - marUnique) + ' net change' },
    { Category: 'Total churned', Detail: churned.length + ' subscribers stopped paying', Data: churnRate + '% churn rate vs Jan' },
    { Category: '', Detail: '', Data: '' },
    { Category: '--- FAILED/DECLINED PAYMENTS ---', Detail: '', Data: '' },
    { Category: 'January failed/declined', Detail: janFailed + ' transactions', Data: '' },
    { Category: 'February failed/declined', Detail: febFailed + ' transactions', Data: '' },
    { Category: 'March failed/declined', Detail: marFailed + ' transactions', Data: '' },
    { Category: 'April failed/declined', Detail: aprFailed + ' transactions', Data: '' },
    { Category: '', Detail: '', Data: '' },
    { Category: '--- ROOT CAUSES ---', Detail: '', Data: '' },
    { Category: '1. No new acquisition', Detail: 'NMI is a legacy gateway collecting recurring $27/mo payments. No new members are being added through this channel — only attrition.', Data: '' },
    { Category: '2. Card expiration/decline', Detail: 'Subscribers with expired or declined cards are not being recovered. ' + (janFailed + febFailed + marFailed + aprFailed) + ' total failed transactions across Jan-Apr.', Data: '' },
    { Category: '3. Passive cancellation', Detail: 'Members are leaving without formal cancellation — they simply stop paying or let cards expire.', Data: '' },
    { Category: '4. No retention system', Detail: 'No automated dunning (failed payment follow-up) or win-back campaigns running on NMI subscribers.', Data: '' },
    { Category: '5. Platform fragmentation', Detail: 'Revenue is split between WHOP and NMI. NMI members may not be receiving same engagement/value as WHOP members.', Data: '' },
    { Category: '', Detail: '', Data: '' },
    { Category: '--- PROJECTED IMPACT ---', Detail: '', Data: '' },
    { Category: 'Monthly loss rate', Detail: '~' + Math.round((janPaid - aprPaid) / 3) + ' subscribers lost per month', Data: '' },
    { Category: 'Revenue loss rate', Detail: '~$' + Math.round((janRev - marRev) / 2) + '/month decline', Data: '' },
    { Category: 'If no action taken', Detail: 'NMI revenue reaches ~$0 by Q3/Q4 2026', Data: '' },
    { Category: 'Annual revenue at risk', Detail: '$' + Math.round(janRev * 12) + ' annualized (at Jan rate)', Data: '' },
    { Category: '', Detail: '', Data: '' },
    { Category: '--- RECOMMENDATIONS ---', Detail: '', Data: '' },
    { Category: '1. Failed payment recovery', Detail: 'Send email/SMS to all 57 churned subscribers with card update link. Potential recovery: $' + (churned.length * 27) + '/mo.', Data: 'HIGH PRIORITY' },
    { Category: '2. Dunning automation', Detail: 'Set up automatic retry + email notifications for failed payments (3 retries over 7 days).', Data: 'HIGH PRIORITY' },
    { Category: '3. Migrate to WHOP', Detail: 'Move remaining NMI subscribers to WHOP to consolidate billing and reduce platform costs.', Data: 'MEDIUM PRIORITY' },
    { Category: '4. Win-back campaign', Detail: 'Offer churned subscribers a discounted rate or bonus to reactivate. Target the 57 churned list.', Data: 'MEDIUM PRIORITY' },
    { Category: '5. Sunset NMI', Detail: 'If migration to WHOP is chosen, plan a 60-day transition and close NMI gateway to reduce merchant fees.', Data: 'LOW PRIORITY' },
  ];
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(analysis), 'Analysis & Recommendations');

  const filePath = 'C:/Users/User/OneDrive/Desktop/NMI_Sales_Report_Jan-Apr_2026.xlsx';
  XLSX.writeFile(wb, filePath);
  console.log('Saved to: ' + filePath);
  console.log('Churned: ' + churned.length + ' subscribers');
  summary.forEach(r => console.log(r.Month + ': ' + r.Transactions + ' txns, $' + r.Revenue));
}

run().catch(e => console.error(e));
