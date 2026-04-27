const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');

const wb = XLSX.readFile(path.join(__dirname, 'inner-circle-active.xlsx'));
const rows = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);

const emails = [...new Set(
  rows.map(r => (r.Email || '').trim().toLowerCase()).filter(e => /.+@.+\..+/.test(e))
)].sort();

const csvPath = path.join('C:/Users/User/Downloads', 'inner-circle-emails.csv');
const txtPath = path.join('C:/Users/User/Downloads', 'inner-circle-guests.txt');

fs.writeFileSync(csvPath, 'Email\n' + emails.join('\n') + '\n');
fs.writeFileSync(txtPath, emails.join(', '));

console.log(`Emails: ${emails.length} unique (from ${rows.length} member rows)`);
console.log(`Wrote ${csvPath}`);
console.log(`Wrote ${txtPath}`);
