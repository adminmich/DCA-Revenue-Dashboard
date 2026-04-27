const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');

// Attendees currently invited to the "3 Day Masterclass" Apr 27-29 (union of both
// recurring series on dane@deficashflowsystems.com).
const masterclassInvited = new Set([
  'nealbarnett@gmail.com','sloan.taylor0774@gmail.com','birdnesty@gmail.com',
  '1sttim1.5@gmail.com','royalsmarket@mail.com','firstfeatures@mail.com',
  'mcarron2004@yahoo.com','henry.f.owens@gmail.com','lisakazempour.crypto@gmail.com',
  'cwemmons@me.com','hancaskykaren3@gmail.com','justinallman95@outlook.com',
  'auto227933@hushmail.com','lrbirrell@gmail.com','monica@delamora.com',
  'freemancastle@yahoo.com','josephvaladez7@yahoo.com','dmariebarr@gmail.com',
  'dane@deficashflowsystems.com','grant@grantporteous.com','bacesforsuccess@gmail.com',
  'jadefrehner@gmail.com','tarynsilva5683@gmail.com','ab.7477@yahoo.com',
  'cms0924@gmail.com','tbmrep@gmail.com','trystan@trenberth.com','herbdavis@icloud.com',
  'seanr525@gmail.com','nonelectric_eel@yahoo.com','almieb@live.com',
  'azsherrie@gmail.com','gosnworu1@gmail.com','char.miaelliott@gmail.com',
  'corey@deficashflowsystems.com','kamelkhan055@gmail.com','amaboston@yahoo.com',
  'mrjasonemoss@gmail.com','whitecar49@gmail.com','cgtdale67@gmail.com',
  'ruth.spillman@yahoo.com','belong@studio3g.com','dmeyers1970@gmail.com',
  'lisadarrell19@gmail.com','theck1group@gmail.com','cgprestigeinvestor@gmail.com',
  'mthomp99556@gmail.com','stevesrvw@gmail.com',
].map(e => e.toLowerCase()));

// Active Inner Circle members
const wb = XLSX.readFile(path.join(__dirname, 'inner-circle-active.xlsx'));
const members = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);

const seen = new Set();
const missing = [];
for (const m of members) {
  const email = (m.Email || '').trim().toLowerCase();
  if (!email || !/.+@.+\..+/.test(email)) continue;
  if (seen.has(email)) continue;
  seen.add(email);
  if (!masterclassInvited.has(email)) missing.push({ ...m, Email: email });
}

missing.sort((a, b) => (a.Name || a.Email).localeCompare(b.Name || b.Email));

const outWb = XLSX.utils.book_new();
XLSX.utils.book_append_sheet(outWb, XLSX.utils.json_to_sheet(missing), 'Not yet invited');
const outPath = 'C:/Users/User/Downloads/inner-circle-missing-from-masterclass.xlsx';
XLSX.writeFile(outWb, outPath);

const txtPath = 'C:/Users/User/Downloads/inner-circle-missing-guests.txt';
fs.writeFileSync(txtPath, missing.map(r => r.Email).join(', '));

console.log(`Active members with email: ${seen.size}`);
console.log(`Already invited (intersection): ${seen.size - missing.length}`);
console.log(`NOT yet invited: ${missing.length}`);
console.log(`\nWrote ${outPath}`);
console.log(`Wrote ${txtPath}`);
