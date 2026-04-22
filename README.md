# DeFi Cash Flow Systems — Financial Dashboard

A centralized financial dashboard displaying revenue data from **WHOP** and **NMI** payment sources.

## Features

- **KPI Cards** — Total Revenue, WHOP, NMI, MRR, NMI Reserve, At-Risk Revenue
- **Revenue Split** — Doughnut chart showing WHOP vs NMI breakdown
- **Membership Plans** — Horizontal bar chart with 30-day projections per plan tier
- **NMI Daily Volume** — Line chart tracking daily transaction activity
- **MRR Waterfall** — Visual breakdown of recurring revenue, at-risk, and cancellations
- **Key Insights** — Actionable alerts and growth recommendations
- **Plan Breakdown Table** — Detailed membership tier analysis with % of revenue
- **NMI Transaction Log** — Scrollable table of recent NMI payments

## Quick Start

```bash
# Open directly in browser
open index.html

# Or serve locally
npx serve .
```

## Tech Stack

- HTML5 / Tailwind CSS (CDN)
- Chart.js 4.x
- Zero build step — works as a static file

## Deployment

- **GitHub Pages**: Push to repo → Settings → Pages → Deploy from `main`
- **Lovable**: Import the repo URL directly into Lovable
- **Vercel/Netlify**: Connect repo for auto-deploy

## Data Sources

| Source | Description |
|--------|------------|
| WHOP | Membership subscription revenue ($9,082/period) |
| NMI | Payment gateway transactions ($3,024/period) |

---

Built for DeFi Cash Flow Systems team presentations.
