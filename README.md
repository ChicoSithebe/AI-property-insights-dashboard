# AI-property-insights-dashboard
AI-powered property analytics dashboard for lease risk monitoring and rent intelligence (Dash +Python + SQLite + AI Copilot)

## Dashboard Preview

# 🏢 AI-Powered Property & Lease Intelligence Platform  
## Executive Risk, Cost & Portfolio Optimization Dashboard

An enterprise-grade **Property Portfolio Intelligence Platform** built using **Python, Dash, SQLite, and AI**, designed to transform complex lease and rent data into structured, decision-ready intelligence.

This solution centralizes contract, payment, and operational data into a governed data model and provides real-time executive visibility into portfolio risk, financial exposure, and optimization opportunities.

<img width="950" height="398" alt="Property Dashboard 1" src="https://github.com/user-attachments/assets/7b54d25d-e6e1-4dbb-af4d-dd376919245a" />

---

# 🎯 Strategic Objective

Large-scale property portfolios create challenges such as:

- Limited visibility into lease expiry risk  
- Manual tracking of rent escalations  
- Difficulty identifying high-cost or underperforming sites  
- Slow response to contract renewals  
- Fragmented data across spreadsheets  
- Limited executive-level analytics  

This dashboard was designed to address those challenges by:

✅ **Centralizing lease and rent intelligence**  
✅ **Structuring data into a governed model**  
✅ **Automating rent escalation logic**  
✅ **Enabling AI-driven data exploration**  
✅ **Providing leadership-ready KPIs**

---

# 🧭 The Five Strategic Pillars of the Dashboard

---

## 1️⃣ Executive Intelligence Layer

### What It Does
The Executive View provides leadership with immediate insight into:

- Total Contracts / Sites  
- Active vs Offline Sites  
- Expired Leases  
- Contracts Expiring Within 1 Year  
- Average Current Rent  
- Average Rent Increase %  
- High-Rent Site Identification  

### KPI Enablers
This layer enables:

**📉 Risk Reduction KPI**
- % of portfolio expiring within next 12 months  
- Expired lease exposure  

**💰 Cost Exposure KPI**
- Average current rent per site  
- High-rent concentration  

**📊 Operational Efficiency KPI**
- Offline sites still incurring rent  
- Lease concentration by region  

### Executive Impact
Enables leadership to:

- Prioritize renegotiations  
- Allocate legal and property resources  
- Identify high-risk regions  
- Reduce financial exposure before contracts lapse  

---

## 2️⃣ Payments & Rent Escalation Analytics

### What It Does
Using contractual validity dates, the system calculates:

- **Initial Rent** (earliest condition)  
- **Current Rent** (based on valid-from/to logic)  
- **Rent Change** (absolute)  
- **Rent Change %** (escalation trend)  
- **Current Validity Period**  

### Why This Matters
Rent escalation often occurs in structured intervals (5-year step-ups, indexed increases, etc.).

This dashboard:

✅ Automatically determines the current applicable rent  
✅ Handles open-ended conditions (e.g., 9999 dates)  
✅ Quantifies escalation over time  

### KPI Enablers

**📈 Escalation Monitoring KPI**
- Average rent increase %  
- Rent growth by region  

**🔍 Anomaly Detection KPI**
- Sites with abnormal escalation patterns  
- Contracts with high cumulative increases  

### Decision-Making Value
- Supports renegotiation strategies  
- Identifies contracts requiring cost containment  
- Enables forward planning for future rent commitments  
- Provides finance teams with structured exposure analysis  
<img width="953" height="450" alt="Payments_Rent" src="https://github.com/user-attachments/assets/932d3194-d3d5-45b8-bded-558af88d2853" />

---

## 3️⃣ AI Copilot (Natural Language Analytics Engine)

### What It Does
The AI Copilot allows stakeholders to ask business questions in plain English, such as:

- “Which sites expire in the next 2 years?”  
- “Show average rent by region.”  
- “Which high-rent sites are currently offline?”  
- “Trend of yearly rent over time for top regions.”  

The system:
1. Interprets the question  
2. Generates safe SQL  
3. Executes against the SQLite model  
4. Returns both narrative insight and visual output  

### Business Impact
- Removes dependency on technical SQL skills  
- Democratizes access to data insights  
- Reduces reporting turnaround time  
- Enables non-technical executives to explore data independently  

### KPI Enabler
**⏱ Decision Velocity KPI**
- Faster insight generation  
- Reduced analytics bottleneck  
<img width="959" height="413" alt="AI for immediate updates" src="https://github.com/user-attachments/assets/defb7483-e15c-4ce2-ae9f-dc56d47a8bc2" />

---

## 4️⃣ SQL & Data Model Architecture

### Raw Data Preservation
All source data is preserved in:

- `raw_contracts`  
- `raw_conditions`  

Ensuring:
- Auditability  
- Data lineage  
- Traceability  

### Modeled Layer (Star Schema Approach)

- `dim_contract`  
  - 1 row per unique contract/site  

- `fact_condition`  
  - All payment condition lines (many per contract)  

- `fact_contract_metrics`  
  - Calculated metrics (initial vs current rent)  

- `vw_contracts_merged`  
  - Business-ready analytical view  

### Governance & Controls
- Surrogate keys  
- FK relationships  
- Derived flags (Expired Lease, High Rent)  
- Controlled metric logic  

### Strategic Benefit
This architecture ensures:

✅ Data consistency  
✅ Calculation transparency  
✅ Reduced manual spreadsheet risk  
✅ Scalable expansion (forecasting, scenario modeling)  

<img width="959" height="411" alt="SQL database for reporting and managing Data" src="https://github.com/user-attachments/assets/84af968d-57fd-4725-bb8e-744567523ce8" />

<img width="955" height="305" alt="Data Model" src="https://github.com/user-attachments/assets/783591b5-bb6a-45d1-a443-6d605458953c" />

---

## 5️⃣ Metadata & Governance Layer

The platform includes a built-in:

### Metadata Schema (Data Dictionary)
Each field is defined by:
- Table name  
- Column name  
- Data type  
- Role (PK, FK, Measure, Derived)  
- Business description  

### Why This Is Critical
- Enhances trust in data  
- Reduces interpretation errors  
- Improves audit readiness  
- Supports compliance requirements  

---

# 📈 Portfolio-Level Achievements & Business Enablement

This dashboard enables measurable improvements in:

## 🔐 Risk Management
- Early identification of expiring leases  
- Reduced legal exposure  
- Structured renewal planning  

## 💵 Cost Optimization
- Identification of high-rent sites  
- Escalation visibility  
- Improved renegotiation leverage  

## 🏗 Operational Efficiency
- Visibility into offline but rent-bearing sites  
- Region-level performance comparisons  

## 📊 Executive Decision Support
- Single source of truth  
- Real-time KPI visibility  
- Data-driven resource allocation  

---

# 🔎 Example Strategic Use Cases

- Executive Committee Review  
- Property & Legal Risk Meetings  
- Budget Forecast Planning  
- Lease Renegotiation Strategy  
- Portfolio Optimization Programs  
- Capital Allocation Prioritization  

---

# 🧠 Advanced Capabilities (Future Expansion)

The current architecture allows for expansion into:

- Rent forecasting & scenario simulation  
- Monte Carlo risk modeling  
- Portfolio optimization algorithms  
- Automated alerts for upcoming expiries  
- Integration with ERP systems  
- Regional profitability overlays  

---

# 🤖 AI Copilot: Example Questions (Try These)

Here are two practical examples you can ask in the AI Copilot:

1) **“Show the top 10 highest current rent sites and indicate which are offline.”**  
   - Output: table + bar chart of current rent, split by site status.

2) **“Plot average current rent by region over time (based on condition valid-from).”**  
   - Output: line chart by region + summary insights.

---

# 🏁 Conclusion

This platform is more than a dashboard.

It is a:

✅ Lease Risk Management Engine  
✅ Cost Optimization Tool  
✅ Executive Decision Support System  
✅ AI-Driven Analytics Platform  
✅ Governed Data Architecture  

It transforms raw contract data into strategic intelligence that supports proactive management, financial optimization, and risk mitigation.

---

# 📁 Repository Structure

ai-property-insights-dashboard/
├── app/
│ ├── dashboard.py
│ └── requirements.txt
├── data/
│ └── (sample datasets only)
├── docs/
│ ├── data_model.md
│ └── screenshots/
├── .gitignore
├── LICENSE
└── README.md
