# Automated Cost Optimization Tool for Azure – Project Roadmap

## Phase 1: Planning & Setup

### 1. Define Project Scope & Requirements [Done]
- List the Azure resources to monitor (VMs, Disks, IPs, App Services, etc.).
- Decide actions: reporting only, or also automated cleanup.
- Choose notification/reporting channels (email, dashboard, Teams/Slack, etc.).

### 2. Set Up Azure Environment [Done]
- Create an Azure Free Trial account (if not already done).
- Register an Azure AD Application (Service Principal) for API access.
- Assign the Service Principal the necessary permissions (Reader/Contributor).

---

## Phase 2: Resource Inventory & Cost Analysis

### 3. Set Up Development Environment [Done]
- Install Python and required Azure SDKs:
  - `azure-identity`
  - `azure-mgmt-resource`
  - `azure-mgmt-compute`
  - `azure-mgmt-costmanagement`
  - `azure-mgmt-network`
  - `azure-mgmt-web`
  - `azure-mgmt-monitor`
- Initialize a code repository (GitHub or Azure Repos).

### 4. Resource Inventory Module [Done]
- Use the Azure SDK to list all resources in the subscription.
- Store resource metadata:
  - Name, type, location, tags, status.

### 5. Cost Data Collection Module [Done]
- Integrate with the Azure Cost Management API.
- Retrieve cost data per resource or resource group (Currently: Monthly Total + Grouped by Resource Type).
- Store cost data for later analysis (CSV, database, or in-memory).

---

## Phase 3: Optimization Logic

### 6. Unused / Underutilized Resource Detection [Partially Done]
- Identify stopped or deallocated VMs. [Done]
- Find unattached disks and unused public IPs. [Done]
- Identify Empty Resource Groups. [Done]
- Identify Empty App Service Plans. [Done]
- Identify Old Disk Snapshots (based on age threshold). [Done]
- Identify Low CPU VMs (reporting Available Memory context). [Done]
- Detect low‑usage App Services or databases (requires metric queries). [Done - Implemented via functions for ASPs, DBs (DTU/vCore), Apps, Gateways]
- **[Remaining]:** (Optional) Leverage Azure Resource Graph for advanced queries.
- **[New]:** Add support for multiple Azure subscriptions
- **[New]:** Implement cost prediction based on historical data
- **[New]:** Add alerting system for unexpected cost spikes

### 7. Cost Optimization Suggestions [Partially Done -> Done]
- Match detected unused/underutilized resources with potential *specific* savings (e.g., using Retail Prices API). [Done - Implemented for Disks, IPs, ASPs, Snapshots, VMs, SQL DBs, App Gateways]
- **[Remaining]:** Refine price filtering logic (SKU/Meter mapping) for accuracy.
- Extend pricing lookup to other resources (IPs, Snapshots, ASPs). [Done]
- Generate actionable recommendations, for example: [Done]
  > "Delete disk `X` to save `$Y` per month.”
- *Currently implemented: Recommendations include estimated current monthly cost for most resource types (Disks, IPs, ASPs, Snapshots, VMs, SQL DBs, Gateways) as potential savings if deleted/scaled down.*

---

## Phase 4: Automation & Reporting

### 8. Automated Cleanup Module (Optional) [Partially Done]
- Implement safe deletion or deallocation of unused resources (Disks, Stopped VMs (deallocate), IPs, Empty RGs, Empty Plans, Snapshots). [Done]
- Provide a **"dry run"** mode (running without `--cleanup` flag). [Done]
- Implement interactive user confirmation before actions. [Done]
- Log all actions for auditability. [Done - Enhanced logging added]
- Improve handling of long-running operations (LROs) like RG deletion (e.g., waiting/status checks). [Done - Handled via poller.result() and enhanced logging]
- Consider non-interactive cleanup mode (with strong warnings). [Done - Implemented via --force-cleanup flag]

### 9. Reporting & Notification Module [Partially Done]
- Generate reports in CSV format. [Done]
- Generate console summary report. [Done]
- Generate reports in HTML, or PDF format. [HTML Done]
- Send reports via email (SMTP, SendGrid) or Azure Logic Apps. [Basic SMTP Done]
- **[Remaining]:** (Optional) Integrate with Teams or Slack for real‑time alerts
- **[New]:** Implement scheduled optimization reports
- **[New]:** Add cost trend analysis and forecasting

---

## Phase 5: Scheduling & Deployment

### 10. Automation & Scheduling [Partially Done]
- **[Remaining]:** Containerize the tool with Docker for portability. [Initial Dockerfile created]
- **[Remaining]:** Test Docker image and container execution.
- **[Remaining]:** Choose deployment method (Azure Functions vs. ACI).
- **[Remaining]:** Deploy as:
  - **Azure Function** (with a Timer Trigger)  
  - or **Azure Container Instance** (with a scheduled job)
- **[Remaining]:** Schedule periodic runs (e.g., weekly) to keep costs optimized.
- **[New]:** Add CI/CD pipeline (GitHub Actions)
  - Automated testing
  - Docker image building
  - Dependency updates

---

## Phase 6: Visualization & Dashboard (Optional)

### 11. Dashboard Integration [Partially Done]
- Push data to Power BI or Grafana for interactive dashboards. [Done - Data exported to local CSV files for Grafana ingestion; Grafana configuration/dashboarding is external]
- **[New]:** Build interactive web dashboard using Streamlit:
  - Cost trends visualization
  - Resource usage patterns
  - Optimization history and savings tracking
  - Anomaly detection and alerts
  - Recommendation history
- **[Remaining]:** Visualize:
  - Cost trends
  - Resource usage
  - Optimization history

---

## Phase 7: Testing, Documentation, and Handover

### 12. Testing [Not Started]
- **[Remaining]:** Test against a variety of resource types and usage scenarios.
- **[Remaining]:** Ensure only intended resources are flagged or deleted.
- **[New]:** Add unit tests for optimization logic
- **[New]:** Implement integration tests for Azure API calls
- **[New]:** Create mock tests for cost scenarios

### 13. Documentation [Not Started]
- **[Remaining]:** Provide setup, configuration, and usage instructions.
- **[Remaining]:** Include troubleshooting tips and FAQs.
- **[New]:** Expand README with:
  - Setup instructions
  - Usage examples
  - Configuration options
- **[New]:** Add API documentation (Sphinx)

### 14. Handover & Presentation [Not Started]
- **[Remaining]:** Prepare a live demo or slide deck.
- **[Remaining]:** Share the repository and documentation with stakeholders.

---

## Sample Timeline

| Week | Milestone                                      |
|------|------------------------------------------------|
| 1    | Planning, Azure setup, Service Principal       |
| 2    | Resource inventory & cost data modules         |
| 3    | Optimization logic & reporting                  |
| 4    | Automation, scheduling, and notifications       |
| 5    | Dashboard (optional), testing, documentation    |

---

## Recommended Tools & Services

- **Languages:**  
  - Python (preferred)  
  - PowerShell (alternative)
- **Azure SDKs:**  
  - `azure-identity`  
  - `azure-mgmt-resource`  
  - `azure-mgmt-compute`  
  - `azure-mgmt-costmanagement`
  - `azure-mgmt-network`
  - `azure-mgmt-web`
  - `azure-mgmt-monitor`
- **Automation & Scheduling:**  
  - Azure Functions  
  - Azure Logic Apps  
  - Azure Container Instances
- **Reporting & Visualization:**  
  - Email (SMTP, SendGrid)  
  - Power BI, Grafana, or custom web dashboard
- **Version Control:**  
  - GitHub or Azure Repos

---

## Architecture Diagram

```text
+-------------------+         +-------------------+         +-------------------+
| Azure Resources   | <-----> | Cost Optimization | <-----> | Notification &    |
| (VMs, Disks, etc) |  APIs   | Tool (Python App) | Reports | Visualization     |
+-------------------+         +-------------------+         +-------------------+
         ^                           |                               |
         |                           v                               v
         +-------------------+   Automated Actions           Power BI/Grafana
         | Azure Functions   |   (Cleanup/Deallocate)        (Optional)
         +-------------------+
```

---

## Next Steps

- Choose a hosting/scheduling method (Azure Function vs. Container).
- Implement Phase 2 modules (inventory & cost data).
- Build and test your first detection rule.
- Iterate on reporting and automation until you reach your cost‑optimization goals.

> _Ready to get started? Clone your repo, spin up your dev environment, and let's optimize!_ 