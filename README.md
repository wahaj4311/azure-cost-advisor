# Azure Cost Advisor

This tool analyzes an Azure subscription to identify potential cost savings opportunities by detecting unused or underutilized resources. It generates reports summarizing findings and potential savings, and can optionally perform cleanup actions.

## Features

*   **Identifies:**
    *   Unattached Managed Disks
    *   Stopped (but not deallocated) VMs
    *   Unused Public IP Addresses
    *   Empty Resource Groups
    *   Empty App Service Plans
    *   Old Disk Snapshots (based on age)
    *   Low CPU VMs
    *   Low CPU App Service Plans
    *   Low DTU/CPU SQL Databases
    *   Idle Application Gateways
    *   Low CPU Web Apps
    *   Orphaned Network Security Groups (NSGs)
    *   Orphaned Route Tables
*   **Estimates Potential Savings:** Uses the Azure Retail Prices API to estimate monthly savings for deletable resources (disks, IPs, empty ASPs, snapshots) and the current cost of underutilized resources (VMs, ASPs, DBs, Gateways).
*   **Reporting:**
    *   Generates detailed HTML report (`.html`)
    *   Generates summary CSV report (`.csv`)
    *   Prints console summary
    *   Exports detailed findings to individual CSV files (in `grafana_export/`) for ingestion into tools like Grafana.
*   **Cleanup (Optional):**
    *   Interactively prompts for cleanup (deallocation/deletion) of identified resources (`--cleanup`).
    *   Supports non-interactive forced cleanup (`--force-cleanup` - **Use with extreme caution!**).
*   **Configuration:** Allows ignoring specific resources via an `ignored_resources.txt` file.

## Setup

This project uses `uv` for environment and package management.

1.  **Install `uv`:** Follow the instructions at [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv).
2.  **Clone the repository:**
    ```bash
    git clone https://github.com/wahaj4311/azure-cost-advisor.git
    cd azure-cost-advisor
    ```
3.  **Set up the virtual environment and install dependencies:**
    ```bash
    uv venv
    uv pip sync requirements.txt
    ```
4.  **Azure Authentication:**
    *   Ensure you are logged in via Azure CLI (`az login`). The script uses `DefaultAzureCredential`, which will pick up your CLI credentials.
    *   Alternatively, configure a Service Principal and set the standard environment variables (`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`).
    *   The identity used needs appropriate permissions (at least `Reader` on the subscription, `Contributor` is required for cleanup actions).
5.  **Subscription ID:**
    *   The script attempts to detect the default subscription from Azure CLI.
    *   You can explicitly set it via the `AZURE_SUBSCRIPTION_ID` environment variable.

## Usage

Run the script using `uv run`:

```bash
uv run python cost_optimizer.py [OPTIONS]
```

**Common Options:**

*   `--html-report FILENAME`: Specify the output HTML report file (default: `azure_cost_optimization_report.html`).
*   `--csv-report FILENAME`: Specify the output CSV summary file (default: `azure_cost_optimization_report.csv`).
*   `--ignore-file FILENAME`: Specify a file containing resource IDs to ignore (one ID per line, comments start with #) (default: `ignored_resources.txt`).
*   `--cleanup`: Enable interactive prompts for cleanup actions.
*   `--force-cleanup`: Enable non-interactive cleanup (DANGEROUS!).
*   `--debug`: Enable debug logging.

**Example:**

```bash
# Run analysis and generate reports
uv run python cost_optimizer.py --html-report my_report.html

# Run analysis with interactive cleanup
uv run python cost_optimizer.py --cleanup
```

## Output

*   **`azure_cost_optimization_report.html` (or custom name):** Detailed HTML report with findings.
*   **`azure_cost_optimization_report.csv` (or custom name):** Summary CSV report.
*   **`cleanup_log.txt`:** Log file containing script execution details and any cleanup actions performed.
*   **`grafana_export/` directory:** Contains individual CSV files for each finding type (e.g., `low_cpu_vms.csv`). These can be used as a data source in Grafana (using the CSV plugin pointed to this directory's absolute path).

## Ignoring Resources

Create a file named `ignored_resources.txt` (or specify a different name using `--ignore-file`) in the project root. Add the full Azure Resource ID of any resource you want the script to ignore during analysis and potential cleanup, one ID per line. Lines starting with `#` are treated as comments.

**Example `ignored_resources.txt`:**

```
# Ignore this specific critical VM
/subscriptions/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/resourceGroups/my-critical-rg/providers/Microsoft.Compute/virtualMachines/dont-touch-this-vm
# Ignore this old disk snapshot we need to keep
/subscriptions/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/resourceGroups/my-archive-rg/providers/Microsoft.Compute/snapshots/archive-snapshot-2023
``` 