import os
import argparse
import logging
import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
import sys

# Import from the new module structure
from azure_cost_advisor import (
    clients, 
    config, 
    utils, 
    analysis, 
    pricing, 
    reporting, 
    actions
)

# Initialize Rich Console (can be passed to module functions)
console = Console()

# Global list to store ignored resource IDs
ignored_resource_ids = set()

# Function to load ignored resources from file
def load_ignored_resources(filename="ignored_resources.txt"):
    global ignored_resource_ids
    ignored_resource_ids.clear() # Clear before loading
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        ignored_resource_ids.add(line)
            logger.info(f"Loaded {len(ignored_resource_ids)} ignored resource IDs from {filename}")
        else:
             logger.info(f"Ignore file '{filename}' not found. No resources will be ignored by default.")
    except Exception as e:
        logger.error(f"Error loading ignored resources from {filename}: {e}")
        console.print(f"[red]Error loading ignored resources file:[/red] {e}")

# Function to filter findings based on ignored list
def filter_ignored(findings_list, finding_key='id'):
    global ignored_resource_ids
    if not findings_list or not ignored_resource_ids:
        return findings_list, []
    
    filtered_list = []
    ignored_items = []
    for item in findings_list:
        resource_id = item.get(finding_key)
        if resource_id and resource_id in ignored_resource_ids:
             ignored_items.append(item)
             logger.info(f"Ignoring resource based on list: {resource_id} ({item.get('Name', 'Unknown')})")
        else:
            filtered_list.append(item)
            
    return filtered_list, ignored_items

# Function to create DataFrame and filter based on ignore list
def process_findings_to_df(findings_list, finding_type, columns=None):
    """Converts a list of findings (dict) to a DataFrame and filters ignored resources."""
    if not findings_list:
        return pd.DataFrame(columns=columns if columns else []), pd.DataFrame(columns=columns if columns else [])

    filtered_list, ignored_list = filter_ignored(findings_list, finding_key='id')
    
    df_filtered = pd.DataFrame(filtered_list)
    df_ignored = pd.DataFrame(ignored_list)
    
    # Select and order columns if specified
    if columns:
        # Ensure columns exist, add if missing (with NaN)
        for df in [df_filtered, df_ignored]:
             if not df.empty:
                 for col in columns:
                     if col not in df.columns:
                         df[col] = pd.NA 
                 # Reindex might drop non-existent columns, use explicit selection
                 # df = df.reindex(columns=columns)
                 df = df[columns] 
        else:
                 # Ensure empty DFs have the right columns
                 df = pd.DataFrame(columns=columns)

    return df_filtered, df_ignored


def main():
    parser = argparse.ArgumentParser(description="Analyze Azure resources for cost optimization opportunities.")
    parser.add_argument("--cleanup", action="store_true", help="Enable interactive cleanup prompts for identified resources.")
    parser.add_argument("--force-cleanup", action="store_true", help="Force cleanup actions without interactive confirmation (USE WITH CAUTION!).")
    parser.add_argument("--wait-for-cleanup", action="store_true", help="Wait for each cleanup operation (delete/deallocate) to complete.")
    parser.add_argument("--send-email", action="store_true", help="Send a summary report email (requires EMAIL_* and SMTP_* env vars).")
    parser.add_argument("--html-report", default="azure_cost_optimization_report.html", help="Filename for the HTML report.")
    parser.add_argument("--csv-report", default="azure_cost_optimization_report.csv", help="Filename for the CSV summary report.")
    parser.add_argument("--ignore-file", default="ignored_resources.txt", help="File containing resource IDs to ignore (one per line).")
    parser.add_argument("--include-ignored-in-report", action="store_true", help="Include ignored resources in a separate section in the HTML report.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    args = parser.parse_args()

    # --- Setup Logging --- (Using the function from utils module)
    log_level = logging.DEBUG if args.debug else logging.WARNING # Default to WARNING
    global logger # Make logger accessible globally if needed elsewhere
    logger = utils.setup_logger(level=log_level, filename=config.LOG_FILENAME) # Pass level and filename
    logger.info("--- Script Execution Started ---") # This INFO message will still go to the file
    logger.info(f"Arguments: {args}") # This INFO message will still go to the file

    # --- Load Ignored Resources ---
    load_ignored_resources(args.ignore_file)

    # --- Authentication --- (Using the function from clients module)
    credential, subscription_id = clients.get_azure_credentials(console=console)
    if not credential or not subscription_id:
        console.print("[bold red]Failed to authenticate or determine subscription. Exiting.[/]")
        sys.exit(1)

    # --- Initialize Azure Clients (pass credential and subscription_id) ---
    # These can be initialized within the functions that need them or passed around.
    # For simplicity here, let's assume functions in analysis/reporting/actions initialize as needed
    # or accept them as parameters.
    
    # --- Resource Analysis --- (Call functions from analysis module)
    # Wrap analysis steps in progress display
    all_findings_raw = {}
    all_ignored_raw = {}
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True, # Remove progress display when done
        console=console
    ) as progress:
        task_analyze = progress.add_task("[cyan]Analyzing Azure resources...[/]", total=None) # Indeterminate

        # Cost Data (Using analysis module now)
        costs_by_type, total_cost, currency = analysis.get_cost_data(credential, subscription_id, console=console)

        # --- Identify Potential Optimizations --- 
        # Pass credential, subscription_id, and console to each function
        all_findings_raw['unattached_disks'] = analysis.find_unattached_disks(credential, subscription_id, console=console)
        all_findings_raw['stopped_vms'] = analysis.find_stopped_vms(credential, subscription_id, console=console)
        all_findings_raw['unused_public_ips'] = analysis.find_unused_public_ips(credential, subscription_id, console=console)
        all_findings_raw['empty_rgs'] = analysis.find_empty_resource_groups(credential, subscription_id, console=console)
        all_findings_raw['empty_asps'] = analysis.find_empty_app_service_plans(credential, subscription_id, console=console)
        all_findings_raw['old_snapshots'] = analysis.find_old_snapshots(credential, subscription_id, age_threshold_days=config.SNAPSHOT_AGE_THRESHOLD_DAYS, console=console)
        all_findings_raw['low_cpu_vms'] = analysis.find_underutilized_vms(credential, subscription_id, cpu_threshold_percent=config.LOW_CPU_THRESHOLD_PERCENT, lookback_days=config.METRIC_LOOKBACK_DAYS, console=console)
        all_findings_raw['low_cpu_asps'] = analysis.find_low_usage_app_service_plans(credential, subscription_id, cpu_threshold_percent=config.APP_SERVICE_PLAN_LOW_CPU_THRESHOLD_PERCENT, lookback_days=config.METRIC_LOOKBACK_DAYS, console=console)
        all_findings_raw['low_dtu_dbs'] = analysis.find_low_dtu_sql_databases(credential, subscription_id, dtu_threshold_percent=config.SQL_DB_LOW_DTU_THRESHOLD_PERCENT, lookback_days=config.METRIC_LOOKBACK_DAYS, console=console)
        all_findings_raw['low_cpu_vcore_dbs'] = analysis.find_low_cpu_sql_vcore_databases(credential, subscription_id, cpu_threshold_percent=config.SQL_VCORE_LOW_CPU_THRESHOLD_PERCENT, lookback_days=config.METRIC_LOOKBACK_DAYS, console=console)
        
        # Ensure low_cpu_vcore_dbs is always a list of dictionaries
        if all_findings_raw['low_cpu_vcore_dbs'] is None:
            all_findings_raw['low_cpu_vcore_dbs'] = []
        elif isinstance(all_findings_raw['low_cpu_vcore_dbs'], list):
            # Check each item in the list to ensure they're dictionaries
            clean_results = []
            for item in all_findings_raw['low_cpu_vcore_dbs']:
                if isinstance(item, dict):
                    clean_results.append(item)
                elif isinstance(item, str):
                    # If it's a string, log it and skip
                    logger.error(f"Unexpected string in low_cpu_vcore_dbs results: {item}")
                else:
                    logger.error(f"Unexpected type in low_cpu_vcore_dbs results: {type(item)}")
            all_findings_raw['low_cpu_vcore_dbs'] = clean_results
        elif isinstance(all_findings_raw['low_cpu_vcore_dbs'], str):
            logger.error(f"low_cpu_vcore_dbs returned a string instead of a list: {all_findings_raw['low_cpu_vcore_dbs']}")
            all_findings_raw['low_cpu_vcore_dbs'] = []
        elif not isinstance(all_findings_raw['low_cpu_vcore_dbs'], list):
            logger.error(f"low_cpu_vcore_dbs returned unexpected type {type(all_findings_raw['low_cpu_vcore_dbs'])}: {all_findings_raw['low_cpu_vcore_dbs']}")
            all_findings_raw['low_cpu_vcore_dbs'] = []
            
        all_findings_raw['idle_gateways'] = analysis.find_idle_application_gateways(credential, subscription_id, lookback_days=config.METRIC_LOOKBACK_DAYS, idle_connection_threshold=config.IDLE_CONNECTION_THRESHOLD_GATEWAY, console=console)
        all_findings_raw['low_cpu_apps'] = analysis.find_low_usage_web_apps(credential, subscription_id, cpu_threshold_percent=config.LOW_CPU_THRESHOLD_WEB_APP, lookback_days=config.METRIC_LOOKBACK_DAYS, console=console)
        all_findings_raw['orphaned_nsgs'] = analysis.find_orphaned_nsgs(credential, subscription_id, console=console)
        all_findings_raw['orphaned_rts'] = analysis.find_orphaned_route_tables(credential, subscription_id, console=console)

        progress.update(task_analyze, completed=1, total=1) # Mark as complete

    console.print("\n[bold green]:mag: Analysis complete.[/]")

    # --- Process Findings into DataFrames & Filter Ignored ---
    findings_dfs = {}
    ignored_dfs = {}
    potential_savings = {}
    total_potential_savings = 0.0

    # Define columns for each DataFrame for consistency
    columns_map = {
        'unattached_disks': ['Name', 'Resource Group', 'Location', 'Size (GB)', 'SKU', 'Potential Monthly Savings', 'ID'],
        'stopped_vms': ['Name', 'Resource Group', 'Location', 'Disk Details', 'Potential Monthly Savings', 'Recommendation', 'ID'], # Added Disk Details & Savings
        'unused_public_ips': ['Name', 'Resource Group', 'Location', 'IP Address', 'SKU', 'Potential Monthly Savings', 'ID'],
        'empty_rgs': ['Name', 'Location', 'Recommendation', 'ID'],
        'empty_asps': ['Name', 'Resource Group', 'Location', 'SKU', 'Tier', 'Potential Monthly Savings', 'ID'],
        'old_snapshots': ['Name', 'Resource Group', 'Location', 'Size (GB)', 'SKU', 'Created Date', 'Potential Monthly Savings', 'ID'],
        'low_cpu_vms': ['Name', 'Resource Group', 'Location', 'OS Type', 'VM Size', 'Avg CPU %', 'Potential Monthly Savings', 'Recommendation', 'ID'], # Added OS Type
        'low_cpu_asps': ['Name', 'Resource Group', 'Location', 'SKU', 'Tier', 'Avg CPU %', 'Potential Monthly Savings', 'Recommendation', 'ID'], # Added Savings
        'low_dtu_dbs': ['Name', 'Resource Group', 'Location', 'SKU', 'Tier', 'Avg DTU %', 'Potential Monthly Savings', 'Recommendation', 'ID'], # Added Savings
        'low_cpu_vcore_dbs': ['Name', 'Resource Group', 'Location', 'SKU', 'Tier', 'Avg CPU %', 'Potential Monthly Savings', 'Recommendation', 'ID'], # Added Savings
        'idle_gateways': ['Name', 'Resource Group', 'Location', 'SKU', 'Tier', 'Avg Connections', 'Potential Monthly Savings', 'Recommendation', 'ID'], # Added Savings
        'low_cpu_apps': ['Name', 'Resource Group', 'Location', 'Plan Name', 'Plan Tier', 'Avg CPU %', 'Potential Monthly Savings', 'Recommendation', 'ID'], # Added Savings (likely 0)
        'orphaned_nsgs': ['Name', 'Resource Group', 'Location', 'Recommendation', 'ID'],
        'orphaned_rts': ['Name', 'Resource Group', 'Location', 'Recommendation', 'ID']
    }

    # --- Calculate Potential Savings ---
    console.print("\n[bold blue]--- Calculating Potential Savings ---[/]")
    # Use Progress for better feedback during potentially slow API calls
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console,
        transient=True
    ) as progress:
        # Flatten the list of all findings for easier progress tracking
        all_raw_items = []
        for key, items_list in all_findings_raw.items():
            if items_list:
                all_raw_items.extend([(key, item) for item in items_list])

        task_savings = progress.add_task("[cyan]Estimating savings...", total=len(all_raw_items))

        # Process each finding type and item individually
        processed_findings = {key: [] for key in all_findings_raw.keys()} # Store processed items

        for key, item in all_raw_items:
            # *** Add type check at the very beginning of the loop ***
            if not isinstance(item, dict):
                logger.error(f"Skipping item processing for key '{key}': Expected a dictionary but got {type(item)}. Item value: {item}")
                continue # Skip this iteration entirely

            item_cost = 0.0
            recommendation = "Review usage and necessity."
            try:
                # --- Existing Cost Estimations ---
                if key == 'unattached_disks':
                    # .get() is safe here because we checked isinstance(item, dict) above
                    item_cost = pricing.estimate_disk_cost(item.get('sku'), item.get('size_gb'), item.get('location'), console=console, logger=logger)
                    recommendation = f"Delete if unused to potentially save ~{currency} {item_cost:.2f}/month."
                elif key == 'unused_public_ips':
                    item_cost = pricing.estimate_public_ip_cost(item.get('sku'), item.get('location'), console=console, logger=logger)
                    recommendation = f"Delete if unused to potentially save ~{currency} {item_cost:.2f}/month."
                elif key == 'empty_asps':
                    item_cost = pricing.estimate_app_service_plan_cost(item.get('tier'), item.get('sku'), item.get('location'), console=console, logger=logger)
                    recommendation = f"Delete if unused to potentially save ~{currency} {item_cost:.2f}/month."
                elif key == 'old_snapshots':
                    item_cost = pricing.estimate_snapshot_cost(item.get('size_gb'), item.get('location'), item.get('sku'), console=console, logger=logger)
                    recommendation = f"Delete if unused to potentially save ~{currency} {item_cost:.2f}/month."

                # --- Updated/New Cost Estimations ---
                elif key == 'stopped_vms':
                    # Assumes 'disks': [{'name': 'disk1', 'size_gb': 128, 'sku': 'Premium_LRS', 'location': 'eastus'}, ...] is added to 'item' by analysis.find_stopped_vms
                    disk_costs = []
                    total_disk_cost = 0.0
                    disks_info = item.get('disks', [])
                    if disks_info:
                        for disk in disks_info:
                            disk_cost = pricing.estimate_disk_cost(disk.get('sku'), disk.get('size_gb'), disk.get('location', item.get('location')), console=console, logger=logger)
                            disk_costs.append(f"{disk.get('name')} ({disk.get('sku')}, {disk.get('size_gb')}GB): ~{currency} {disk_cost:.2f}/month")
                            total_disk_cost += disk_cost
                        item['Disk Details'] = "; ".join(disk_costs)
                        item_cost = total_disk_cost
                        recommendation = f"VM is stopped, but disks still incur costs (~{currency} {item_cost:.2f}/month). Deallocate VM (if not done) or delete VM and disks if no longer needed."
                    else:
                         item['Disk Details'] = "Disk info not available."
                         recommendation = "VM is stopped. Deallocate to stop compute charges or delete if no longer needed (check disk costs separately)."
                         item_cost = 0.0 # Cannot estimate disk cost without info

                elif key == 'low_cpu_vms':
                    # Estimate CURRENT cost of the VM as potential saving if deleted
                    vm_size = item.get('size') # Original key from analysis
                    location = item.get('location')
                    # Assume Linux if OS not provided by analysis function (needs update there too ideally)
                    os_type = item.get('os_type', 'Linux')
                    if vm_size and location:
                        # Placeholder for the new pricing function call
                        item_cost = pricing.estimate_vm_cost(vm_size, location, os_type=os_type, console=console, logger=logger)
                        recommendation = f"Low CPU usage detected (Avg: {item.get('avg_cpu_percent', 'N/A'):.1f}%). Consider resizing to a smaller instance type. Current estimated compute cost is ~{currency} {item_cost:.2f}/month (potential saving if deleted)."
                    else:
                        recommendation = f"Low CPU usage detected (Avg: {item.get('avg_cpu_percent', 'N/A'):.1f}%). Consider resizing to a smaller instance type. (Could not estimate current cost)."
                        item_cost = 0.0

                elif key == 'low_cpu_asps':
                     # Estimate CURRENT cost of the ASP
                     tier = item.get('tier')
                     sku_name = item.get('sku')
                     location = item.get('location')
                     if tier and sku_name and location:
                         item_cost = pricing.estimate_app_service_plan_cost(tier, sku_name, location, console=console, logger=logger)
                         recommendation = f"Low CPU usage detected (Avg: {item.get('avg_cpu_percent', 'N/A'):.1f}%). Consider scaling down the plan or consolidating apps. Current estimated plan cost is ~{currency} {item_cost:.2f}/month."
                     else:
                         recommendation = f"Low CPU usage detected (Avg: {item.get('avg_cpu_percent', 'N/A'):.1f}%). Consider scaling down the plan or consolidating apps. (Could not estimate current cost)."
                         item_cost = 0.0

                elif key == 'low_cpu_apps':
                    # Cost is tied to the plan, already estimated in 'low_cpu_asps'
                    item_cost = 0.0 # No direct cost saving from the app itself
                    recommendation = f"Low CPU usage detected (Avg: {item.get('avg_cpu_percent', 'N/A'):.1f}%). Saving potential is linked to scaling down the App Service Plan '{item.get('plan_name', 'Unknown')}'."

                elif key == 'low_dtu_dbs' or key == 'low_cpu_vcore_dbs':
                    # Estimate CURRENT cost of the DB
                    tier = item.get('tier')
                    sku_name = item.get('sku')
                    family = item.get('family') # e.g., 'Basic', 'Standard', 'GP', 'BC'
                    capacity = item.get('capacity') # e.g., DTUs or vCores
                    location = item.get('location')
                    avg_metric = item.get('avg_dtu_percent') if key == 'low_dtu_dbs' else item.get('avg_cpu_percent')
                    metric_name = "DTU" if key == 'low_dtu_dbs' else "CPU"

                    if location: # Tier/SKU might be complex, focus on getting *some* estimate
                        item_cost = pricing.estimate_sql_database_cost(tier, sku_name, family, capacity, location, console=console, logger=logger)
                        recommendation = f"Low {metric_name} usage detected (Avg: {avg_metric:.1f}%). Consider scaling down the database tier/size. Current estimated cost is ~{currency} {item_cost:.2f}/month."
                    else:
                         recommendation = f"Low {metric_name} usage detected (Avg: {avg_metric:.1f}%). Consider scaling down the database tier/size. (Could not estimate current cost)."
                         item_cost = 0.0

                elif key == 'idle_gateways':
                    # Estimate CURRENT cost of the Gateway
                    tier = item.get('tier') # e.g., 'Standard', 'WAF'
                    sku_name = item.get('sku') # e.g., 'Standard_Small', 'WAF_Medium'
                    location = item.get('location')
                    if tier and sku_name and location:
                         # Placeholder for the new pricing function call
                         item_cost = pricing.estimate_app_gateway_cost(tier, sku_name, location, console=console, logger=logger)
                         recommendation = f"Gateway appears idle (Avg Connections: {item.get('avg_current_connections', 'N/A'):.1f}). Consider resizing, pausing (if applicable), or deleting if unused. Current estimated cost is ~{currency} {item_cost:.2f}/month."
                    else:
                         recommendation = f"Gateway appears idle (Avg Connections: {item.get('avg_current_connections', 'N/A'):.1f}). Consider resizing, pausing, or deleting if unused. (Could not estimate current cost)."
                         item_cost = 0.0

                # --- Resources with no direct cost or already handled ---
                elif key == 'empty_rgs' or key == 'orphaned_nsgs' or key == 'orphaned_rts':
                    recommendation = "Delete if confirmed unused."
                    item_cost = 0.0 # No direct cost associated

            except Exception as e:
                # *** Modify exception logging to be safer ***
                item_name_for_log = item.get('name', 'Unknown') if isinstance(item, dict) else str(item) # Safely get name or string representation
                logger.warning(f"Error processing item for key '{key}' (Name/ID: '{item_name_for_log}'): {e}", exc_info=args.debug) # Show stacktrace if debug
                item_cost = 0.0 # Default to 0 if estimation fails

            item['Potential Monthly Savings'] = item_cost if item_cost is not None else 0.0 # Ensure it's a float
            item['Recommendation'] = recommendation
            # Don't add to total here, recalculate after processing all items

            # Rename/map columns to match target DataFrame columns (use .pop with default None)
            item['Name'] = item.pop('name', item.get('Name')) # Keep original if exists
            item['Resource Group'] = item.pop('resource_group', item.get('Resource Group'))
            item['Location'] = item.pop('location', item.get('Location'))
            item['Size (GB)'] = item.pop('size_gb', item.get('Size (GB)'))
            item['SKU'] = item.pop('sku', item.get('SKU'))
            item['Created Date'] = item.pop('time_created', item.get('Created Date')) # Handle 'time_created' from analysis
            item['VM Size'] = item.pop('size', item.get('VM Size')) # Handle 'size' from analysis
            item['Avg CPU %'] = item.pop('avg_cpu_percent', item.get('Avg CPU %'))
            item['Avg DTU %'] = item.pop('avg_dtu_percent', item.get('Avg DTU %'))
            item['Avg Connections'] = item.pop('avg_current_connections', item.get('Avg Connections'))
            item['Plan Name'] = item.pop('plan_name', item.get('Plan Name'))
            item['Plan Tier'] = item.pop('plan_tier', item.get('Plan Tier'))
            item['Tier'] = item.pop('tier', item.get('Tier')) # Keep tier if already exists
            item['IP Address'] = item.pop('ip_address', item.get('IP Address'))
            item['ID'] = item.pop('id', item.get('ID')) # Ensure ID is standardized
            item['OS Type'] = item.pop('os_type', item.get('OS Type')) # Add mapping for OS Type

            processed_findings[key].append(item)
            progress.update(task_savings, advance=1)

        # Recalculate total savings from processed items in case of errors / None values
        total_potential_savings = sum(item.get('Potential Monthly Savings', 0.0)
                                    for findings_list in processed_findings.values()
                                    for item in findings_list)
        potential_savings = {key: sum(item.get('Potential Monthly Savings', 0.0) for item in items)
                             for key, items in processed_findings.items() if items}

    console.print(f"\n[bold green]:dollar: Potential monthly savings identified: ~{currency} {total_potential_savings:.2f}[/]")

    # --- Create DataFrames from Processed Findings ---
    console.print("\n[bold blue]--- Preparing Report Data ---[/]")
    with console.status("[cyan]Creating result tables...[/]"):
        for key, findings_list in processed_findings.items():
            cols = columns_map.get(key)
            # Use the process_findings_to_df function which handles filtering and column selection
            findings_dfs[key], ignored_dfs[key] = process_findings_to_df(
                findings_list,
                finding_type=key,
                columns=cols
            )
            logger.info(f"Processed {key}: Found {len(findings_dfs[key])} actionable items, {len(ignored_dfs[key])} ignored items.")
            if not findings_dfs[key].empty:
                 logger.debug(f"Actionable {key} columns: {findings_dfs[key].columns.tolist()}")
                 logger.debug(f"Actionable {key} head:\n{findings_dfs[key].head().to_string()}")
            if not ignored_dfs[key].empty:
                 logger.debug(f"Ignored {key} columns: {ignored_dfs[key].columns.tolist()}")
                 logger.debug(f"Ignored {key} head:\n{ignored_dfs[key].head().to_string()}")

    # --- Export Findings for Grafana/External Tools ---
    grafana_export_dir = "grafana_export"
    console.print(f"\n[bold blue]--- Exporting Findings to CSV for External Tools ({grafana_export_dir}/) ---[/]")
    reporting.export_findings_to_csv_local(findings_dfs, grafana_export_dir)

    # --- Generate Console Summary Report ---
    reporting.generate_summary_report(
        findings_dfs=findings_dfs, 
        total_potential_savings=total_potential_savings,
        currency=currency,
        output_csv_file=args.csv_report, # Pass CSV filename
        console=console
    )

    # --- Generate HTML Report --- 
    # Use the reporting module
    console.print(f"\n[bold blue]--- Generating HTML Report ({args.html_report}) ---[/]")
    html_content = reporting.generate_html_report_content(
        findings=findings_dfs, # Pass processed DFs
        cost_data=None, # Pass raw data if needed by HTML report
        unattached_disks_df=findings_dfs.get('unattached_disks'),
        stopped_vms_df=findings_dfs.get('stopped_vms'),
        unused_public_ips_df=findings_dfs.get('unused_public_ips'),
        empty_resource_groups_df=findings_dfs.get('empty_rgs'),
        old_snapshots_df=findings_dfs.get('old_snapshots'),
        low_cpu_vms_df=findings_dfs.get('low_cpu_vms'),
        low_usage_app_service_plans_df=findings_dfs.get('low_cpu_asps'), # Key mismatch fix
        empty_plans_df=findings_dfs.get('empty_asps'), # Added for HTML report call consistency
        low_dtu_dbs_df=findings_dfs.get('low_dtu_dbs'),
        low_cpu_vcore_dbs_df=findings_dfs.get('low_cpu_vcore_dbs'),
        idle_gateways_df=findings_dfs.get('idle_gateways'),
        low_usage_apps_df=findings_dfs.get('low_cpu_apps'), # Key mismatch fix
        orphaned_nsgs_df=findings_dfs.get('orphaned_nsgs'),
        orphaned_route_tables_df=findings_dfs.get('orphaned_rts'),
        potential_savings=potential_savings,
        total_potential_savings=total_potential_savings,
        cost_breakdown=costs_by_type, # Use the cost data fetched earlier
        ignored_resources_df=pd.concat(ignored_dfs.values(), ignore_index=True) if ignored_dfs else pd.DataFrame(), # Pass combined ignored DF
        include_ignored=args.include_ignored_in_report,
        subscription_id=subscription_id, # Pass context
        currency=currency # Pass context
    )
    reporting.write_html_report(html_content, args.html_report)

    # --- Email Report --- (Using the function from reporting module)
    if args.send_email:
        # Prepare email config from env vars (or another source)
        smtp_config = {
            'host': os.environ.get('SMTP_HOST'),
            'port': int(os.environ.get('SMTP_PORT', 587)), # Default port
            'user': os.environ.get('SMTP_USER'),
            'password': os.environ.get('SMTP_PASSWORD'),
            'sender': os.environ.get('EMAIL_SENDER'),
            'recipient': [e.strip() for e in os.environ.get('EMAIL_RECIPIENT', '').split(',') if e.strip()]
        }
        # Use the console summary text for the email body
        reporting.send_email_report(console_summary, smtp_config, console=console)
    else:
        console.print("\n⏩ Email notification skipped. Use the --send-email flag and configure SMTP environment variables.")

    # --- Interactive Cleanup --- (Using the function from actions module)
    if args.cleanup or args.force_cleanup:
        actions.perform_interactive_cleanup(
            credential=credential, 
            subscription_id=subscription_id,
            findings_dfs=findings_dfs, # Pass the dictionary of filtered DataFrames
            console=console,
            wait_for_completion=args.wait_for_cleanup, 
            force_cleanup=args.force_cleanup
        )
    else:
        console.print("\n⏩ Cleanup actions skipped. Use --cleanup for interactive or --force-cleanup for non-interactive cleanup.")

    console.print("\n[bold green]🎉 Script finished.[/bold green]")
    logger.info("--- Script Execution Finished ---")


if __name__ == "__main__":
    main()