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
    log_level = logging.DEBUG if args.debug else logging.INFO
    global logger # Make logger accessible globally if needed elsewhere
    logger = utils.setup_logger(level=log_level, filename=config.LOG_FILENAME) # Pass level and filename
    logger.info("--- Script Execution Started ---")
    logger.info(f"Arguments: {args}")

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
        'stopped_vms': ['Name', 'Resource Group', 'Location', 'Recommendation', 'ID'],
        'unused_public_ips': ['Name', 'Resource Group', 'Location', 'IP Address', 'SKU', 'Potential Monthly Savings', 'ID'],
        'empty_rgs': ['Name', 'Location', 'Recommendation', 'ID'],
        'empty_asps': ['Name', 'Resource Group', 'Location', 'SKU', 'Tier', 'Potential Monthly Savings', 'ID'],
        'old_snapshots': ['Name', 'Resource Group', 'Location', 'Size (GB)', 'SKU', 'Created Date', 'Potential Monthly Savings', 'ID'],
        'low_cpu_vms': ['Name', 'Resource Group', 'Location', 'VM Size', 'Avg CPU %', 'Recommendation', 'ID'],
        'low_cpu_asps': ['Name', 'Resource Group', 'Location', 'SKU', 'Tier', 'Avg CPU %', 'Recommendation', 'ID'],
        'low_dtu_dbs': ['Name', 'Resource Group', 'Location', 'SKU', 'Tier', 'Avg DTU %', 'Recommendation', 'ID'],
        'low_cpu_vcore_dbs': ['Name', 'Resource Group', 'Location', 'SKU', 'Tier', 'Avg CPU %', 'Recommendation', 'ID'],
        'idle_gateways': ['Name', 'Resource Group', 'Location', 'SKU', 'Tier', 'Avg Connections', 'Recommendation', 'ID'],
        'low_cpu_apps': ['Name', 'Resource Group', 'Location', 'Plan Name', 'Plan Tier', 'Avg CPU %', 'Recommendation', 'ID'],
        'orphaned_nsgs': ['Name', 'Resource Group', 'Location', 'Recommendation', 'ID'],
        'orphaned_rts': ['Name', 'Resource Group', 'Location', 'Recommendation', 'ID']
    }

    # --- Calculate Potential Savings --- 
    console.print("\n[bold blue]--- Calculating Potential Savings ---[/]")
    with console.status("[cyan]Estimating savings for identified resources...[/]"):
        # Process each finding type
        for key, findings_list in all_findings_raw.items():
            if not findings_list: continue # Skip if no raw findings

            processed_list = []
            category_savings = 0.0
            
            # Add cost estimation logic here for relevant types
            for item in findings_list:
                item_cost = 0.0
                recommendation = "Review usage and necessity."
                try:
                    if key == 'unattached_disks':
                        item_cost = pricing.estimate_disk_cost(item.get('sku'), item.get('size_gb'), item.get('location'), console=console)
                        recommendation = f"Delete if unused to potentially save ~{currency} {item_cost:.2f}/month."
                    elif key == 'unused_public_ips':
                        item_cost = pricing.estimate_public_ip_cost(item.get('sku'), item.get('location'), console=console)
                        recommendation = f"Delete if unused to potentially save ~{currency} {item_cost:.2f}/month."
                    elif key == 'empty_asps':
                        item_cost = pricing.estimate_app_service_plan_cost(item.get('tier'), item.get('sku'), item.get('location'), console=console)
                        recommendation = f"Delete if unused to potentially save ~{currency} {item_cost:.2f}/month."
                    elif key == 'old_snapshots':
                         item_cost = pricing.estimate_snapshot_cost(item.get('size_gb'), item.get('location'), item.get('sku'), console=console)
                         recommendation = f"Delete if unused to potentially save ~{currency} {item_cost:.2f}/month."
                    elif key == 'stopped_vms':
                         recommendation = "Deallocate to stop compute charges, or delete if no longer needed."
                         # Note: Direct saving is compute, but keep disk cost in mind.
                    elif key == 'low_cpu_vms':
                         recommendation = "Consider resizing to a smaller, cheaper instance type."
                         # Savings calculation requires knowing target size - complex
                    elif key == 'low_cpu_asps' or key == 'low_cpu_apps':
                         recommendation = "Consider scaling down the plan or consolidating applications."
                         # Savings calculation requires knowing target SKU - complex
                    elif key == 'low_dtu_dbs' or key == 'low_cpu_vcore_dbs':
                         recommendation = "Consider scaling down the database tier."
                         # Savings calculation requires knowing target tier - complex
                    elif key == 'idle_gateways':
                         recommendation = "Consider resizing, pausing (if possible), or deleting if unused."
                         # Savings calculation depends on action
                    elif key == 'empty_rgs' or key == 'orphaned_nsgs' or key == 'orphaned_rts':
                         recommendation = "Delete if confirmed unused."
                         # No direct cost associated with the container/rule itself usually

                except Exception as e:
                    logger.warning(f"Error estimating cost for {key} '{item.get('name')}': {e}", exc_info=True)
                    item_cost = 0.0 # Default to 0 if estimation fails
                
                item['Potential Monthly Savings'] = item_cost
                item['Recommendation'] = recommendation
                # Rename/map columns to match target DataFrame columns
                item['Size (GB)'] = item.pop('size_gb', None)
                item['SKU'] = item.pop('sku', None)
                item['Created Date'] = item.pop('time_created', None)
                item['VM Size'] = item.pop('size', None)
                item['Avg CPU %'] = item.pop('avg_cpu_percent', None)
                item['Avg DTU %'] = item.pop('avg_dtu_percent', None)
                item['Avg Connections'] = item.pop('avg_current_connections', None)
                item['Plan Name'] = item.pop('plan_name', None)
                item['Plan Tier'] = item.pop('plan_tier', None)
                item['Tier'] = item.pop('tier', item.get('Tier')) # Keep tier if already exists
                item['IP Address'] = item.pop('ip_address', None)
                item['Details'] = item.pop('details', None) # Catch all for specific details
                item['Name'] = item.get('name') # Ensure Name exists
                item['Resource Group'] = item.get('resource_group')
                item['Location'] = item.get('location')
                item['ID'] = item.get('id')
                
                processed_list.append(item)
                # Add item_cost only if it's not ignored later
                # We calculate total savings AFTER filtering

            # Create DataFrame and filter ignored resources
            target_columns = columns_map.get(key, list(processed_list[0].keys()) if processed_list else [])
            df_filtered, df_ignored = process_findings_to_df(processed_list, key, columns=target_columns)
            
            findings_dfs[key] = df_filtered
            ignored_dfs[key] = df_ignored

            # Calculate savings based on FILTERED items
            if not df_filtered.empty and 'Potential Monthly Savings' in df_filtered.columns:
                category_savings = pd.to_numeric(df_filtered['Potential Monthly Savings'], errors='coerce').sum()
                if category_savings > 0:
                    potential_savings[key.replace('_', ' ').title()] = category_savings
                    total_potential_savings += category_savings

    # Combine all ignored items into one DataFrame for the HTML report
    all_ignored_list = []
    for key, df_ignored in ignored_dfs.items():
        if not df_ignored.empty:
            temp_df = df_ignored.copy()
            temp_df['Finding Type'] = key.replace('_', ' ').title() # Add type
            # Ensure standard columns exist
            base_cols = ['Finding Type', 'Name', 'Resource Group', 'Location', 'ID']
            for col in base_cols:
                if col not in temp_df.columns:
                    temp_df[col] = 'N/A'
            all_ignored_list.append(temp_df[base_cols]) # Select standard columns
    
    ignored_resources_df = pd.concat(all_ignored_list, ignore_index=True) if all_ignored_list else pd.DataFrame()
    
    # --- Generate Console Summary Report --- 
    # Use the reporting module
    console_summary = reporting.generate_summary_report(
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
        ignored_resources_df=ignored_resources_df, # Pass combined ignored DF
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