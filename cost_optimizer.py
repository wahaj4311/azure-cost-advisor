import os
import csv
import argparse
import time
import smtplib
from email.message import EmailMessage
import logging
import requests
import json
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.costmanagement.models import QueryTimePeriod, QueryDataset, QueryDefinition
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.web import WebSiteManagementClient
from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.sql import SqlManagementClient
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
import rich

# Initialize Rich Console
console = Console()

# --- Configuration ---
SNAPSHOT_AGE_THRESHOLD_DAYS = 90
LOW_CPU_THRESHOLD_PERCENT = 5
# LOW_MEMORY_THRESHOLD_PERCENT = 20 # Removing memory threshold for now
APP_SERVICE_PLAN_LOW_CPU_THRESHOLD_PERCENT = 10 # Flag ASPs averaging below this CPU %
METRIC_LOOKBACK_DAYS = 7
LOG_FILENAME = "cleanup_log.txt"
SQL_DB_LOW_DTU_THRESHOLD_PERCENT = 10 # Flag DBs averaging below this DTU %
RETAIL_PRICES_API_ENDPOINT = "https://prices.azure.com/api/retail/prices"
HOURS_PER_MONTH = 730 # Approximate hours for monthly cost estimation

# --- Pricing Cache --- 
# Simple in-memory cache { filter_string: price_object }
price_cache = {}

# --- Pricing Helper Function --- 
def get_retail_price(filter_string):
    """Gets Azure retail price using OData filter, with basic caching."""
    global price_cache
    # Add logger for potential debugging
    import logging
    logger = logging.getLogger()

    if filter_string in price_cache:
        logger.debug(f"Cache hit for filter: {filter_string}")
        return price_cache[filter_string]

    api_url = f"{RETAIL_PRICES_API_ENDPOINT}?$filter={filter_string}"
    selected_price_info = None
    try:
        logger.debug(f"Querying Retail API: {api_url}")
        response = requests.get(api_url)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()

        items = data.get('Items', [])
        logger.debug(f"Filter '{filter_string}' returned {len(items)} items.")
        if len(items) > 5:
            logger.debug(f"First 5 items: {items[:5]}") # Log first few items if many

        if items:
            # Iterate to find the most suitable price
            for item in items:
                price_type = item.get('priceType', '')
                retail_price = item.get('retailPrice', 0)
                meter_name = item.get('meterName', '') # Potential future filter criteria
                sku_name_api = item.get('skuName', '')     # Potential future filter criteria
                
                # Prioritize Consumption pricing and non-zero prices
                if price_type == 'Consumption' and retail_price > 0:
                    selected_price_info = item
                    logger.debug(f"Selected item: {item} based on Consumption/Price > 0")
                    break # Take the first suitable item
            
            # Fallback: If no ideal match, take the first item with a price > 0
            if not selected_price_info:
                 for item in items:
                    retail_price = item.get('retailPrice', 0)
                    if retail_price > 0:
                        selected_price_info = item
                        logger.debug(f"Selected fallback item: {item} based on Price > 0")
                        break

            # Fallback 2: If still no item, log warning and take the first item if exists
            if not selected_price_info and items:
                selected_price_info = items[0]
                logger.warning(f"No ideal price found for filter '{filter_string}'. Using first item: {selected_price_info}")

        if not selected_price_info:
             logger.warning(f"No price information found or matched for filter: {filter_string}")
             pass # Keep selected_price_info as None

    except requests.exceptions.RequestException as e:
        logger.error(f"Error querying Azure Retail Prices API ({api_url}): {e}")
    except json.JSONDecodeError as e:
         logger.error(f"Error parsing price API response ({api_url}): {e}")
    except Exception as e:
         logger.error(f"Unexpected error fetching price ({api_url}): {e}")

    # Cache the result (even if None) to avoid re-querying for misses
    price_cache[filter_string] = selected_price_info
    return selected_price_info

def estimate_monthly_cost(price_info):
    """Estimates monthly cost from a price info object."""
    if not price_info or price_info.get('retailPrice') is None:
        return None, None # Indicate cost couldn't be estimated

    price = price_info['retailPrice']
    unit = price_info.get('unitOfMeasure', '').lower()
    currency = price_info.get('currencyCode', 'USD') # Default to USD if missing
    
    monthly_cost = None
    # Handle common pricing units
    if 'hour' in unit:
        # Assumes price is per unit per hour (e.g., VM cost, IP Address Hour)
        monthly_cost = price * HOURS_PER_MONTH 
    elif 'gb/month' in unit:
        # Price is directly per GB per month (e.g., some storage types)
        monthly_cost = price 
    elif unit == '1 gb' or unit == 'gb': 
        # Assume price is per GB per Hour? Or per GB per Month?
        # Let's assume per GB / Month for snapshot-like storage for now
        # TODO: Verify this assumption for different services using 'GB' unit
        monthly_cost = price 
        # If it was per GB / Hour, it would be: price * HOURS_PER_MONTH
    elif 'month' in unit: 
        # Catch other direct per-month prices (e.g., support plans)
         monthly_cost = price
    # Add other unit conversions if needed (e.g., per 10k transactions)
    else:
         # Cannot easily convert this unit to monthly cost
         print(f"  - Warning: Cannot estimate monthly cost for unit '{unit}'. Reporting raw price: {price} {currency}")
         return price, f"{currency} / {unit}" # Return raw price and unit

    return monthly_cost, f"{currency} / Month"

# --- Specific Cost Estimators ---
def estimate_disk_cost(sku_name, size_gb, location):
    """Estimates the monthly cost for a managed disk."""
    logger = logging.getLogger()
    # Disk SKUs often include tier and redundancy (e.g., Premium_LRS, StandardSSD_ZRS)
    # Disk Sizes map to tiers (e.g., P10 for 128GiB Premium SSD, E10 for 128GiB Standard SSD)
    # Meter names can be like 'P10 LRS Disk', 'E10 LRS Disk', 'Standard HDD Managed Disks' (per GB)

    # Normalize location for API (e.g., 'East US' -> 'eastus')
    api_location = location.lower().replace(' ', '')

    filter_base = f"armRegionName eq '{api_location}' and priceType eq 'Consumption'"

    # Attempt to filter directly by SKU name first (more precise for Premium/StandardSSD tiers)
    filter_str = filter_base + f" and skuName eq '{sku_name}'"
    # Example: "armRegionName eq 'eastus' and priceType eq 'Consumption' and skuName eq 'Premium_LRS_P10'"
    logger.debug(f"Attempting direct SKU lookup for disk: {sku_name} in {api_location}")
    price_info = get_retail_price(filter_str)

    # Fallback: If direct SKU match fails (e.g., for Standard HDD which might not have size in skuName),
    # try filtering by service name and meter name containing 'Standard HDD'
    # Also use this fallback if the direct SKU lookup returned a price of 0 (might happen for some composite SKUs)
    if (not price_info or price_info.get('retailPrice', 0) == 0) and \
       'standard' in sku_name.lower() and 'ssd' not in sku_name.lower():
        logger.debug(f"Direct SKU lookup failed or returned zero price for {sku_name}. Trying Standard HDD filter...")
        filter_str = filter_base + f" and serviceName eq 'Managed Disks' and contains(meterName, 'Standard HDD Managed Disks')"
        price_info = get_retail_price(filter_str)

    cost, unit_str = estimate_monthly_cost(price_info)

    final_cost = 0.0
    if cost is not None and price_info:
        # If the price is per GB/Month (like Standard HDD or Snapshots), multiply by size
        unit = price_info.get('unitOfMeasure', '').lower()
        if 'gb' in unit:
            final_cost = cost * size_gb
            logger.debug(f"Disk {sku_name} cost is per GB/Month. Price/GB: {cost}, Size: {size_gb}GB, Final: {final_cost}")
        else:
            # Assume fixed monthly cost for the tier (e.g., P10 LRS Disk is a fixed monthly price)
            final_cost = cost
            logger.debug(f"Disk {sku_name} cost is fixed monthly. Price: {cost}")
    elif cost is not None: # Handle cases where estimate_monthly_cost returned raw price/unit
        logger.warning(f"Could not estimate monthly cost for disk {sku_name} with unit {unit_str}. Raw price: {cost}")
        final_cost = 0.0 # Cannot reliably estimate savings
    else:
         logger.warning(f"Could not find price info for disk {sku_name} in {location} using filter: {filter_str}")
         final_cost = 0.0

    return final_cost

def estimate_public_ip_cost(sku_name, location):
    """Estimates the monthly cost for a Public IP address (placeholder)."""
    # Example filter (Needs refinement based on actual SKU naming and meters)
    # Standard_LRS, Premium_LRS, StandardSSD_LRS, UltraSSD_LRS
    # Meter names vary, e.g., 'Standard Managed Disk LRS Snapshots', 'P10 LRS Disk'
    # This is complex due to tiers, performance, snapshots etc.
    # Placeholder filter - needs accurate serviceName, meterName/skuName mapping
    filter_str = f"armRegionName eq '{location}' and serviceFamily eq 'Storage' and contains(skuName, '{sku_name}') and contains(meterName, 'Disk')" # Highly approximate
    
    price_info = get_retail_price(filter_str)
    # TODO: Refine filter and handle different disk types/pricing models (per GB, per IOPS etc.)
    cost, _ = estimate_monthly_cost(price_info) 
    # Simplistic: If cost is per GB/Month, multiply by size. If per hour, already handled.
    # This needs a robust mapping from SKU to pricing model.
    if cost and price_info and 'gb/month' in price_info.get('unitOfMeasure', '').lower():
        cost *= size_gb # Adjust cost based on size if priced per GB/Month

    return cost if cost is not None else 0.0 # Return 0 if cost cannot be estimated

def estimate_public_ip_cost(sku_name, location):
    """Estimates the monthly cost for a Public IP address (placeholder)."""
    # Public IP costs depend on SKU (Basic/Standard), Static/Dynamic, and potentially usage.
    # For unused static IPs, there's usually an hourly charge.
    # Meter names like 'Standard Static IP Address Hours', 'Basic Static IP Address Hours'
    sku_filter = sku_name if sku_name else 'Basic' # Default to Basic if SKU is missing
    meter_name_fragment = f"{sku_filter} Static IP Address Hours"
    
    # Construct filter
    filter_str = f"armRegionName eq '{location}' and serviceFamily eq 'Networking' and contains(meterName, '{meter_name_fragment}') and priceType eq 'Consumption'"
    
    price_info = get_retail_price(filter_str)
    cost, _ = estimate_monthly_cost(price_info)
    
    return cost if cost is not None else 0.0 # Return 0 if cost cannot be estimated

def estimate_snapshot_cost(size_gb, location, sku_name):
    """Estimates the monthly cost for a managed disk snapshot."""
    # Snapshots are typically priced per GB/Month based on storage type.
    # Meter names often include 'Managed Disks Snapshots'
    # e.g., 'Standard HDD Managed Disks Snapshots', 'Premium SSD Managed Disks Snapshots' (LRS/ZRS)
    # SkuName might be less helpful here compared to meterName.
    
    # Determine storage type from SKU (simplified)
    storage_type = "Standard HDD" # Default
    if 'Premium'.lower() in sku_name.lower():
        storage_type = "Premium SSD"
    elif 'StandardSSD'.lower() in sku_name.lower():
        storage_type = "Standard SSD"
    # TODO: Handle ZRS vs LRS if needed (meter name usually includes this)
    
    meter_name_fragment = f"{storage_type} Managed Disks Snapshots"
    
    # Construct filter
    filter_str = f"armRegionName eq '{location}' and serviceFamily eq 'Storage' and contains(meterName, '{meter_name_fragment}') and priceType eq 'Consumption'" 
    
    price_info = get_retail_price(filter_str)
    cost_per_gb_month, _ = estimate_monthly_cost(price_info) 
    
    final_cost = 0.0
    if cost_per_gb_month is not None:
        # Check if the unit was per GB/Month or similar (handled by estimate_monthly_cost)
        # If the price was indeed per GB/Month, multiply by the size.
        unit = price_info.get('unitOfMeasure', '').lower()
        if 'gb' in unit:
             final_cost = cost_per_gb_month * size_gb
        else:
             # If the unit was just /Month, maybe it's a base cost? Unlikely for snapshots.
             # Log a warning if the unit isn't GB-based
             print(f"  - Warning: Unexpected unit '{unit}' for snapshot cost calculation. Price: {cost_per_gb_month}")
             final_cost = cost_per_gb_month # Use the cost directly, might be wrong

    return final_cost

def estimate_app_service_plan_cost(sku_name, location):
    """Estimates the monthly cost for an App Service Plan SKU."""
    # ASP costs depend heavily on the SKU (e.g., B1, S1, P1V2, I1) and OS.
    # Meter names often look like 'B1 App Service Plan Hours', 'S1 App Service Plan'
    # We need to map the input sku_name (e.g., 'B1', 'S1') to the meterName or productName.
    
    # Simplistic approach: Assume meterName contains the SKU name directly.
    # This might need adjustment for PremiumV2/V3, Isolated SKUs, Windows vs Linux plans.
    meter_name_fragment = f"{sku_name} App Service Plan"
    
    # Construct filter - Add OS specific filters if needed (Windows/Linux often have different meters)
    filter_str = f"armRegionName eq '{location}' and serviceFamily eq 'Compute' and contains(meterName, '{meter_name_fragment}') and priceType eq 'Consumption'"
    
    price_info = get_retail_price(filter_str)
    cost, _ = estimate_monthly_cost(price_info)
    
    # ASPs are usually priced per hour, estimate_monthly_cost handles the conversion.
    return cost if cost is not None else 0.0

def estimate_sql_database_cost(sku_name, location):
    """Estimates the monthly cost for a SQL Database DTU tier (placeholder)."""
    # Pricing depends on DTU tier (Basic, S0, S1, P1 etc.) or vCore model.
    # This function currently focuses on DTU Tiers found by 'find_low_dtu_sql_databases'.
    # Meter names might be 'Basic DTU', 'Standard S0 DTUs', 'Premium P1 DTUs'
    
    # Simplistic: Assume sku_name matches the core part of the meter name (e.g., 'S0', 'Basic')
    # This needs refinement based on actual meter naming conventions.
    meter_name_fragment = f"{sku_name} DTU"
    if 'basic' in sku_name.lower(): # Basic tier might have different meter naming
        meter_name_fragment = 'Basic DTU'
    elif 'standard' in sku_name.lower():
         meter_name_fragment = f"{sku_name.replace('Standard ', '')} DTUs" # e.g. S0 DTUs
    elif 'premium' in sku_name.lower():
         meter_name_fragment = f"{sku_name.replace('Premium ', '')} DTUs" # e.g. P1 DTUs
    
    # Construct filter
    filter_str = f"armRegionName eq '{location}' and serviceFamily eq 'Databases' and contains(meterName, '{meter_name_fragment}') and priceType eq 'Consumption'"
    
    price_info = get_retail_price(filter_str)
    cost, _ = estimate_monthly_cost(price_info)
    
    # DTU costs are usually fixed per month for the tier, handled by estimate_monthly_cost.
    return cost if cost is not None else 0.0

# --- Logging Setup Function ---
def setup_logger():
    """Configures the logger to write to a file."""
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=LOG_FILENAME,
        filemode='a' # Append to the log file
    )
    # Optional: Also log to console during development/debugging
    # console_handler = logging.StreamHandler()
    # console_handler.setLevel(logging.INFO)
    # formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # console_handler.setFormatter(formatter)
    # logging.getLogger('').addHandler(console_handler)

def get_azure_credentials():
    """Authenticates and determines the Azure Subscription ID."""
    try:
        subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID")
        # Wrap credential fetching in status (might take a moment)
        with console.status("[cyan]Authenticating with Azure...[/]"):
            credential = DefaultAzureCredential()

        if not subscription_id:
            console.print("[yellow]AZURE_SUBSCRIPTION_ID not set. Attempting to detect subscription...[/]")
            # Use SubscriptionClient to find accessible subscriptions
            subscription_client = SubscriptionClient(credential)
            with console.status("[cyan]Listing accessible subscriptions...[/]"):
                subs = list(subscription_client.subscriptions.list())

            if not subs:
                raise ValueError("No Azure subscriptions found for the current credential.")
            elif len(subs) == 1:
                subscription_id = subs[0].subscription_id
                console.print(f"Automatically detected and using subscription: [bold cyan]{subs[0].display_name}[/] ({subscription_id})")
            else:
                console.print("[bold yellow]Multiple Azure subscriptions found:[/]")
                for sub in subs:
                    console.print(f"  - [cyan]{sub.display_name}[/] ({sub.subscription_id})")
                raise ValueError("Multiple subscriptions found. Please set the AZURE_SUBSCRIPTION_ID environment variable to specify which one to use.")

        console.print(f"Using Subscription ID: [bold cyan]{subscription_id}[/]")
        console.print(":white_check_mark: [bold green]Authenticated successfully.[/]")
        return credential, subscription_id

    except Exception as e:
        console.print(f"[bold red]Authentication or subscription detection failed:[/] {e}")
        return None, None

def list_all_resources(credential, subscription_id):
    """Lists all resources in the subscription."""
    resources = []
    try:
        resource_client = ResourceManagementClient(credential, subscription_id)
        console.print("\n[bold blue]--- Fetching Azure Resources ---[/]")
        # Use console.status for the potentially long-running list operation
        with console.status("[cyan]Listing all resources...[/]"):
            resource_list = list(resource_client.resources.list())
        
        # Process the list after fetching is complete
        for resource in resource_list:
            resources.append({
                "name": resource.name,
                "type": resource.type,
                "location": resource.location,
                "id": resource.id,
                "tags": resource.tags
            })
            # Keep this print minimal or remove for cleaner output
            # console.print(f"  :mag: Found: {resource.name} ([dim]{resource.type}[/]) in {resource.location}")
        console.print(f":white_check_mark: Total resources found: {len(resources)}")
        return resources
    except Exception as e:
        console.print(f"[bold red]Error listing resources:[/] {e}")
        return []

def get_cost_data(credential, subscription_id):
    """Retrieves cost data for the current billing month, grouped by Resource Type."""
    costs_by_type = defaultdict(float)
    total_cost = 0.0
    currency = "N/A"
    try:
        cost_client = CostManagementClient(credential)
        scope = f"subscriptions/{subscription_id}"

        now = datetime.now(timezone.utc)
        start_of_month = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
        time_period = QueryTimePeriod(from_property=start_of_month, to=now)

        query_definition = QueryDefinition(
            type="ActualCost",
            timeframe="Custom",
            time_period=time_period,
            dataset=QueryDataset(
                granularity="None",
                aggregation={"totalCost": {"name": "Cost", "function": "Sum"}},
                grouping=[{"type": "Dimension", "name": "ResourceType"}]
            )
        )

        console.print("\n[bold blue]--- Fetching Cost Data ---[/]")
        with console.status("[cyan]Querying cost data for current month...[/]"):
            result = cost_client.query.usage(scope=scope, parameters=query_definition)

        if result and result.rows:
            cost_index, currency_index, res_type_index = -1, -1, -1
            if result.columns:
                for i, col in enumerate(result.columns):
                    if col.name == "Cost": cost_index = i
                    elif col.name == "Currency": currency_index = i
                    elif col.name == "ResourceType": res_type_index = i

            if cost_index != -1 and currency_index != -1 and res_type_index != -1:
                console.print("  [bold]Cost Breakdown by Resource Type (Month-to-Date):[/]")
                # Use Rich Table for better formatting
                table = Table(show_header=False, box=None, padding=(0, 1))
                table.add_column(style="cyan") # Resource Type
                table.add_column(style="green") # Cost
                
                sorted_rows = sorted([row for row in result.rows if row[res_type_index]], key=lambda r: r[cost_index], reverse=True)

                for row in sorted_rows:
                    cost = row[cost_index]
                    curr = row[currency_index]
                    res_type = row[res_type_index]
                    if currency == "N/A": currency = curr
                    costs_by_type[res_type] += cost
                    total_cost += cost
                    table.add_row(f"  - {res_type}", f"{cost:.2f} {curr}")
                console.print(table)
                console.print(f"  [bold]Total Estimated Cost:[/][bold green] {total_cost:.2f} {currency}[/]")
            else:
                 console.print("[yellow]  - Warning: Could not parse cost data columns correctly.[/]")
                 # Fallback logic
                 if result.rows and result.rows[0]:
                     total_cost = result.rows[0][0] if len(result.rows[0]) > 0 else 0.0
                     currency = result.rows[0][1] if len(result.rows[0]) > 1 else "N/A"
                     console.print(f"[yellow]  - Fallback Total Cost:[/][bold yellow] {total_cost:.2f} {currency}[/]")
        else:
            console.print("[yellow]  - No cost data found for the period.[/]")

        return costs_by_type, total_cost, currency

    except Exception as e:
        console.print(f"[bold red]Error fetching cost data:[/] {e}")
        if "429" in str(e):
            console.print("[yellow]  - Suggestion: Cost Management API might be throttled. Try again later.[/]")
        return costs_by_type, total_cost, currency # Return empty/default values

def find_unattached_disks(credential, subscription_id):
    """Finds managed disks that are not attached to any VM."""
    console.print("\n:floppy_disk: [blue]Checking for unattached managed disks...[/]")
    compute_client = ComputeManagementClient(credential, subscription_id)
    unattached_disks = []
    try:
        with console.status("[cyan]Listing disks...[/]"):
            disk_list = list(compute_client.disks.list())
        
        for disk in disk_list:
            if disk.managed_by is None and disk.disk_state == 'Unattached':
                unattached_disks.append({
                    'name': disk.name,
                    'resource_group': disk.id.split('/')[4],
                    'location': disk.location,
                    'size_gb': disk.disk_size_gb,
                    'sku': disk.sku.name,
                    'id': disk.id,
                })
        if not unattached_disks:
            console.print("  :heavy_check_mark: No unattached managed disks found.")
        else:
             console.print(f"  :warning: Found {len(unattached_disks)} unattached disk(s).")
    except Exception as e:
        console.print(f"  [bold red]Error checking for unattached disks:[/] {e}")
    return unattached_disks

# --- Add Stopped VM Detection Function ---
def find_stopped_vms(credential, subscription_id):
    """Finds VMs that are stopped but not deallocated."""
    stopped_vms = []
    console.print("\n:stop_sign: [blue]Checking for stopped (not deallocated) VMs...[/]")
    try:
        compute_client = ComputeManagementClient(credential, subscription_id)
        vm_list = []
        with console.status("[cyan]Listing all VMs...[/]"):
            vm_list = list(compute_client.virtual_machines.list_all())
        
        # Check power state after listing
        checked_count = 0
        with console.status("[cyan]Checking VM power states...[/]"):
            for vm in vm_list:
                checked_count += 1
                # Update status periodically
                if checked_count % 10 == 0:
                     console.status(f"[cyan]Checking VM power states ({checked_count}/{len(vm_list)})...[/]")
                try:
                    instance_view = compute_client.virtual_machines.instance_view(
                        resource_group_name=vm.id.split('/')[4],
                        vm_name=vm.name
                    )
                    power_state = None
                    for status in instance_view.statuses:
                        if status.code.startswith("PowerState/"):
                            power_state = status.code.split('/')[-1]
                            break
                    if power_state == "stopped":
                        stopped_vms.append({
                            "name": vm.name, "id": vm.id, "resource_group": vm.id.split('/')[4],
                            "location": vm.location
                        })
                except Exception as iv_error:
                     console.print(f"  [yellow]Warning:[/][dim] Could not get instance view for VM {vm.name}. Error: {iv_error}[/]")

        if not stopped_vms:
            console.print("  :heavy_check_mark: No stopped (but not deallocated) VMs found.")
        else:
            console.print(f"  :warning: Found {len(stopped_vms)} stopped VM(s).")
        return stopped_vms

    except Exception as e:
        console.print(f"[bold red]Error checking for stopped VMs:[/] {e}")
        return []

# --- Add Unused Public IP Detection Function ---
def find_unused_public_ips(credential, subscription_id):
    """Finds Public IP Addresses that are not associated with any resource."""
    unused_ips = []
    console.print("\n:globe_with_meridians: [blue]Checking for unused Public IP Addresses...[/]")
    try:
        network_client = NetworkManagementClient(credential, subscription_id)
        ip_list = []
        with console.status("[cyan]Listing public IP addresses...[/]"):
             ip_list = list(network_client.public_ip_addresses.list_all())

        for ip in ip_list:
            if ip.ip_configuration is None:
                sku_name = ip.sku.name if ip.sku else "Basic"
                unused_ips.append({
                    "name": ip.name, "id": ip.id, "resource_group": ip.id.split('/')[4],
                    "location": ip.location, "ip_address": ip.ip_address,
                    "sku": sku_name
                })

        if not unused_ips:
            console.print("  :heavy_check_mark: No unused Public IP Addresses found.")
        else:
            console.print(f"  :warning: Found {len(unused_ips)} unused Public IP(s).")
        return unused_ips

    except Exception as e:
        console.print(f"[bold red]Error checking for unused Public IPs:[/] {e}")
        return []

# --- Add Empty Resource Group Detection Function ---
def find_empty_resource_groups(credential, subscription_id):
    """Finds Resource Groups that contain no resources."""
    empty_rgs = []
    console.print("\n:wastebasket: [blue]Checking for empty Resource Groups...[/]")
    try:
        resource_client = ResourceManagementClient(credential, subscription_id)
        rg_list = []
        with console.status("[cyan]Listing resource groups...[/]"):
            rg_list = list(resource_client.resource_groups.list())
        
        checked_count = 0
        with console.status("[cyan]Checking resource group contents...[/]"):
            for rg in rg_list:
                checked_count += 1
                if checked_count % 10 == 0:
                     console.status(f"[cyan]Checking resource group contents ({checked_count}/{len(rg_list)})...[/]")
                resources_in_rg = list(resource_client.resources.list_by_resource_group(rg.name))
                if not resources_in_rg:
                    empty_rgs.append({
                        "name": rg.name, "id": rg.id, "location": rg.location,
                    })

        if not empty_rgs:
            console.print("  :heavy_check_mark: No empty Resource Groups found.")
        else:
            console.print(f"  :warning: Found {len(empty_rgs)} empty Resource Group(s).")
        return empty_rgs

    except Exception as e:
        console.print(f"[bold red]Error checking for empty Resource Groups:[/] {e}")
        return []

# --- Add Empty App Service Plan Detection Function ---
def find_empty_app_service_plans(credential, subscription_id):
    """Finds App Service Plans that host no applications."""
    empty_plans = []
    console.print("\n:spider_web: [blue]Checking for empty App Service Plans...[/]") # Changed icon
    try:
        web_client = WebSiteManagementClient(credential, subscription_id)
        plan_list = []
        with console.status("[cyan]Listing App Service Plans...[/]"):
             plan_list = list(web_client.app_service_plans.list())
        
        checked_count = 0
        with console.status("[cyan]Checking for apps in plans...[/]"):
            for plan in plan_list:
                checked_count += 1
                if checked_count % 5 == 0:
                     console.status(f"[cyan]Checking for apps in plans ({checked_count}/{len(plan_list)})...[/]")
                try:
                    plan_rg = plan.id.split('/')[4]
                    apps_in_plan = list(web_client.app_service_plans.list_web_apps(plan_rg, plan.name))
                    if not apps_in_plan:
                        empty_plans.append({
                            "name": plan.name, "id": plan.id, "resource_group": plan_rg,
                            "location": plan.location, "sku": plan.sku.name if plan.sku else "Unknown"
                        })
                except Exception as list_apps_error:
                    console.print(f"  [yellow]Warning:[/][dim] Could not check apps for App Service Plan {plan.name}. Error: {list_apps_error}[/]")

        if not empty_plans:
            console.print("  :heavy_check_mark: No empty App Service Plans found.")
        else:
            console.print(f"  :warning: Found {len(empty_plans)} empty App Service Plan(s).")
        return empty_plans

    except Exception as e:
        console.print(f"[bold red]Error checking for empty App Service Plans:[/] {e}")
        return []

# --- Add Old Snapshot Detection Function ---
def find_old_snapshots(credential, subscription_id, age_threshold_days=SNAPSHOT_AGE_THRESHOLD_DAYS):
    """Finds disk snapshots older than a specified number of days."""
    old_snapshots = []
    now = datetime.now(timezone.utc)
    age_threshold = timedelta(days=age_threshold_days)
    try:
        compute_client = ComputeManagementClient(credential, subscription_id)
        print(f"\nChecking for disk snapshots older than {age_threshold_days} days...")

        for snapshot in compute_client.snapshots.list():
            try:
                # Ensure time_created is timezone-aware for comparison
                creation_time = snapshot.time_created
                if creation_time.tzinfo is None:
                    creation_time = creation_time.replace(tzinfo=timezone.utc) # Assume UTC if not specified

                if (now - creation_time) > age_threshold:
                    rg_name = snapshot.id.split('/')[4]
                    old_snapshots.append({
                        "name": snapshot.name,
                        "id": snapshot.id,
                        "resource_group": rg_name,
                        "location": snapshot.location,
                        "time_created": creation_time.isoformat(),
                        "size_gb": snapshot.disk_size_gb
                    })
                    print(f"  - Found old snapshot: {snapshot.name} (RG: {rg_name}, Created: {creation_time.strftime('%Y-%m-%d')}, Size: {snapshot.disk_size_gb}GB)")

            except Exception as snap_error:
                 print(f"  - Warning: Could not process snapshot {snapshot.name}. Error: {snap_error}")

        if not old_snapshots:
            print(f"  - No disk snapshots older than {age_threshold_days} days found.")
        else:
            print(f"Total old snapshots found: {len(old_snapshots)}")

        return old_snapshots

    except Exception as e:
        print(f"Error checking for old snapshots: {e}")
        return []

# --- Modify Underutilized VM Detection Function (Query Available Memory Bytes) ---
def find_underutilized_vms(credential, subscription_id, 
                         cpu_threshold_percent=LOW_CPU_THRESHOLD_PERCENT, 
                         lookback_days=METRIC_LOOKBACK_DAYS):
    """Finds running VMs with low average CPU usage and reports available memory."""
    low_cpu_vms = []
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=lookback_days)
    
    time_format = "%Y-%m-%dT%H:%M:%SZ"
    start_time_str = start_time.strftime(time_format)
    end_time_str = now.strftime(time_format)
    timespan = f"{start_time_str}/{end_time_str}"
    interval = "PT1H" 

    try:
        compute_client = ComputeManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)
        print(f"\nChecking running VMs for low avg CPU (< {cpu_threshold_percent}%) and reporting avg available memory over the last {lookback_days} days...")
        print(f"  - Querying metrics with timespan: {timespan}")

        # --- Get list of running VMs only --- 
        running_vms = []
        for vm_obj in compute_client.virtual_machines.list_all():
             try:
                 instance_view = compute_client.virtual_machines.instance_view(vm_obj.id.split('/')[4], vm_obj.name)
                 power_state = "unknown"
                 for status in instance_view.statuses:
                     if status.code.startswith("PowerState/"):
                         power_state = status.code.split('/')[-1]
                         break
                 if power_state == "running":
                     running_vms.append(vm_obj)
             except Exception as iv_error:
                 print(f"  - Warning: Could not get instance view for VM {vm_obj.name}. Skipping usage check. Error: {iv_error}")

        if not running_vms:
            print("  - No running VMs found to check metrics for.")
            return [] # Return one empty list now
        else:
             print(f"  - Found {len(running_vms)} running VMs to analyze...")

        # --- Query Metrics for Running VMs --- 
        for vm in running_vms:
            vm_resource_uri = vm.id
            avg_cpu = None
            avg_available_memory_bytes = None # Changed variable name
            try:
                # Query 'Percentage CPU' and 'Available Memory Bytes'
                metric_names = "Percentage CPU,Available Memory Bytes"
                metrics_data = monitor_client.metrics.list(
                    resource_uri=vm_resource_uri,
                    timespan=timespan,
                    interval=interval,
                    metricnames=metric_names,
                    aggregation="Average" 
                )

                # Process results for both metrics
                if metrics_data.value:
                    for metric in metrics_data.value:
                        metric_name = metric.name.value
                        time_series = metric.timeseries
                        if time_series and time_series[0].data:
                            total_val = 0
                            count = 0
                            for point in time_series[0].data:
                                if point.average is not None:
                                    total_val += point.average
                                    count += 1
                            if count > 0:
                                avg_val = total_val / count
                                if metric_name == "Percentage CPU":
                                    avg_cpu = avg_val
                                elif metric_name == "Available Memory Bytes":
                                    avg_available_memory_bytes = avg_val # Assign to new variable
                
                # Prepare log message
                log_msg = f"  - Analyzed VM: {vm.name}"
                cpu_msg = f"Avg CPU: {avg_cpu:.2f}%" if avg_cpu is not None else "Avg CPU: N/A"
                # Format available memory bytes (e.g., to MB/GB for readability)
                if avg_available_memory_bytes is not None:
                    if avg_available_memory_bytes > (1024**3):
                        mem_msg = f"Avg Available Mem: {avg_available_memory_bytes / (1024**3):.2f} GB"
                    elif avg_available_memory_bytes > (1024**2):
                        mem_msg = f"Avg Available Mem: {avg_available_memory_bytes / (1024**2):.2f} MB"
                    else:
                         mem_msg = f"Avg Available Mem: {avg_available_memory_bytes / 1024:.2f} KB"
                else:
                    mem_msg = "Avg Available Mem: N/A"
                log_msg += f" ({cpu_msg}, {mem_msg})"
                
                # Flag based ONLY on CPU threshold now
                vm_details = {
                    "name": vm.name, "id": vm.id, "resource_group": vm.id.split('/')[4],
                    "location": vm.location, "avg_cpu_percent": avg_cpu,
                    "avg_available_memory_bytes": avg_available_memory_bytes # Store available bytes
                }
                flagged = False
                if avg_cpu is not None and avg_cpu < cpu_threshold_percent:
                    low_cpu_vms.append(vm_details)
                    log_msg += " - LOW CPU" 
                    flagged = True
                
                if not flagged:
                     log_msg += " - OK" 
                     
                print(log_msg)

            except Exception as metric_error:
                print(f"  - Warning: Could not get metrics for VM {vm.name}. Error: {metric_error}")

        print("\n--- VM Usage Analysis Summary ---")
        if not low_cpu_vms:
            print(f"  - No VMs found with avg CPU < {cpu_threshold_percent}%.")
        else:
            print(f"  - Low CPU VMs (< {cpu_threshold_percent}%): {len(low_cpu_vms)}")

        return low_cpu_vms # Return only low_cpu_vms list

    except Exception as e:
        print(f"Error checking for underutilized VMs: {e}")
        return [] # Return one empty list

# --- Add Low Usage App Service Plan Detection Function ---
def find_low_usage_app_service_plans(credential, subscription_id, 
                                     cpu_threshold_percent=APP_SERVICE_PLAN_LOW_CPU_THRESHOLD_PERCENT, 
                                     lookback_days=METRIC_LOOKBACK_DAYS):
    """Finds App Service Plans (Basic tier+) with low average CPU usage."""
    low_cpu_plans = []
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=lookback_days)
    
    time_format = "%Y-%m-%dT%H:%M:%SZ"
    start_time_str = start_time.strftime(time_format)
    end_time_str = now.strftime(time_format)
    timespan = f"{start_time_str}/{end_time_str}"
    interval = "PT1H" 

    try:
        web_client = WebSiteManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)
        print(f"\nChecking App Service Plans (Basic tier+) for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...")
        print(f"  - Querying metrics with timespan: {timespan}")

        plans_to_check = []
        for plan in web_client.app_service_plans.list():
            # Filter out Free/Shared plans - focus on paid tiers (Basic, Standard, Premium, etc.)
            if plan.sku and plan.sku.tier and plan.sku.tier.lower() not in ['free', 'shared', 'dynamic']: # Dynamic is Consumption for Functions
                 plans_to_check.append(plan)
            else:
                 print(f"  - Skipping plan {plan.name} (Tier: {plan.sku.tier if plan.sku else 'Unknown'}) - Not checking metrics for Free/Shared/Dynamic tiers.")

        if not plans_to_check:
            print("  - No App Service Plans found in relevant tiers (Basic+) to check metrics for.")
            return []
        else:
             print(f"  - Found {len(plans_to_check)} App Service Plans in relevant tiers to analyze...")

        # --- Query Metrics for Relevant Plans --- 
        for plan in plans_to_check:
            plan_resource_uri = plan.id
            avg_cpu = None
            try:
                # Query the 'CpuPercentage' metric for App Service Plans
                metric_names = "CpuPercentage"
                metrics_data = monitor_client.metrics.list(
                    resource_uri=plan_resource_uri,
                    timespan=timespan,
                    interval=interval,
                    metricnames=metric_names,
                    aggregation="Average"
                )

                # Process results 
                if metrics_data.value:
                     # ASP metrics might have slightly different structure
                     if metrics_data.value[0].timeseries:
                         time_series = metrics_data.value[0].timeseries
                         if time_series and time_series[0].data:
                             total_cpu = 0
                             count = 0
                             for point in time_series[0].data:
                                 if point.average is not None: 
                                     total_cpu += point.average
                                     count += 1
                             if count > 0:
                                 avg_cpu = total_cpu / count
                
                log_msg = f"  - Analyzed ASP: {plan.name}"
                cpu_msg = f"Avg CPU: {avg_cpu:.2f}%" if avg_cpu is not None else "Avg CPU: N/A"
                log_msg += f" ({cpu_msg})"
                
                # Flag based on threshold
                if avg_cpu is not None and avg_cpu < cpu_threshold_percent:
                    rg_name = plan.id.split('/')[4]
                    low_cpu_plans.append({
                        "name": plan.name, "id": plan.id, "resource_group": rg_name,
                        "location": plan.location, "avg_cpu_percent": avg_cpu,
                        "sku": plan.sku.name if plan.sku else "Unknown"
                    })
                    log_msg += f" - LOW CPU (< {cpu_threshold_percent}%)"
                else:
                    log_msg += " - OK"
                    
                print(log_msg)

            except Exception as metric_error:
                # Handle cases where metrics might not be available (e.g., new plan, specific SKUs?)
                print(f"  - Warning: Could not get CPU metrics for App Service Plan {plan.name}. Error: {metric_error}")
                # Check for common errors like unsupported metrics for the SKU
                if "metric is not supported" in str(metric_error).lower():
                     print(f"    (Metric 'CpuPercentage' might not be supported for SKU {plan.sku.name if plan.sku else 'Unknown'} on this plan)")

        print("\n--- App Service Plan Usage Analysis Summary ---")
        if not low_cpu_plans:
            print(f"  - No App Service Plans found with avg CPU < {cpu_threshold_percent}%.")
        else:
            print(f"  - Low CPU App Service Plans (< {cpu_threshold_percent}%): {len(low_cpu_plans)}")

        return low_cpu_plans

    except Exception as e:
        print(f"Error checking for low usage App Service Plans: {e}")
        return []

# --- Add Low DTU SQL DB Detection Function ---
def find_low_dtu_sql_databases(credential, subscription_id,
                               dtu_threshold_percent=SQL_DB_LOW_DTU_THRESHOLD_PERCENT,
                               lookback_days=METRIC_LOOKBACK_DAYS):
    """Finds DTU-based SQL Databases with low average DTU consumption."""
    low_dtu_dbs = []
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=lookback_days)
    time_format = "%Y-%m-%dT%H:%M:%SZ"
    start_time_str = start_time.strftime(time_format)
    end_time_str = now.strftime(time_format)
    timespan = f"{start_time_str}/{end_time_str}"
    interval = "PT1H"

    try:
        sql_client = SqlManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)
        print(f"\nChecking SQL Databases (DTU-based) for avg DTU < {dtu_threshold_percent}% over the last {lookback_days} days...")
        print(f"  - Querying metrics with timespan: {timespan}")

        dbs_to_check = []
        # Need to list servers first, then databases within each server
        print("  - Listing SQL servers and databases...")
        for server in sql_client.servers.list():
            rg_name = server.id.split('/')[4]
            server_name = server.name
            try:
                for db in sql_client.databases.list_by_server(rg_name, server_name):
                    # Check if it's a DTU-based model (current.sku.name is not None and tier is not ElasticPool/Hyperscale/Serverless)
                    # Focus on provisioned DTU models; others have different metrics/scaling
                    # Simple check: look for non-vCore skus (like S0, S1, P1 etc.) or Basic/Standard/Premium tiers
                    # This filtering might need refinement based on exact SKU naming conventions
                    if db.sku and db.sku.tier and db.sku.tier.lower() in ['basic', 'standard', 'premium'] and not db.kind.lower().startswith('vcore'):
                        # Check if database is online
                         if db.status == "Online":
                              dbs_to_check.append(db)
                         else:
                              print(f"  - Skipping DB {db.name} on server {server_name} (Status: {db.status})")
                    else:
                        tier_name = f"{db.sku.tier}/{db.sku.name}" if db.sku else "Unknown"
                        print(f"  - Skipping DB {db.name} on server {server_name} (Tier: {tier_name}) - Not DTU-based or not provisioned?")
            except Exception as db_list_error:
                 print(f"  - Warning: Could not list databases for server {server_name}. Error: {db_list_error}")

        if not dbs_to_check:
            print("  - No online, DTU-based SQL Databases found to check metrics for.")
            return []
        else:
             print(f"  - Found {len(dbs_to_check)} DTU-based SQL Databases to analyze...")

        # --- Query Metrics for Relevant DBs ---
        for db in dbs_to_check:
            db_resource_uri = db.id
            avg_dtu_percent = None
            try:
                metric_names = "dtu_consumption_percent"
                metrics_data = monitor_client.metrics.list(
                    resource_uri=db_resource_uri,
                    timespan=timespan,
                    interval=interval,
                    metricnames=metric_names,
                    aggregation="Average"
                )

                if metrics_data.value and metrics_data.value[0].timeseries and metrics_data.value[0].timeseries[0].data:
                    time_series = metrics_data.value[0].timeseries[0].data
                    total_dtu = 0
                    count = 0
                    for point in time_series:
                        if point.average is not None:
                            total_dtu += point.average
                            count += 1
                    if count > 0:
                        avg_dtu_percent = total_dtu / count

                log_msg = f"  - Analyzed DB: {db.name}"
                dtu_msg = f"Avg DTU: {avg_dtu_percent:.2f}%" if avg_dtu_percent is not None else "Avg DTU: N/A"
                log_msg += f" ({dtu_msg})"

                if avg_dtu_percent is not None and avg_dtu_percent < dtu_threshold_percent:
                    rg_name = db.id.split('/')[4]
                    low_dtu_dbs.append({
                        "name": db.name,
                        "id": db.id,
                        "resource_group": rg_name,
                        "server_name": db.id.split('/')[8], # Extract server name
                        "location": db.location,
                        "avg_dtu_percent": avg_dtu_percent,
                        "sku": f"{db.sku.tier} {db.sku.name}" if db.sku else "Unknown"
                    })
                    log_msg += f" - LOW DTU (< {dtu_threshold_percent}%)"
                else:
                    log_msg += " - OK"
                    
                print(log_msg)

            except Exception as metric_error:
                print(f"  - Warning: Could not get DTU metrics for DB {db.name}. Error: {metric_error}")

        print("\n--- SQL Database Usage Analysis Summary ---")
        if not low_dtu_dbs:
            print(f"  - No SQL Databases found with avg DTU < {dtu_threshold_percent}%.")
        else:
            print(f"  - Low DTU SQL Databases (< {dtu_threshold_percent}%): {len(low_dtu_dbs)}")

        return low_dtu_dbs

    except Exception as e:
        print(f"Error checking for low DTU SQL Databases: {e}")
        return []

# --- Add HTML Report Function --- 
def generate_html_report_content(
    findings,
    cost_data,
    unattached_disks,
    stopped_vms,
    unused_public_ips,
    empty_resource_groups,
    low_usage_app_service_plans,
    low_usage_apps_df,
    orphaned_nsgs_df, # Added param
    orphaned_route_tables_df, # Added param
    potential_savings,
    total_potential_savings,
    cost_breakdown,
    ignored_resources,
    include_ignored
):
    """Generates the report content as an HTML string with improved styling."""
    import pandas as pd
    import datetime

    # --- Helper function to convert DataFrame to HTML table within a Bootstrap Card ---
    def df_to_html_card(df, title, card_id, icon_class="bi-question-circle"):
        # Use Bootstrap Icons (requires adding the CDN link)
        card_header = f"<h5 class=\"mb-0\"><i class=\"{icon_class} me-2\"></i>{title}</h5>"
        
        if df is None or df.empty:
            card_body = f"<p class=\"card-text\"><em>None found.</em></p>"
        else:
            # Use Bootstrap table classes for better appearance
            # Add 'table-responsive' for smaller screens
            # Add 'table-bordered' for clearer cell separation
            # Add 'table-sm' for more compact tables
            table_html = df.to_html(index=False, classes='table table-striped table-hover table-bordered table-sm', border=0)
            card_body = f"<div class=\"table-responsive\">{table_html}</div>"

        # Corrected f-string for the card HTML structure
        card = f"""
        <div class=\"card mb-4 shadow-sm\" id=\"{card_id}\">
            <div class=\"card-header\">{card_header}</div>
            <div class=\"card-body\">{card_body}</div>
        </div>
        """
        return card

    # --- Start HTML document ---
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Azure Cost Optimization Report</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.5/font/bootstrap-icons.css">
    <style>
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            padding: 20px;
            background-color: #f8f9fa; /* Light gray background */
        }}
        h1 {{ 
            color: #0d6efd; /* Bootstrap primary blue */
            border-bottom: 3px solid #0d6efd;
            padding-bottom: 15px;
            margin-bottom: 30px;
            text-align: center;
        }}
        .summary-card {{ 
            background-color: #ffffff;
            border: 1px solid #dee2e6;
            border-radius: .375rem; 
            padding: 2rem;
            margin-bottom: 30px; 
            text-align: center;
            box-shadow: 0 .125rem .25rem rgba(0,0,0,.075);
        }}
        .summary-card h2 {{
             margin-bottom: 1.5rem;
             color: #6c757d; /* Bootstrap secondary text color */
        }}
        .potential-savings {{
            font-size: 2.5rem;
            font-weight: 500;
            color: #198754; /* Bootstrap success green */
        }}
        .card-header {{
            background-color: #e9ecef; /* Lighter header background */
            border-bottom: 1px solid #dee2e6;
            font-weight: 500;
        }}
        .footer {{
             margin-top: 40px; 
             font-size: 0.9em; 
             color: #6c757d; 
             text-align: center; 
        }}
        /* Ensure responsiveness */
        .table-responsive {{ margin-bottom: 0; }}
    </style>
</head>
<body>
    <div class="container">
        <h1><i class="bi bi-cloud-check-fill me-2"></i>Azure Cost Optimization Report</h1>
        <p class="text-center text-muted mb-4">Generated on: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>

        <div class="summary-card">
            <h2>Total Potential Monthly Savings</h2>
            <p class="potential-savings">${total_potential_savings:.2f}</p>
            <p class="text-muted">(Note: Savings estimates might be based on placeholders or simplified calculations)</p>
        </div>

        <h2><i class="bi bi-search me-2"></i>Findings & Recommendations</h2>
    """

    # --- Add findings sections using cards ---
    # Assigning icons for different categories
    html += df_to_html_card(unattached_disks, "Unattached Disks", "unattached-disks", "bi-hdd-stack")
    html += df_to_html_card(stopped_vms, "Stopped VMs (Consider Deallocation)", "stopped-vms", "bi-stop-circle")
    html += df_to_html_card(unused_public_ips, "Unused Public IPs", "unused-ips", "bi-globe2")
    html += df_to_html_card(empty_resource_groups, "Empty Resource Groups", "empty-rgs", "bi-trash3")
    html += df_to_html_card(low_usage_app_service_plans, "Low Usage App Service Plans", "low-asps", "bi-bar-chart-line")
    html += df_to_html_card(low_usage_apps_df, "Low CPU Web Apps", "low-webapps", "bi-browser-chrome") # Placeholder icon
    # Placeholder DataFrames for orphaned resources for now
    # html += df_to_html_card(orphaned_nsgs_df, "Orphaned Network Security Groups", "orphaned-nsgs", "bi-shield-slash") 
    # html += df_to_html_card(orphaned_route_tables_df, "Orphaned Route Tables", "orphaned-rts", "bi-map") 
    
    # Add Cost Breakdown Card
    cost_breakdown_df = pd.DataFrame(list(cost_breakdown.items()), columns=['Resource Type', 'Estimated Cost']) if cost_breakdown else pd.DataFrame()
    if not cost_breakdown_df.empty:
         cost_breakdown_df['Estimated Cost'] = cost_breakdown_df['Estimated Cost'].apply(lambda x: f"${x:.2f}")
    html += df_to_html_card(cost_breakdown_df, "Cost Breakdown by Resource Type (Monthly Estimate)", "cost-breakdown", "bi-currency-dollar")

    # Add Potential Savings Breakdown Card
    savings_breakdown_df = pd.DataFrame(list(potential_savings.items()), columns=['Category', 'Potential Savings']) if potential_savings else pd.DataFrame()
    if not savings_breakdown_df.empty:
         savings_breakdown_df['Potential Savings'] = savings_breakdown_df['Potential Savings'].apply(lambda x: f"${x:.2f}")
         # Filter out $0.00 savings for cleaner display
         savings_breakdown_df = savings_breakdown_df[savings_breakdown_df['Potential Savings'] != '$0.00']
    html += df_to_html_card(savings_breakdown_df, "Potential Savings Breakdown (Monthly Estimate)", "savings-breakdown", "bi-graph-up-arrow")

    # Add Ignored Resources section (if applicable)
    if include_ignored and ignored_resources is not None and not ignored_resources.empty:
         html += df_to_html_card(ignored_resources, "Ignored Resources", "ignored-resources", "bi-eye-slash")

    # --- End HTML document ---
    html += f"""
        <div class="footer">
            <p>Report generated by Azure Cost Optimization Script.</p>
        </div>
    </div> <!-- Closing container -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
    """
    return html

def write_html_report(html_content, filename):
    """Writes the HTML content to a file."""
    # Get the logger instance
    import logging
    logger = logging.getLogger()
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logger.info(f"HTML report successfully written to {filename}")
        return True
    except IOError as e:
        logger.error(f"Error writing HTML report to {filename}: {e}")
        return False

def generate_summary_report(
    unattached_disks, stopped_vms, unused_public_ips, empty_rgs, empty_plans, old_snapshots, 
    low_cpu_vms, low_cpu_plans,
    low_dtu_dbs,
    costs_by_type, total_cost, currency, output_csv_file=None):
    """Generates console/CSV summary report, estimates specific costs for unattached disks."""
    
    # --- Build report_lines for console/email --- 
    report_lines = []
    report_lines.append("--- Azure Cost Optimization Summary Report ---")

    # Cost Summary
    if total_cost is not None:
        report_lines.append(f"\n💰 Total Estimated Cost (Month-to-Date): {total_cost:.2f} {currency}")
    if costs_by_type:
        report_lines.append("\n  Cost Breakdown by Resource Type (Top 5):")
        sorted_costs = sorted(costs_by_type.items(), key=lambda item: item[1], reverse=True)
        for i, (res_type, cost) in enumerate(sorted_costs):
            if i >= 5: break
            report_lines.append(f"    - {res_type}: {cost:.2f} {currency}")
        if len(sorted_costs) > 5: report_lines.append("    - ...")

    has_findings = any([unattached_disks, stopped_vms, unused_public_ips, empty_rgs, empty_plans, old_snapshots, low_cpu_vms, low_cpu_plans, low_dtu_dbs])

    if not has_findings:
        report_lines.append("\n✅ No immediate cost optimization opportunities or cleanup suggestions found based on current checks.")
    else:
        report_lines.append("\nPotential cost savings / cleanup suggestions identified:")
        
        # --- Unattached Disks Section (with Pricing) --- 
        if unattached_disks:
            disk_cost_total = costs_by_type.get('microsoft.compute/disks', 0.0)
            report_lines.append(f"\n➡️ Unattached Disks ({len(unattached_disks)}): (Total Disk Costs MTD: {disk_cost_total:.2f} {currency})")
            recommendation_base = f"Delete if no longer needed. Part of {disk_cost_total:.2f} {currency} monthly disk costs."
            
            for disk in unattached_disks:
                estimated_cost_str = "(Cost N/A)"
                recommendation = recommendation_base # Default
                if disk.get('pricing_sku') and disk.get('pricing_arm_region'):
                    # Construct filter - This is tricky and may need refinement!
                    filter_str = f"serviceFamily eq 'Storage' and serviceName eq 'Managed Disks' and armRegionName eq '{disk['pricing_arm_region']}' and skuName eq '{disk['pricing_sku']}' and contains(meterName, 'Disk')"
                    price_info = get_retail_price(filter_str)
                    monthly_cost, cost_unit = estimate_monthly_cost(price_info)
                    
                    if monthly_cost is not None:
                         # Adjust for disk size if price is per GB/Month
                         if price_info and 'gb' in price_info.get('unitOfMeasure','').lower():
                              monthly_cost *= disk['size_gb']
                         estimated_cost_str = f"(Est. ~{monthly_cost:.2f} {cost_unit})"
                         recommendation = f"Delete to save ~{monthly_cost:.2f} {cost_unit}. {recommendation_base}" 
                    else:
                         estimated_cost_str = "(Cost Lookup Failed)"
                else:
                     estimated_cost_str = "(Pricing Info Missing)"
                
                # Add to console/email report lines
                report_lines.append(f"  - {disk['name']} {estimated_cost_str} (RG: {disk['resource_group']}, Size: {disk['size_gb']}GB, SKU: {disk['sku']})" )
                # Store details for CSV
                disk['estimated_cost_str'] = estimated_cost_str # Store for CSV
                disk['recommendation_specific'] = recommendation # Store for CSV
            
            report_lines.append(f"  * Recommendation: {recommendation_base}") # General recommendation for the section

        # --- Other Findings Sections (Without Specific Pricing Yet) --- 
        if stopped_vms:
            vm_disk_cost = costs_by_type.get('microsoft.compute/disks', 0.0)
            report_lines.append(f"\n➡️ Stopped VMs ({len(stopped_vms)}): (Associated disks part of {vm_disk_cost:.2f} {currency} monthly disk costs)")
            for vm in stopped_vms:
                report_lines.append(f"  - {vm['name']} (RG: {vm['resource_group']})")
            report_lines.append(f"  * Recommendation: Deallocate if temporarily unused, or delete if no longer needed.")
        
        if unused_public_ips:
            ip_cost = costs_by_type.get('microsoft.network/publicipaddresses', 0.0)
            report_lines.append(f"\n➡️ Unused Public IPs ({len(unused_public_ips)}): (Total Public IP Costs MTD: {ip_cost:.2f} {currency})")
            for ip in unused_public_ips:
                report_lines.append(f"  - {ip['name']} (RG: {ip['resource_group']}, IP: {ip['ip_address']}, SKU: {ip['sku']})")
            report_lines.append(f"  * Recommendation: Delete if no longer needed. Part of {ip_cost:.2f} {currency} monthly Public IP costs.")
        
        if empty_rgs:
            report_lines.append(f"\n➡️ Empty Resource Groups ({len(empty_rgs)}):")
            for rg in empty_rgs:
                report_lines.append(f"  - {rg['name']} (Location: {rg['location']})")
            report_lines.append(f"  * Recommendation: Delete if confirmed empty and no longer needed.")
        
        if empty_plans:
            plan_cost = costs_by_type.get('microsoft.web/serverfarms', 0.0)
            report_lines.append(f"\n➡️ Empty App Service Plans ({len(empty_plans)}):")
            for plan in empty_plans:
                report_lines.append(f"  - {plan['name']} (RG: {plan['resource_group']}, SKU: {plan['sku']}, Location: {plan['location']})")
            report_lines.append(f"  * Recommendation: Delete if no longer needed. Part of {plan_cost:.2f} {currency} monthly App Service Plan costs.")
        
        if old_snapshots:
            snapshot_cost = costs_by_type.get('microsoft.compute/snapshots', 0.0)
            report_lines.append(f"\n➡️ Old Disk Snapshots (> {SNAPSHOT_AGE_THRESHOLD_DAYS} days) ({len(old_snapshots)}): (Total Snapshot Costs MTD: {snapshot_cost:.2f} {currency})")
            for snap in old_snapshots:
                report_lines.append(f"  - {snap['name']} (RG: {snap['resource_group']}, Created: {datetime.fromisoformat(snap['time_created']).strftime('%Y-%m-%d')}, Size: {snap['size_gb']}GB)")
            report_lines.append(f"  * Recommendation: Delete if no longer required. Part of {snapshot_cost:.2f} {currency} monthly snapshot costs.")
        
        if low_cpu_vms:
            vm_cost = costs_by_type.get('microsoft.compute/virtualmachines', 0.0)
            report_lines.append(f"\n➡️ Low CPU VMs (< {LOW_CPU_THRESHOLD_PERCENT}%) ({len(low_cpu_vms)}): (Total VM Costs MTD: {vm_cost:.2f} {currency})")
            recommendation = f"Consider resizing to a smaller instance type. Analyze Avg Available Memory and workload patterns before resizing."
            for vm in low_cpu_vms:
                cpu_str = f"{vm['avg_cpu_percent']:.2f}%" if vm['avg_cpu_percent'] is not None else "N/A"
                mem_bytes = vm['avg_available_memory_bytes']
                mem_str = "N/A"
                if mem_bytes is not None:
                    if mem_bytes > (1024**3): mem_str = f"{mem_bytes / (1024**3):.2f} GB"
                    elif mem_bytes > (1024**2): mem_str = f"{mem_bytes / (1024**2):.2f} MB"
                    else: mem_str = f"{mem_bytes / 1024:.2f} KB"
                details = f"Avg CPU: {cpu_str}, Avg Available Mem: {mem_str} (Last {METRIC_LOOKBACK_DAYS} days), Loc: {vm['location']}"
                report_lines.append(f"  - Name: {vm['name']}, RG: {vm['resource_group']}, {details} [Low CPU]")
            report_lines.append(f"  * Recommendation: {recommendation}")
        
        if low_cpu_plans:
            plan_cost = costs_by_type.get('microsoft.web/serverfarms', 0.0) 
            report_lines.append(f"\n➡️ Low CPU App Service Plans (< {APP_SERVICE_PLAN_LOW_CPU_THRESHOLD_PERCENT}%) ({len(low_cpu_plans)}):")
            for plan in low_cpu_plans:
                report_lines.append(f"  - {plan['name']} (RG: {plan['resource_group']}, Avg CPU: {plan['avg_cpu_percent']:.2f}%, SKU: {plan['sku']})")
            report_lines.append(f"  * Recommendation: Consider scaling down the App Service Plan SKU or consolidating apps. Analyze workload patterns.")
        
        if low_dtu_dbs:
            sql_cost = costs_by_type.get('microsoft.sql/servers/databases', 0.0) 
            report_lines.append(f"\n➡️ Low DTU SQL Databases (< {SQL_DB_LOW_DTU_THRESHOLD_PERCENT}%) ({len(low_dtu_dbs)}):")
            for db in low_dtu_dbs:
                report_lines.append(f"  - {db['name']} (RG: {db['resource_group']}, Avg DTU: {db['avg_dtu_percent']:.2f}%, SKU: {db['sku']})")
            report_lines.append(f"  * Recommendation: Consider scaling down the database tier (DTUs). Analyze query performance and peak load before changing.")

    # --- Generate Console Output --- 
    console_output = "\n".join(report_lines)
    print(console_output)
    
    # --- Handle CSV Output --- 
    if output_csv_file: 
        report_data_for_csv = [] 
        if has_findings:
            # --- Populate report_data_for_csv --- 
            if unattached_disks:
                for disk in unattached_disks:
                    details = f"Size: {disk['size_gb']}GB, SKU: {disk['sku']}, Location: {disk['location']} {disk.get('estimated_cost_str', '(Cost N/A)')}"
                    recommendation = disk.get('recommendation_specific', "Delete if no longer needed.")
                    report_data_for_csv.append(["Unattached Disk", disk['name'], disk['resource_group'], details, recommendation])
            
            # Stopped VMs
            if stopped_vms:
                 recommendation = "Deallocate if temporarily unused, or delete if no longer needed."
                 for vm in stopped_vms:
                     details = f"Location: {vm['location']}"
                     report_data_for_csv.append(["Stopped VM", vm['name'], vm['resource_group'], details, recommendation])
            
            # Unused Public IPs
            if unused_public_ips:
                 ip_cost = costs_by_type.get('microsoft.network/publicipaddresses', 0.0)
                 recommendation = f"Delete if no longer needed. Part of {ip_cost:.2f} {currency} monthly Public IP costs."
                 # TODO: Add specific cost estimation for IPs later
                 for ip in unused_public_ips:
                     details = f"IP: {ip['ip_address']}, SKU: {ip['sku']}, Location: {ip['location']}"
                     report_data_for_csv.append(["Unused Public IP", ip['name'], ip['resource_group'], details, recommendation])

            # Empty Resource Groups
            if empty_rgs:
                recommendation = "Delete if confirmed empty and no longer needed."
                for rg in empty_rgs:
                    details = f"Location: {rg['location']}"
                    report_data_for_csv.append(["Empty Resource Group", rg['name'], rg['name'], details, recommendation]) # RG name in both name/RG columns

            # Empty App Service Plans
            if empty_plans:
                plan_cost = costs_by_type.get('microsoft.web/serverfarms', 0.0)
                recommendation = f"Delete if no longer needed. Part of {plan_cost:.2f} {currency} monthly App Service Plan costs."
                # TODO: Add specific cost estimation for ASPs later
                for plan in empty_plans:
                    details = f"SKU: {plan['sku']}, Location: {plan['location']}"
                    report_data_for_csv.append(["Empty App Service Plan", plan['name'], plan['resource_group'], details, recommendation])

            # Old Disk Snapshots
            if old_snapshots:
                snapshot_cost = costs_by_type.get('microsoft.compute/snapshots', 0.0)
                recommendation = f"Delete if no longer required. Part of {snapshot_cost:.2f} {currency} monthly snapshot costs."
                # TODO: Add specific cost estimation for Snapshots later
                for snap in old_snapshots:
                    details = f"Created: {datetime.fromisoformat(snap['time_created']).strftime('%Y-%m-%d')}, Size: {snap['size_gb']}GB, Loc: {snap['location']}"
                    report_data_for_csv.append(["Old Disk Snapshot", snap['name'], snap['resource_group'], details, recommendation])

            # Low CPU VMs (Already implemented)
            if low_cpu_vms:
                 recommendation = "Consider resizing. Analyze workload patterns."
                 for vm in low_cpu_vms:
                     cpu_str = f"{vm['avg_cpu_percent']:.2f}%" if vm['avg_cpu_percent'] is not None else "N/A"
                     mem_bytes = vm['avg_available_memory_bytes']
                     mem_str = "N/A"
                     if mem_bytes is not None:
                         if mem_bytes > (1024**3): mem_str = f"{mem_bytes / (1024**3):.2f} GB"
                         elif mem_bytes > (1024**2): mem_str = f"{mem_bytes / (1024**2):.2f} MB"
                         else: mem_str = f"{mem_bytes / 1024:.2f} KB"
                     details = f"Avg CPU: {cpu_str}, Avg Available Mem: {mem_str} (Last {METRIC_LOOKBACK_DAYS} days), Loc: {vm['location']}"
                     report_data_for_csv.append(["Low CPU VM", vm['name'], vm['resource_group'], details, recommendation])
            
            # Low CPU App Service Plans
            if low_cpu_plans:
                recommendation = f"Consider scaling down the App Service Plan SKU or consolidating apps. Analyze workload patterns."
                for plan in low_cpu_plans:
                    details = f"Avg CPU: {plan['avg_cpu_percent']:.2f}%, SKU: {plan['sku']}, Loc: {plan['location']}"
                    report_data_for_csv.append(["Low CPU App Service Plan", plan['name'], plan['resource_group'], details, recommendation])

            # Low DTU SQL Databases
            if low_dtu_dbs:
                recommendation = f"Consider scaling down the database tier (DTUs). Analyze query performance and peak load before changing."
                for db in low_dtu_dbs:
                    details = f"Avg DTU: {db['avg_dtu_percent']:.2f}%, SKU: {db['sku']}, Server: {db['server_name']}, Loc: {db['location']}"
                    report_data_for_csv.append(["Low DTU SQL DB", db['name'], db['resource_group'], details, recommendation])

        # --- Write CSV --- 
        try:
            with open(output_csv_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Finding Type", "Name", "Resource Group", "Details", "Recommendation"])
                if report_data_for_csv:
                    writer.writerows(report_data_for_csv)
                elif not has_findings: 
                     writer.writerow(["Summary", "-", "-", "No findings based on current checks", "-"])
            print(f"\n📄 Summary report successfully written to: {output_csv_file}")
        except Exception as e:
            print(f"\n⚠️ Error writing CSV report to {output_csv_file}: {e}")

    return console_output # Return the content used for console/email

# --- Add Email Sending Function ---
def send_email_report(report_content):
    """Sends the report content via email using environment variables for config."""
    print("\n📧 Attempting to send email report...")
    # Get config from environment variables
    smtp_host = os.environ.get('SMTP_HOST')
    smtp_port_str = os.environ.get('SMTP_PORT')
    smtp_user = os.environ.get('SMTP_USER')
    smtp_password = os.environ.get('SMTP_PASSWORD')
    sender_email = os.environ.get('EMAIL_SENDER')
    recipient_emails_str = os.environ.get('EMAIL_RECIPIENT')

    # Basic validation
    if not all([smtp_host, smtp_port_str, smtp_user, smtp_password, sender_email, recipient_emails_str]):
        print("  - ⚠️ Email configuration incomplete. Missing one or more environment variables:")
        print("     SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, EMAIL_SENDER, EMAIL_RECIPIENT")
        print("  - Email not sent.")
        return False

    try:
        smtp_port = int(smtp_port_str)
        recipients = [email.strip() for email in recipient_emails_str.split(',')]

        msg = EmailMessage()
        msg['Subject'] = f"Azure Cost Optimization Report - {datetime.now().strftime('%Y-%m-%d')}"
        msg['From'] = sender_email
        msg['To'] = ", ".join(recipients)
        msg.set_content(report_content)

        # Connect and send (adapt based on port for SSL/TLS)
        if smtp_port == 465:
            # Use SMTP_SSL for port 465
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
        else:
            # Assume TLS for other ports (like 587)
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls() # Secure the connection
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
        
        print(f"  - ✅ Email report successfully sent to: {', '.join(recipients)}")
        return True

    except ValueError:
        print(f"  - ⚠️ Invalid SMTP_PORT: '{smtp_port_str}'. Must be an integer.")
        print("  - Email not sent.")
        return False
    except smtplib.SMTPAuthenticationError:
         print(f"  - ❌ SMTP Authentication Failed for user '{smtp_user}'. Check credentials (SMTP_USER/SMTP_PASSWORD).")
         print("  - Email not sent.")
         return False
    except Exception as e:
        print(f"  - ❌ Failed to send email: {e}")
        return False

# --- Action Functions (Add Logging) ---
def delete_resource(credential, subscription_id, resource_id, resource_type, resource_name, clients, wait_for_completion=False):
    """Generic function to delete a resource after confirmation using appropriate client."""
    logger = logging.getLogger() # Get logger instance
    confirm = input(f"❓ Delete {resource_type} '{resource_name}' ({resource_id})? [y/N]: ").lower()
    if confirm == 'y':
        log_prefix = f"ACTION - DELETE - {resource_type} - {resource_name} ({resource_id})"
        logger.info(f"{log_prefix}: User confirmed deletion.")
        logger.info(f"{log_prefix}: Attempting deletion...") # Use logger
        poller = None
        try:
            rg_name = resource_id.split('/')[4] # Extract RG for most resources

            if resource_type == "Unattached Disk":
                poller = clients['compute'].disks.begin_delete(rg_name, resource_name)
            elif resource_type == "Unused Public IP":
                poller = clients['network'].public_ip_addresses.begin_delete(rg_name, resource_name)
            elif resource_type == "Empty Resource Group":
                poller = clients['resource'].resource_groups.begin_delete(resource_name)
            elif resource_type == "Empty App Service Plan":
                poller = clients['web'].app_service_plans.begin_delete(rg_name, resource_name)
            elif resource_type == "Old Disk Snapshot":
                 poller = clients['compute'].snapshots.begin_delete(rg_name, resource_name)
            else:
                logger.warning(f"{log_prefix}: Deletion logic for resource type '{resource_type}' not implemented.") # Use logger
                return False

            logger.info(f"{log_prefix}: Deletion initiated. Initial state: {poller.status()}")

            if wait_for_completion and poller:
                logger.info(f"{log_prefix}: Waiting for completion...") # Use logger
                start_wait = time.time()
                poller.result() # This blocks until completion
                end_wait = time.time()
                wait_duration = end_wait - start_wait
                logger.info(f"{log_prefix}: Deletion completed. Final State: {poller.status()} (Wait time: {wait_duration:.2f}s)") # Use logger
            elif poller:
                 logger.info(f"{log_prefix}: Deletion initiated, not waiting for completion (poller status: {poller.status()}).") # Use logger

            return True
        except ResourceNotFoundError:
             log_msg = f"{log_prefix}: Resource not found (perhaps already deleted?)."
             logger.warning(log_msg) # Use logger
             return False
        except Exception as e:
            final_status = f"failed ({e})" if not poller else f"{poller.status()} ({e})"
            log_msg = f"{log_prefix}: Error during deletion process. Final status: {final_status}"
            logger.error(log_msg, exc_info=True) # Use logger
            return False
    else:
        logger.info(f"ACTION - SKIP DELETE - {resource_type} '{resource_name}' ({resource_id}).") # Use logger
        return False

def deallocate_vm(credential, subscription_id, rg_name, vm_name, compute_client, wait_for_completion=False):
    """Deallocates a VM after confirmation."""
    logger = logging.getLogger() # Get logger instance
    confirm = input(f"❓ Deallocate VM '{vm_name}' in RG '{rg_name}'? (Keeps disks) [y/N]: ").lower()
    if confirm == 'y':
        log_prefix = f"ACTION - DEALLOCATE - VM - {vm_name} (RG: {rg_name})"
        logger.info(f"{log_prefix}: User confirmed deallocation.")
        logger.info(f"{log_prefix}: Attempting deallocation...") # Use logger
        poller = None
        try:
            poller = compute_client.virtual_machines.begin_deallocate(rg_name, vm_name)
            logger.info(f"{log_prefix}: Deallocation initiated. Initial state: {poller.status()}") # Use logger

            if wait_for_completion and poller:
                logger.info(f"{log_prefix}: Waiting for completion...") # Use logger
                start_wait = time.time()
                poller.result() # Blocks until complete
                end_wait = time.time()
                wait_duration = end_wait - start_wait
                logger.info(f"{log_prefix}: Deallocation completed. Final State: {poller.status()} (Wait time: {wait_duration:.2f}s)") # Use logger
            elif poller:
                 logger.info(f"{log_prefix}: Deallocation initiated, not waiting for completion (poller status: {poller.status()}).") # Use logger

            return True
        except ResourceNotFoundError:
             log_msg = f"{log_prefix}: VM not found (perhaps already deleted/deallocated?)."
             logger.warning(log_msg) # Use logger
             return False
        except Exception as e:
            final_status = f"failed ({e})" if not poller else f"{poller.status()} ({e})"
            log_msg = f"{log_prefix}: Error during deallocation process. Final status: {final_status}"
            logger.error(log_msg, exc_info=True) # Use logger
            return False
    else:
        logger.info(f"ACTION - SKIP DEALLOCATE - VM '{vm_name}' (RG: {rg_name}).") # Use logger
        return False

# --- Update Cleanup Logic Function ---
def perform_interactive_cleanup(credential, subscription_id, unattached_disks, stopped_vms, unused_public_ips, empty_rgs, empty_plans, old_snapshots, wait_for_completion=False):
    """Iterates through findings and prompts for cleanup actions, using initialized clients."""
    print("\n--- Interactive Cleanup --- ")
    print("⚠️ You will be prompted to confirm each action. Deletions may take time. ⚠️")
    if wait_for_completion:
        print("⏳ Waiting for each cleanup operation to complete...")

    # Initialize clients needed for actions
    clients = {
        'resource': ResourceManagementClient(credential, subscription_id),
        'compute': ComputeManagementClient(credential, subscription_id),
        'network': NetworkManagementClient(credential, subscription_id),
        'web': WebSiteManagementClient(credential, subscription_id)
    }

    # Pass wait_for_completion flag to action functions
    if unattached_disks:
        print("\n🧹 Checking Unattached Disks for cleanup...")
        for disk in list(unattached_disks):
            deleted = delete_resource(credential, subscription_id, disk['id'], "Unattached Disk", disk['name'], clients, wait_for_completion)
            # Optional: Remove from original list if successful
            # if deleted: unattached_disks.remove(disk)

    # Cleanup Stopped VMs (Deallocate)
    if stopped_vms:
        print("\n🧹 Checking Stopped VMs for deallocation...")
        for vm in list(stopped_vms):
            deallocated = deallocate_vm(credential, subscription_id, vm['resource_group'], vm['name'], clients['compute'], wait_for_completion)
            # if deallocated: stopped_vms.remove(vm)

    # Cleanup Unused Public IPs
    if unused_public_ips:
        print("\n🧹 Checking Unused Public IPs for cleanup...")
        for ip in list(unused_public_ips):
            deleted = delete_resource(credential, subscription_id, ip['id'], "Unused Public IP", ip['name'], clients, wait_for_completion)
            # if deleted: unused_public_ips.remove(ip)

    # Cleanup Empty Resource Groups
    if empty_rgs:
        print("\n🧹 Checking Empty Resource Groups for cleanup...")
        # Important: Deleting an RG can take a long time and might fail if resources were recently added.
        # Consider adding delays or checks if implementing automatic retry.
        for rg in list(empty_rgs):
             deleted = delete_resource(credential, subscription_id, rg['id'], "Empty Resource Group", rg['name'], clients, wait_for_completion)
             # if deleted: empty_rgs.remove(rg)

    # Cleanup Empty App Service Plans
    if empty_plans:
        print("\n🧹 Checking Empty App Service Plans for cleanup...")
        for plan in list(empty_plans):
            deleted = delete_resource(credential, subscription_id, plan['id'], "Empty App Service Plan", plan['name'], clients, wait_for_completion)
            # if deleted: empty_plans.remove(plan)

    # Cleanup Old Disk Snapshots
    if old_snapshots:
        print("\n🧹 Checking Old Disk Snapshots for cleanup...")
        for snap in list(old_snapshots):
            deleted = delete_resource(credential, subscription_id, snap['id'], "Old Disk Snapshot", snap['name'], clients, wait_for_completion)
            # if deleted: old_snapshots.remove(snap)

    print("\n--- Interactive Cleanup Finished ---")

def find_idle_application_gateways(credential, subscription_id, lookback_days=METRIC_LOOKBACK_DAYS, idle_connection_threshold=5):
    """Finds Application Gateways with very low average connections over the lookback period."""
    print(f"\nChecking Application Gateways for low average connections (< {idle_connection_threshold}) over the last {lookback_days} days...")
    network_client = NetworkManagementClient(credential, subscription_id)
    monitor_client = MonitorManagementClient(credential, subscription_id)
    idle_gateways = []
    
    try:
        app_gateways = list(network_client.application_gateways.list_all())
        if not app_gateways:
            print("  - No Application Gateways found in the subscription.")
            return idle_gateways

        print(f"  - Found {len(app_gateways)} Application Gateways to analyze...")

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=lookback_days)
        # Ensure ISO 8601 format with Z for UTC
        timespan = f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        # timespan = f"{start_time.isoformat()}/{end_time.isoformat()}" # Previous attempt
        metric_name = "CurrentConnections" # Or Throughput, FailedRequests etc.
        aggregation = "Average"

        for gw in app_gateways:
            try:
                # Filter out gateways that might not have metrics (e.g., provisioning state issues)
                if gw.provisioning_state.lower() != 'succeeded':
                     print(f"  - Skipping GW '{gw.name}' due to provisioning state: {gw.provisioning_state}")
                     continue

                metric_data = monitor_client.metrics.list(
                    resource_uri=gw.id,
                    timespan=timespan,
                    metricnames=metric_name,
                    aggregation=aggregation,
                    interval=timedelta(days=1) # Use a larger interval like 1 day for avg over period
                )

                avg_connections = None
                if metric_data.value and metric_data.value[0].timeseries:
                    # Access the average value from the time series data
                    # Assuming a single timeseries result for the overall average
                    data_points = metric_data.value[0].timeseries[0].data
                    if data_points:
                        # Calculate overall average if multiple data points returned, otherwise take the single value
                        valid_points = [dp.average for dp in data_points if dp.average is not None]
                        if valid_points:
                            avg_connections = sum(valid_points) / len(valid_points)
                        else:
                             avg_connections = 0 # Treat as zero if no valid data points

                if avg_connections is not None:
                    print(f"  - Analyzed GW: '{gw.name}' (Avg Connections: {avg_connections:.2f})")
                    if avg_connections < idle_connection_threshold:
                        print(f"    - LOW CONNECTIONS DETECTED for '{gw.name}'")
                        idle_gateways.append({
                            'name': gw.name,
                            'resource_group': gw.id.split('/')[4],
                            'location': gw.location,
                            'sku': f"{gw.sku.name} (Tier: {gw.sku.tier}, Capacity: {gw.sku.capacity})",
                            'avg_connections': avg_connections,
                            'id': gw.id
                            # Add estimated cost later
                        })
                else:
                     print(f"  - Could not retrieve valid '{metric_name}' metric data for GW: '{gw.name}'")

            except Exception as metric_error:
                print(f"  - Error getting metrics for Application Gateway '{gw.name}': {metric_error}")
                # Optionally log the error in more detail
                logging.warning(f"Metric query failed for GW {gw.id}: {metric_error}")

    except Exception as e:
        print(f"  - Error listing Application Gateways: {e}")
        logging.error(f"Failed to list Application Gateways: {e}", exc_info=True)

    return idle_gateways

def find_low_usage_web_apps(credential, subscription_id, cpu_threshold_percent=10, lookback_days=METRIC_LOOKBACK_DAYS):
    """Finds running Web Apps with low average CPU usage over a period."""
    print(f"\nChecking Web Apps for low average CPU (< {cpu_threshold_percent}%) over the last {lookback_days} days...")
    web_client = WebSiteManagementClient(credential, subscription_id)
    monitor_client = MonitorManagementClient(credential, subscription_id)
    low_usage_apps = []

    try:
        web_apps = list(web_client.web_apps.list())
        if not web_apps:
            print("  - No Web Apps found in the subscription.")
            return low_usage_apps
        
        print(f"  - Found {len(web_apps)} Web Apps to analyze...")

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=lookback_days)
        timespan = f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        # Try 'AverageCpuPercentage' first, fallback might be needed (e.g., calculating from CpuTime)
        metric_name = "AverageCpuPercentage" 
        aggregation = "Average"

        for app in web_apps:
            try:
                # Skip non-running apps
                if app.state.lower() != 'running':
                    # print(f"  - Skipping App '{app.name}' (State: {app.state})")
                    continue

                # Get App Service Plan details to check tier (skip Free/Shared)
                asp_id_parts = app.server_farm_id.split('/')
                asp_rg = asp_id_parts[4]
                asp_name = asp_id_parts[-1]
                asp = web_client.app_service_plans.get(asp_rg, asp_name)

                # Skip Free (F1) and Shared (D1) tiers for this check
                if asp.sku.tier.lower() in ['free', 'shared']:
                    # print(f"  - Skipping App '{app.name}' on Free/Shared tier ({asp.sku.tier})")
                    continue
                
                # print(f"  - Analyzing App: '{app.name}' on Plan: '{asp.name}' (Tier: {asp.sku.tier})...")

                metric_data = monitor_client.metrics.list(
                    resource_uri=app.id,
                    timespan=timespan,
                    metricnames=metric_name,
                    aggregation=aggregation,
                    # interval=timedelta(hours=1) # Adjust interval as needed
                )

                avg_cpu = None
                if metric_data.value and metric_data.value[0].timeseries:
                    data_points = metric_data.value[0].timeseries[0].data
                    if data_points:
                        valid_points = [dp.average for dp in data_points if dp.average is not None]
                        if valid_points:
                            avg_cpu = sum(valid_points) / len(valid_points)
                        else:
                            avg_cpu = 0 # Treat as zero if no valid points
                
                # Handle case where metric might not be available or returns no data
                if avg_cpu is None:
                    # TODO: Could try querying 'CpuTime' and calculate percentage if 'AverageCpuPercentage' fails
                    print(f"  - Warning: Could not retrieve '{metric_name}' metric for App '{app.name}'. Skipping CPU check.")
                    continue

                print(f"  - Analyzed App: '{app.name}' (Avg CPU: {avg_cpu:.2f}%)")
                if avg_cpu < cpu_threshold_percent:
                    print(f"    - LOW CPU DETECTED for App '{app.name}'")
                    low_usage_apps.append({
                        'name': app.name,
                        'resource_group': app.resource_group,
                        'location': app.location,
                        'app_service_plan': f"{asp_name} (Tier: {asp.sku.tier})",
                        'avg_cpu_percent': avg_cpu,
                        'id': app.id
                        # Cost estimation is complex - related to the plan, not the app itself
                    })

            except Exception as metric_error:
                print(f"  - Error processing Web App '{app.name}': {metric_error}")
                logging.warning(f"Processing failed for Web App {app.id}: {metric_error}")

    except Exception as e:
        print(f"  - Error listing Web Apps: {e}")
        logging.error(f"Failed to list Web Apps: {e}", exc_info=True)

    return low_usage_apps

def find_orphaned_nsgs(credential, subscription_id):
    """Finds Network Security Groups not associated with any NIC or Subnet."""
    print("\nChecking for orphaned Network Security Groups (NSGs)...")
    network_client = NetworkManagementClient(credential, subscription_id)
    orphaned_nsgs = []
    try:
        for nsg in network_client.network_security_groups.list_all():
            is_associated_nic = nsg.network_interfaces is not None and len(nsg.network_interfaces) > 0
            is_associated_subnet = nsg.subnets is not None and len(nsg.subnets) > 0
            
            if not is_associated_nic and not is_associated_subnet:
                print(f"  - Found potential orphaned NSG: {nsg.name} (RG: {nsg.id.split('/')[4]})")
                orphaned_nsgs.append({
                    'name': nsg.name,
                    'resource_group': nsg.id.split('/')[4],
                    'location': nsg.location,
                    'id': nsg.id
                })
        
        if not orphaned_nsgs:
            print("  - No orphaned NSGs found.")
            
    except Exception as e:
        print(f"  - Error checking for orphaned NSGs: {e}")
        logging.error(f"Failed to check orphaned NSGs: {e}", exc_info=True)
    
    return orphaned_nsgs

def find_orphaned_route_tables(credential, subscription_id):
    """Finds Route Tables not associated with any Subnet."""
    print("\nChecking for orphaned Route Tables...")
    network_client = NetworkManagementClient(credential, subscription_id)
    orphaned_tables = []
    try:
        for rt in network_client.route_tables.list_all():
            # Route table is orphaned if its subnets collection is empty or None
            if not rt.subnets:
                print(f"  - Found potential orphaned Route Table: {rt.name} (RG: {rt.id.split('/')[4]})")
                orphaned_tables.append({
                    'name': rt.name,
                    'resource_group': rt.id.split('/')[4],
                    'location': rt.location,
                    'id': rt.id
                })
        
        if not orphaned_tables:
            print("  - No orphaned Route Tables found.")
            
    except Exception as e:
        print(f"  - Error checking for orphaned Route Tables: {e}")
        logging.error(f"Failed to check orphaned Route Tables: {e}", exc_info=True)
    
    return orphaned_tables

def main():
    # --- Setup Logging ---
    setup_logger()
    logging.info("--- Script Execution Started ---")

    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description='Azure Cost Optimization Tool')
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Enable interactive cleanup of identified resources.'
    )
    parser.add_argument(
        '--send-email',
        action='store_true',
        help='Send the summary report via email (requires EMAIL/SMTP env vars).'
    )
    parser.add_argument(
        '--wait-for-cleanup', # New argument
        action='store_true',
        help='Wait for delete/deallocate operations to complete during cleanup.'
    )
    parser.add_argument(
        '--html-report', 
        type=str, 
        metavar='FILENAME', 
        help='Generate an HTML report and save it to the specified file.'
    )
    args = parser.parse_args()
    logging.info(f"Script arguments: {args}")

    console.print("[bold green]Starting Azure Cost Optimization Tool...[/]")
    credential, subscription_id = get_azure_credentials()

    if not credential or not subscription_id:
        console.print("[bold red]Exiting due to authentication or subscription issues.[/]")
        return

    # --- Call get_cost_data ---
    costs_by_type, total_cost, currency = get_cost_data(credential, subscription_id)
    # We list resources first just to have the full list
    all_resources = list_all_resources(credential, subscription_id)
    # if all_resources and not costs_by_type:
    #     costs_by_type, total_cost, currency = get_cost_data(credential, subscription_id)
    # else:
    #     console.print("[yellow]Skipping cost data collection as listing resources failed.[/]")

    # --- Optimization Checks --- 
    console.print("\n[bold blue]--- Running Optimization Checks ---[/]")
    # These find_* functions will still use basic print for now
    unattached_disks = find_unattached_disks(credential, subscription_id)
    stopped_vms = find_stopped_vms(credential, subscription_id)
    unused_public_ips = find_unused_public_ips(credential, subscription_id)
    empty_rgs = find_empty_resource_groups(credential, subscription_id)
    empty_plans = find_empty_app_service_plans(credential, subscription_id)
    old_snapshots = find_old_snapshots(credential, subscription_id)
    low_cpu_vms = find_underutilized_vms(credential, subscription_id)
    low_cpu_plans = find_low_usage_app_service_plans(credential, subscription_id)
    low_dtu_dbs = find_low_dtu_sql_databases(credential, subscription_id)
    idle_gateways = find_idle_application_gateways(credential, subscription_id)
    low_usage_apps = find_low_usage_web_apps(credential, subscription_id)
    orphaned_nsgs = find_orphaned_nsgs(credential, subscription_id)
    orphaned_route_tables = find_orphaned_route_tables(credential, subscription_id)

    # --- Process Findings and Calculate Savings ---
    console.print("\n[bold blue]--- Processing Findings & Calculating Savings ---[/]") # Use console
    potential_savings = {}

    # Unattached Disks
    unattached_disks_df = pd.DataFrame([{
        'Name': disk['name'],
        'Resource Group': disk['resource_group'],
        'Location': disk['location'],
        'Size (GB)': disk['size_gb'],
        'SKU': disk['sku'],
        'Estimated Monthly Cost ($)': 0.0, # Placeholder
        'Resource ID': disk['id']
    } for disk in unattached_disks],
    columns=['Name', 'Resource Group', 'Location', 'Size (GB)', 'SKU', 'Estimated Monthly Cost ($)', 'Resource ID'])
    potential_savings['Unattached Disks'] = unattached_disks_df['Estimated Monthly Cost ($)'].sum() if not unattached_disks_df.empty else 0.0

    # Stopped VMs
    stopped_vms_df = pd.DataFrame([{
        'Name': vm['name'],
        'Resource Group': vm['resource_group'],
        'Location': vm['location'],
        'Size': 'N/A', # Placeholder
        'Estimated Monthly Cost ($)': 0.0, # Placeholder for disk cost
        'Resource ID': vm['id']
    } for vm in stopped_vms],
    columns=['Name', 'Resource Group', 'Location', 'Size', 'Estimated Monthly Cost ($)', 'Resource ID'])
    # potential_savings['Stopped VMs'] = stopped_vms_df['Estimated Monthly Cost ($)'].sum() if not stopped_vms_df.empty else 0.0

    # Unused Public IPs
    unused_public_ips_df = pd.DataFrame([{
        'Name': ip['name'],
        'Resource Group': ip['resource_group'],
        'Location': ip['location'],
        'SKU': ip['sku'],
        'IP Address': ip['ip_address'],
        # 'Estimated Monthly Cost ($)': 0.0, # Placeholder
        'Estimated Monthly Cost ($)': estimate_public_ip_cost(ip['sku'], ip['location']),
        'Resource ID': ip['id']
    } for ip in unused_public_ips],
    columns=['Name', 'Resource Group', 'Location', 'SKU', 'IP Address', 'Estimated Monthly Cost ($)', 'Resource ID'])
    potential_savings['Unused Public IPs'] = unused_public_ips_df['Estimated Monthly Cost ($)'].sum() if not unused_public_ips_df.empty else 0.0

    # Empty Resource Groups
    empty_resource_groups_df = pd.DataFrame([{
        'Name': rg['name'],
        'Location': rg['location'],
        'Resource ID': rg['id']
    } for rg in empty_rgs],
    columns=['Name', 'Location', 'Resource ID'])
    # potential_savings['Empty Resource Groups'] = 0

    # Empty App Service Plans
    empty_plans_df = pd.DataFrame([{
        'Name': plan['name'],
        'Resource Group': plan['resource_group'],
        'Location': plan['location'],
        'SKU': plan['sku'],
        # 'Estimated Monthly Cost ($)': 0.0, # Placeholder
        'Estimated Monthly Cost ($)': estimate_app_service_plan_cost(plan['sku'], plan['location']),
        'Resource ID': plan['id']
    } for plan in empty_plans],
    columns=['Name', 'Resource Group', 'Location', 'SKU', 'Estimated Monthly Cost ($)', 'Resource ID'])
    potential_savings['Empty App Service Plans'] = empty_plans_df['Estimated Monthly Cost ($)'].sum() if not empty_plans_df.empty else 0.0

    # Old Snapshots
    old_snapshots_df = pd.DataFrame([{
        'Name': snap['name'],
        'Resource Group': snap['resource_group'],
        'Location': snap['location'],
        'Created': snap['time_created'],
        'Size (GB)': snap['size_gb'],
        # 'Estimated Monthly Cost ($)': 0.0, # Placeholder
        'Estimated Monthly Cost ($)': estimate_snapshot_cost(snap['size_gb'], snap['location'], snap.get('sku', 'Standard_LRS')), # Use default SKU if missing
        'Resource ID': snap['id']
    } for snap in old_snapshots],
    columns=['Name', 'Resource Group', 'Location', 'Created', 'Size (GB)', 'Estimated Monthly Cost ($)', 'Resource ID'])
    potential_savings['Old Snapshots'] = old_snapshots_df['Estimated Monthly Cost ($)'].sum() if not old_snapshots_df.empty else 0.0

    # --- Low Usage Resources (Reporting only, no direct savings calculation here) ---
    low_cpu_vms_df = pd.DataFrame([{
        'Name': vm['name'],
        'Resource Group': vm['resource_group'],
        'Location': vm['location'],
        'Avg CPU (%)': f"{vm['avg_cpu_percent']:.2f}" if vm['avg_cpu_percent'] is not None else "N/A",
        'Avg Available Memory (Bytes)': vm['avg_available_memory_bytes'],
        'Resource ID': vm['id']
    } for vm in low_cpu_vms],
    columns=['Name', 'Resource Group', 'Location', 'Avg CPU (%)', 'Avg Available Memory (Bytes)', 'Resource ID'])

    low_cpu_plans_df = pd.DataFrame([{
        'Name': plan['name'],
        'Resource Group': plan['resource_group'],
        'Location': plan['location'],
        'SKU': plan['sku'],
        'Avg CPU (%)': f"{plan['avg_cpu_percent']:.2f}" if plan['avg_cpu_percent'] is not None else "N/A",
        'Resource ID': plan['id']
    } for plan in low_cpu_plans],
    columns=['Name', 'Resource Group', 'Location', 'SKU', 'Avg CPU (%)', 'Resource ID'])

    low_dtu_dbs_df = pd.DataFrame([{
        'Name': db['name'],
        'Resource Group': db['resource_group'],
        'Location': db['location'],
        'Server': db['server_name'],
        'SKU': db['sku'],
        'Avg DTU (%)': f"{db['avg_dtu_percent']:.2f}" if db['avg_dtu_percent'] is not None else "N/A",
        'Estimated Monthly Cost ($)': estimate_sql_database_cost(db['sku'], db['location']), # Add estimated cost
        'Resource ID': db['id']
    } for db in low_dtu_dbs],
    columns=['Name', 'Resource Group', 'Location', 'Server', 'SKU', 'Avg DTU (%)', 'Estimated Monthly Cost ($)', 'Resource ID'])

    idle_gateways_df = pd.DataFrame([{
        'Name': gw['name'],
        'Resource Group': gw['resource_group'],
        'Location': gw['location'],
        'SKU': gw['sku'],
        'Avg Connections': f"{gw['avg_connections']:.2f}" if gw.get('avg_connections') is not None else "N/A",
        'Resource ID': gw['id']
    } for gw in idle_gateways],
    columns=['Name', 'Resource Group', 'Location', 'SKU', 'Avg Connections', 'Resource ID'])

    low_usage_apps_df = pd.DataFrame([{
        'Name': app['name'],
        'Resource Group': app['resource_group'],
        'Location': app['location'],
        'App Service Plan': app['app_service_plan'],
        'Avg CPU (%)': f"{app['avg_cpu_percent']:.2f}" if app.get('avg_cpu_percent') is not None else "N/A",
        'Resource ID': app['id']
    } for app in low_usage_apps],
    columns=['Name', 'Resource Group', 'Location', 'App Service Plan', 'Avg CPU (%)', 'Resource ID'])

    orphaned_nsgs_df = pd.DataFrame(orphaned_nsgs, columns=['Name', 'Resource Group', 'Location', 'Resource ID'])
    orphaned_route_tables_df = pd.DataFrame(orphaned_route_tables, columns=['Name', 'Resource Group', 'Location', 'Resource ID'])

    total_potential_savings = sum(potential_savings.values())
    cost_breakdown = costs_by_type

    # --- Output Results to Console using Rich --- 
    console.print("\n[bold magenta]--- Cost Optimization Findings (Console) ---[/]")

    # Rich Table printing helper function (defined inside main)
    def print_rich_table(df, title, icon=":mag:"):
        if df is None or df.empty:
            console.print(f"\n{icon} [bold blue]{title}:[/] [dim]None found.[/]")
            return

        console.print(f"\n{icon} [bold blue]{title}:[/]")
        table = Table(show_header=True, header_style="bold cyan", box=rich.box.SIMPLE)
        
        for col in df.columns:
            justify = "right" if ("Cost" in col or "Size" in col or "Avg" in col or "(%)" in col or "Bytes" in col) else "left"
            style = ""
            if "Cost" in col: style = "green"
            elif "Name" in col: style = "bold"
            elif "ID" in col: style = "dim"
            table.add_column(col, style=style, justify=justify)

        for _, row in df.iterrows():
            table.add_row(*(str(item) if item is not None else "N/A" for item in row))
        
        console.print(table)

    # Call the new table printer
    print_rich_table(unattached_disks_df, "Unattached Disks", icon=":floppy_disk:")
    print_rich_table(stopped_vms_df, "Stopped VMs", icon=":stop_sign:")
    print_rich_table(unused_public_ips_df, "Unused Public IPs", icon=":globe_with_meridians:")
    print_rich_table(empty_resource_groups_df, "Empty Resource Groups", icon=":wastebasket:")
    print_rich_table(empty_plans_df, "Empty App Service Plans", icon=":spider_web:")
    print_rich_table(old_snapshots_df, "Old Snapshots", icon=":camera_flash:")
    print_rich_table(low_cpu_vms_df, "Low CPU VMs", icon=":chart_decreasing:")
    print_rich_table(low_cpu_plans_df, "Low CPU App Service Plans", icon=":chart_decreasing:")
    print_rich_table(low_dtu_dbs_df, "Low DTU SQL Databases", icon=":chart_decreasing:")
    print_rich_table(idle_gateways_df, "Potentially Idle Application Gateways", icon=":electric_plug:") 
    print_rich_table(low_usage_apps_df, "Low CPU Web Apps", icon=":computer_mouse:") 
    print_rich_table(orphaned_nsgs_df, "Orphaned Network Security Groups", icon=":shield:")
    print_rich_table(orphaned_route_tables_df, "Orphaned Route Tables", icon=":world_map:")

    # Prepare combined ignored resources DataFrame if needed for HTML report
    ignored_resources_df = pd.DataFrame() # Placeholder

    # --- Cost Summary Section --- 
    console.print("\n[bold magenta]--- Cost Summary (Console) ---[/]")
    console.print(":moneybag: [bold]Estimated Cost Breakdown by Resource Type (Month-to-Date):[/]")
    if cost_breakdown:
        breakdown_table = Table(show_header=False, box=None, padding=(0, 1))
        breakdown_table.add_column(style="cyan")
        breakdown_table.add_column(style="green")
        for r_type, cost in sorted(cost_breakdown.items(), key=lambda item: item[1], reverse=True):
            breakdown_table.add_row(f"  - {r_type}", f"${cost:.2f} {currency}")
        console.print(breakdown_table)
    else:
        console.print("  [dim](Cost breakdown data not available)[/]")

    console.print("\n:chart_increasing: [bold]Potential Monthly Savings Breakdown:[/]")
    savings_found = False
    savings_table = Table(show_header=False, box=None, padding=(0, 1))
    savings_table.add_column(style="cyan")
    savings_table.add_column(style="bold green")
    for category, saving in potential_savings.items():
        if saving > 0:
            savings_table.add_row(f"  - {category}", f"${saving:.2f}")
            savings_found = True
    if not savings_found:
         console.print("  [dim](No specific potential savings calculated yet)[/]")
    else:
         console.print(savings_table)
         
    console.print(f"\n:white_check_mark: [bold]Total Potential Monthly Savings (Estimated):[/] [bold green]${total_potential_savings:.2f}[/]")

    # --- HTML Report Generation (if requested) ---
    if args.html_report:
        print(f"\n📄 Generating HTML report: {args.html_report}")
        html_content = generate_html_report_content(
            findings={ # Pass a combined findings dict (or individual dfs)
                'Unattached Disks': unattached_disks_df,
                'Stopped VMs': stopped_vms_df,
                'Unused Public IPs': unused_public_ips_df,
                'Empty Resource Groups': empty_resource_groups_df,
                'Empty App Service Plans': empty_plans_df,
                'Old Snapshots': old_snapshots_df,
                'Low CPU VMs': low_cpu_vms_df,
                'Low CPU App Service Plans': low_cpu_plans_df,
                'Low DTU SQL Databases': low_dtu_dbs_df,
                'Idle Gateways': idle_gateways_df,
                'Low CPU Web Apps': low_usage_apps_df,
                'Orphaned NSGs': orphaned_nsgs_df, # Added Orphaned NSGs
                'Orphaned Route Tables': orphaned_route_tables_df # Added Orphaned Route Tables
            },
            cost_data=costs_by_type, # Pass costs by type
            unattached_disks=unattached_disks_df, # Pass individual DFs needed by HTML func
            stopped_vms=stopped_vms_df,
            unused_public_ips=unused_public_ips_df,
            empty_resource_groups=empty_resource_groups_df,
            low_usage_app_service_plans=low_cpu_plans_df,
            low_usage_apps_df=low_usage_apps_df, 
            orphaned_nsgs_df=orphaned_nsgs_df, # Pass the new DataFrame
            orphaned_route_tables_df=orphaned_route_tables_df, # Pass the new DataFrame
            potential_savings=potential_savings,
            total_potential_savings=total_potential_savings,
            cost_breakdown=cost_breakdown,
            ignored_resources=ignored_resources_df, # Pass empty df for now
            include_ignored=False # Placeholder, connect to args if needed
        )
        write_html_report(html_content, args.html_report)
    
    # --- Email Notification (if requested) ---
    if args.send_email:
        # TODO: Refactor email content generation if desired
        send_email_report(generate_summary_report(
            unattached_disks, stopped_vms, unused_public_ips, empty_rgs, empty_plans, old_snapshots, 
            low_cpu_vms, low_cpu_plans,
            low_dtu_dbs,
            costs_by_type, total_cost, currency
        )) # Still uses old report generator for email body
    else:
        console.print("\n:fast_forward: [dim]Email notification skipped. Use the --send-email flag to enable.[/]")

    # --- Interactive Cleanup (if requested) --- 
    actionable_findings = any([unattached_disks, stopped_vms, unused_public_ips, empty_rgs, empty_plans, old_snapshots]) # Re-check potentially modified lists
    if args.cleanup:
        logging.info("Cleanup requested by user.")
        # Refactor cleanup prompts/status later
        console.print("\n[bold yellow]--- Interactive Cleanup ---[/]")
        console.print(":warning: [yellow]You will be prompted to confirm each action. Deletions may take time.[/]", highlight=False)
        if args.wait_for_cleanup:
            console.print(":hourglass: [cyan]Waiting for each cleanup operation to complete...[/]")
            
        if actionable_findings:
             logging.info("Actionable findings present. Starting interactive cleanup.")
             # Cleanup function still uses basic print/input
             perform_interactive_cleanup(credential, subscription_id, 
                                       unattached_disks, stopped_vms, unused_public_ips, 
                                       empty_rgs, empty_plans, old_snapshots, 
                                       args.wait_for_cleanup)
        else:
            logging.info("Cleanup requested, but no actionable findings identified.")
            console.print("\n:information_source: No actionable findings identified for cleanup.")
    else:
        logging.info("Cleanup actions were not requested (--cleanup flag not used).")
        console.print("\n:fast_forward: [dim]Cleanup actions skipped. Use the --cleanup flag to enable interactive cleanup.[/]")


    logging.info("--- Script Execution Finished ---")
    console.print("\n:party_popper: [bold green]Script finished.[/]")


# --- Main Execution --- 
if __name__ == "__main__":
    # Removed old print_df_if_exists definition
    console.print(" [bold green]Starting Azure Cost Optimization Tool...[/]")
    main() 