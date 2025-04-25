import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# Azure SDK clients
from azure.identity import DefaultAzureCredential, AzureCliCredential, ManagedIdentityCredential, ChainedTokenCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.web import WebSiteManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.sql import SqlManagementClient
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.costmanagement.models import QueryTimePeriod, QueryDataset, QueryDefinition
from azure.core.exceptions import HttpResponseError, ClientAuthenticationError
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions

# Rich for console output
from rich.console import Console
from rich.table import Table
from rich.progress import track # For simple progress bars if needed elsewhere
import concurrent.futures # For potential parallelization
import time # For potential retries

# Import constants from config module
# Use relative import assuming config.py is in the same directory
from .config import (
    SNAPSHOT_AGE_THRESHOLD_DAYS,
    LOW_CPU_THRESHOLD_PERCENT,
    APP_SERVICE_PLAN_LOW_CPU_THRESHOLD_PERCENT,
    METRIC_LOOKBACK_DAYS,
    SQL_DB_LOW_DTU_THRESHOLD_PERCENT,
    SQL_VCORE_LOW_CPU_THRESHOLD_PERCENT,
    IDLE_CONNECTION_THRESHOLD_GATEWAY,
    LOW_CPU_THRESHOLD_WEB_APP
)

# Initialize console for potential standalone use or if passed
_console = Console()

# --- Utility Function for Timespan ---

def _get_iso8601_timespan(lookback_days: int) -> str:
    """Generates an ISO 8601 compliant timespan string (start/end)."""
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=lookback_days)
    # Format: YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ
    timespan = f"{start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}/{now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    return timespan

# --- Resource Listing and Cost Data ---

def list_all_resources(credential, subscription_id, console: Console = _console):
    """Lists all resources in the subscription."""
    resources = []
    try:
        resource_client = ResourceManagementClient(credential, subscription_id)
        console.print("\n[bold blue]--- Fetching Azure Resources ---[/]")
        resource_list = list(resource_client.resources.list())
        
        for resource in resource_list:
            resources.append({
                "name": resource.name,
                "type": resource.type,
                "location": resource.location,
                "id": resource.id,
                "tags": resource.tags
            })
        console.print(f":white_check_mark: Total resources found: {len(resources)}")
        return resources
    except Exception as e:
        console.print(f"[bold red]Error listing resources:[/] {e}")
        return []

def get_cost_data(credential, subscription_id, console: Console = _console):
    """Retrieves cost data for the current billing month, grouped by Resource Type."""
    costs_by_type = defaultdict(float)
    total_cost = 0.0
    currency = "N/A"
    try:
        # Explicitly set the base_url to ensure HTTPS is used
        cost_client = CostManagementClient(
            credential=credential,
            subscription_id=subscription_id,
            base_url="https://management.azure.com" # Force HTTPS endpoint
        )
        scope = f"/subscriptions/{subscription_id}" # Correct scope format

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
                table = Table(show_header=False, box=None, padding=(0, 1))
                table.add_column(style="cyan") # Resource Type
                table.add_column(style="green") # Cost
                
                # Filter out rows where resource type is None or empty before sorting
                valid_rows = [row for row in result.rows if len(row) > res_type_index and row[res_type_index]]
                sorted_rows = sorted(valid_rows, key=lambda r: r[cost_index], reverse=True)

                for row in sorted_rows:
                    cost = row[cost_index]
                    curr = row[currency_index]
                    res_type = row[res_type_index]
                    if currency == "N/A": currency = curr # Capture currency from first valid row
                    costs_by_type[res_type] += cost
                    total_cost += cost
                    table.add_row(f"  - {res_type}", f"{cost:.2f} {curr}")
                console.print(table)
                console.print(f"  [bold]Total Estimated Cost:[/][bold green] {total_cost:.2f} {currency}[/]")
            else:
                 console.print("[yellow]  - Warning: Could not parse cost data columns correctly.[/]")
                 # Attempt basic fallback if possible
                 if result.rows and result.rows[0] and len(result.rows[0]) >= 2:
                     try:
                        total_cost = float(result.rows[0][0])
                        currency = result.rows[0][1]
                        console.print(f"[yellow]  - Fallback Total Cost (sum of first row):[/][bold yellow] {total_cost:.2f} {currency}[/]")
                     except (ValueError, TypeError):
                         console.print("[red]  - Error in fallback cost parsing.[/]")
                 else:
                      console.print("[yellow]  - Insufficient data for fallback cost parsing.[/]")
        else:
            console.print("[yellow]  - No cost data found for the period.[/]")

        return costs_by_type, total_cost, currency

    except Exception as e:
        console.print(f"[bold red]Error fetching cost data:[/] {e}")
        if "429" in str(e) or "throttled" in str(e).lower():
            console.print("[yellow]  - Suggestion: Cost Management API might be throttled. Consider adding delays or retries.[/]")
        return costs_by_type, total_cost, currency # Return empty/default values

# --- Specific Analysis Functions ---

def find_unattached_disks(credential, subscription_id, console: Console):
    """Finds managed disks that are not attached to any VM using Azure Resource Graph."""
    logger = logging.getLogger()
    logger.info("💾 Checking for unattached managed disks (using ARG)...")
    console.print("\n💾 Checking for unattached managed disks (using ARG)...")
    disks = []
    try:
        # Use Resource Graph Client
        arg_client = ResourceGraphClient(credential)

        # KQL query to find unattached disks
        # Checks for diskState == 'Unattached' and managedBy property is null or empty
        # Projects the required fields
        kql_query = """
        Resources
        | where type =~ 'microsoft.compute/disks'
        | where properties.diskState == 'Unattached' and (isnull(properties.managedBy) or properties.managedBy == '')
        | project name, id, resourceGroup, location, sizeGb = properties.diskSizeGB, skuName = sku.name
        """

        # Create the query request
        query_request = QueryRequest(subscriptions=[subscription_id], query=kql_query)
        
        logger.debug(f"Executing ARG query for unattached disks: {kql_query}")
        query_response = arg_client.resources(query_request)
        logger.debug(f"ARG query returned {query_response.total_records} records.")

        if query_response.total_records > 0 and query_response.data:
            # Process the results (which are in query_response.data as a list of dicts)
            for disk_data in query_response.data:
                disks.append({
                    'name': disk_data.get('name', 'Unknown'),
                    'resource_group': disk_data.get('resourceGroup', 'Unknown'),
                    'location': disk_data.get('location', 'Unknown'),
                    'size_gb': disk_data.get('sizeGb'), # Note the field name from project
                    'sku': disk_data.get('skuName', 'Unknown'), # Note the field name
                    'id': disk_data.get('id', 'Unknown'),
                })

        if not disks:
            console.print("  :heavy_check_mark: No unattached managed disks found.")
        else:
             console.print(f"  :warning: Found {len(disks)} unattached disk(s).")
    except Exception as e:
        logger.error(f"Error checking for unattached disks using ARG: {e}", exc_info=True)
        console.print(f"  [bold red]Error checking for unattached disks (ARG):[/] {e}")
    return disks

def find_stopped_vms(credential, subscription_id, console: Console):
    """Finds VMs that are stopped but not deallocated."""
    logger = logging.getLogger()
    logger.info("🛑 Checking for stopped (not deallocated) VMs...")
    console.print("\n🛑 Checking for stopped (not deallocated) VMs...") # Keep simple print
    stopped_vms = []
    try:
        compute_client = ComputeManagementClient(credential, subscription_id)
        vm_list = list(compute_client.virtual_machines.list_all())
        for vm in vm_list:
            rg_name = None
            try:
                # Extract RG name safely
                try:
                    rg_name = vm.id.split('/')[4]
                except IndexError:
                    logger.warning(f"Could not parse resource group for VM {vm.name}. Skipping.")
                    console.print(f"  [yellow]Warning:[/][dim] Could not parse resource group for VM {vm.name}. Skipping.[/]")
                    continue

                instance_view = compute_client.virtual_machines.instance_view(
                    resource_group_name=rg_name,
                    vm_name=vm.name
                )
                power_state = None
                if instance_view.statuses:
                    for status in instance_view.statuses:
                        if status.code and status.code.startswith("PowerState/"):
                            power_state = status.code.split('/')[-1]
                            break
                # Check if the power state is specifically 'stopped' (not 'deallocated')
                if power_state == "stopped":
                    vm_details = {
                        "name": vm.name,
                        "id": vm.id,
                        "resource_group": rg_name,
                        "location": vm.location,
                        "disks": [] # Initialize list for disk details
                    }

                    # --- Get Disk Details --- 
                    disk_info_list = []
                    if vm.storage_profile:
                         # Get OS Disk details
                         if vm.storage_profile.os_disk and vm.storage_profile.os_disk.managed_disk:
                             try:
                                 os_disk_name = vm.storage_profile.os_disk.name
                                 os_disk_id = vm.storage_profile.os_disk.managed_disk.id
                                 os_disk_gb = vm.storage_profile.os_disk.disk_size_gb
                                 # Get the full disk resource to find SKU and location
                                 disk_resource = compute_client.disks.get(rg_name, os_disk_name)
                                 os_disk_sku = disk_resource.sku.name if disk_resource.sku else 'Unknown'
                                 os_disk_location = disk_resource.location
                                 disk_info_list.append({
                                     'name': os_disk_name,
                                     'size_gb': os_disk_gb,
                                     'sku': os_disk_sku,
                                     'location': os_disk_location, # Use actual disk location
                                     'id': os_disk_id
                                 })
                             except Exception as disk_err:
                                 logger.warning(f"Could not fetch OS disk details for VM {vm.name}: {disk_err}")
                                 disk_info_list.append({'name': vm.storage_profile.os_disk.name, 'error': 'Could not fetch full details'})

                         # Get Data Disk details
                         if vm.storage_profile.data_disks:
                             for data_disk in vm.storage_profile.data_disks:
                                 if data_disk.managed_disk:
                                      try:
                                         data_disk_name = data_disk.name
                                         data_disk_id = data_disk.managed_disk.id
                                         data_disk_gb = data_disk.disk_size_gb
                                         disk_resource = compute_client.disks.get(rg_name, data_disk_name)
                                         data_disk_sku = disk_resource.sku.name if disk_resource.sku else 'Unknown'
                                         data_disk_location = disk_resource.location
                                         disk_info_list.append({
                                             'name': data_disk_name,
                                             'size_gb': data_disk_gb,
                                             'sku': data_disk_sku,
                                             'location': data_disk_location,
                                             'id': data_disk_id
                                         })
                                      except Exception as disk_err:
                                         logger.warning(f"Could not fetch data disk {data_disk.name} details for VM {vm.name}: {disk_err}")
                                         disk_info_list.append({'name': data_disk.name, 'error': 'Could not fetch full details'})

                    vm_details['disks'] = disk_info_list
                    stopped_vms.append(vm_details)

            except Exception as iv_error:
                 # Log specific error for instance view failure
                 # Use rg_name if available, otherwise vm.id
                 vm_identifier = f"VM {vm.name} in RG {rg_name}" if rg_name else f"VM ID {vm.id}"
                 logging.warning(f"Could not get instance view for {vm_identifier}. Error: {iv_error}", exc_info=True)
                 console.print(f"  [yellow]Warning:[/yellow] Could not get instance view for VM {vm.name}. Skipping status check.[/]")

        if not stopped_vms:
            console.print("  :heavy_check_mark: No stopped (but not deallocated) VMs found.")
        else:
            console.print(f"  :warning: Found {len(stopped_vms)} stopped VM(s) that are incurring compute costs.")
        return stopped_vms

    except Exception as e:
        logging.error(f"Error checking for stopped VMs: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for stopped VMs:[/] {e}")
        return []

def find_unused_public_ips(credential, subscription_id, console: Console):
    """Finds Public IP addresses not associated with any resource."""
    logger = logging.getLogger()
    logger.info("🌐 Checking for unused Public IP Addresses...")
    console.print("\n🌐 Checking for unused Public IP Addresses...") # Keep simple print
    unused_ips = []
    try:
        network_client = NetworkManagementClient(credential, subscription_id)
        public_ips = list(network_client.public_ip_addresses.list_all())
        for ip in public_ips:
            if ip.ip_configuration is None: # Primary indicator of being unattached
                # Also check if it's associated with a NAT gateway (nat_gateway attribute)
                # Or a Load Balancer frontend IP config (though ip_configuration check usually covers this)
                if ip.nat_gateway is None:
                    sku_name = ip.sku.name if ip.sku else "Basic"
                    unused_ips.append({
                        "name": ip.name, 
                        "id": ip.id, 
                        "resource_group": ip.id.split('/')[4],
                        "location": ip.location, 
                        "ip_address": ip.ip_address,
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

def find_empty_resource_groups(credential, subscription_id, console: Console):
    """Finds resource groups that contain no resources using Azure Resource Graph."""
    logger = logging.getLogger()
    logger.info("🗑 Checking for empty Resource Groups (using ARG)...")
    console.print("\n🗑 Checking for empty Resource Groups (using ARG)...") 
    empty_rgs = []
    try:
        arg_client = ResourceGraphClient(credential)

        # KQL Query to find resource groups with no resources
        # - Select resource groups
        # - Join with resource counts per group
        # - Filter where count is null or zero
        kql_query = """
        ResourceContainers
        | where type == 'microsoft.resources/subscriptions/resourcegroups'
        | project id, name, location, resourceGroup = name 
        | join kind=leftouter (Resources | summarize count() by resourceGroup) on resourceGroup
        | where isnull(count_) or count_ == 0
        | project name, id, location
        """

        query_request = QueryRequest(subscriptions=[subscription_id], query=kql_query)

        logger.debug(f"Executing ARG query for empty resource groups: {kql_query}")
        query_response = arg_client.resources(query_request)
        logger.debug(f"ARG query returned {query_response.total_records} empty resource groups.")

        if query_response.total_records > 0 and query_response.data:
            for rg_data in query_response.data:
                empty_rgs.append({
                    "name": rg_data.get('name', 'Unknown'), 
                    "id": rg_data.get('id', 'Unknown'), 
                    "location": rg_data.get('location', 'Unknown'),
                })

        if not empty_rgs:
            console.print("  :heavy_check_mark: No empty Resource Groups found.")
        else:
            console.print(f"  :warning: Found {len(empty_rgs)} empty Resource Group(s).")
        return empty_rgs

    except Exception as e:
        logger.error(f"Error checking for empty Resource Groups using ARG: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for empty Resource Groups (ARG):[/] {e}")
        return []

def find_empty_app_service_plans(credential, subscription_id, console: Console):
    """Finds App Service Plans that host no applications."""
    logger = logging.getLogger()
    logger.info("🕸 Checking for empty App Service Plans...")
    console.print("\n🕸 Checking for empty App Service Plans...") # Keep simple print
    empty_asps = []
    try:
        web_client = WebSiteManagementClient(credential, subscription_id)
        plans = list(web_client.app_service_plans.list())
        for plan in plans:
            apps_in_plan = list(web_client.web_apps.list_by_resource_group(plan.id.split('/')[4])) # Extract RG
            # Filter apps that specifically belong to this plan
            apps_on_this_plan = [app for app in apps_in_plan if app.server_farm_id and app.server_farm_id.lower() == plan.id.lower()]
            if not apps_on_this_plan:
                empty_asps.append({
                    "name": plan.name, 
                    "id": plan.id, 
                    "resource_group": plan.id.split('/')[4],
                    "location": plan.location, 
                    "sku": plan.sku.name if plan.sku else "Unknown",
                    "tier": plan.sku.tier if plan.sku else "Unknown" # Add tier info
                })

        if not empty_asps:
            console.print("  :heavy_check_mark: No empty App Service Plans found.")
        else:
            console.print(f"  :warning: Found {len(empty_asps)} empty App Service Plan(s).")
        return empty_asps

    except Exception as e:
        logging.error(f"Error checking for empty App Service Plans: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for empty App Service Plans:[/bold red] {e}")
        return []

def find_old_snapshots(credential, subscription_id, age_threshold_days, console: Console):
    """Finds managed disk snapshots older than a specified threshold."""
    logger = logging.getLogger()
    logger.info(f":camera_flash: Checking for disk snapshots older than {age_threshold_days} days...")
    console.print(f"\n:camera_flash: Checking for disk snapshots older than {age_threshold_days} days...") # Keep simple print
    old_snapshots = []
    try:
        compute_client = ComputeManagementClient(credential, subscription_id)
        snapshots = list(compute_client.snapshots.list())
        for snapshot in snapshots:
            try:
                creation_time = snapshot.time_created
                if not creation_time:
                    logging.warning(f"Snapshot {snapshot.name} has no creation time. Skipping age check.")
                    continue
                
                # Ensure time_created is timezone-aware for comparison
                if creation_time.tzinfo is None:
                    # Attempt to assume UTC, but log a warning as this might be inaccurate
                    logging.warning(f"Snapshot {snapshot.name} creation time is naive. Assuming UTC.")
                    creation_time = creation_time.replace(tzinfo=timezone.utc)

                if (datetime.now(timezone.utc) - creation_time) > timedelta(days=age_threshold_days):
                    rg_name = snapshot.id.split('/')[4]
                    # Determine snapshot SKU type for potential cost estimation
                    sku_name = snapshot.sku.name if snapshot.sku else 'Standard_LRS' # Default assumption

                    old_snapshots.append({
                        "name": snapshot.name,
                        "id": snapshot.id,
                        "resource_group": rg_name,
                        "location": snapshot.location,
                        "time_created": creation_time.isoformat(),
                        "size_gb": snapshot.disk_size_gb,
                        "sku": sku_name # Add SKU
                    })
                    # Keep console output concise
                    # console.print(f"  - Found old snapshot: {snapshot.name} (RG: {rg_name}, Created: {creation_time.strftime('%Y-%m-%d')}, Size: {snapshot.disk_size_gb}GB)")

            except Exception as snap_error:
                 logging.warning(f"Could not process snapshot {snapshot.name}. Error: {snap_error}", exc_info=True)
                 console.print(f"  - [yellow]Warning:[/yellow] Could not process snapshot {snapshot.name}.")

        if not old_snapshots:
            console.print(f"  :heavy_check_mark: No disk snapshots older than {age_threshold_days} days found.")
        else:
            console.print(f"  :warning: Found {len(old_snapshots)} snapshot(s) older than {age_threshold_days} days.")

        return old_snapshots

    except Exception as e:
        logging.error(f"Error checking for old snapshots: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for old snapshots:[/bold red] {e}")
        return []

def find_underutilized_vms(credential, subscription_id, cpu_threshold_percent, lookback_days, console: Console):
    """Finds running VMs with average CPU utilization below a threshold."""
    logger = logging.getLogger()
    logger.info(f"📉 Checking running VMs for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...")
    console.print(f"\n📉 Checking running VMs for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...")
    underutilized_vms = []
    monitor_client = None # Initialize outside try block
    try:
        compute_client = ComputeManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)

        # Get the correct timespan format
        timespan = _get_iso8601_timespan(lookback_days)
        logger.debug(f"Using timespan for VM metrics: {timespan}")

        vm_list = list(compute_client.virtual_machines.list_all())
        running_vm_count = 0

        # Check running state first
        vms_to_check_metrics = []
        for vm in vm_list:
            rg_name = None
            try:
                rg_name = vm.id.split('/')[4]
            except IndexError:
                logger.warning(f"Could not parse resource group for VM {vm.name}. Skipping power state check.")
                continue # Skip this VM entirely if RG can't be determined

            try:
                 instance_view = compute_client.virtual_machines.instance_view(
                     resource_group_name=rg_name,
                     vm_name=vm.name
                 )
                 power_state = None
                 if instance_view.statuses:
                     for status in instance_view.statuses:
                         if status.code and status.code.startswith("PowerState/"):
                             power_state = status.code.split('/')[-1]
                             break
                 if power_state == "running":
                      vms_to_check_metrics.append((vm, rg_name))
                      running_vm_count += 1
            except HttpResponseError as http_err:
                 # Log non-critical errors like 'NotFound' if VM was deleted during scan
                 if http_err.status_code == 404:
                     logger.warning(f"VM {vm.name} in RG {rg_name} not found during instance view check (likely deleted). Skipping.")
                 else:
                     # Log other HTTP errors more visibly
                     logger.error(f"HTTP error checking instance view for VM {vm.name}: {http_err}", exc_info=True)
                     console.print(f"  [yellow]Warning:[/yellow] HTTP error checking state for VM {vm.name}. Skipping metrics check.")
            except Exception as e:
                 logger.error(f"Error checking power state for VM {vm.name}: {e}", exc_info=True)
                 console.print(f"  [yellow]Warning:[/yellow] Could not check power state for VM {vm.name}. Skipping metrics check.")

        if not vms_to_check_metrics:
            console.print("  ℹ No running VMs found to analyze.")
            return []

        console.print(f"  - Found {running_vm_count} running VMs to analyze...")

        # Now query metrics only for running VMs
        for vm, rg_name in vms_to_check_metrics:
            try:
                vm_info = {
                    "name": vm.name,
                    "id": vm.id,
                    "resource_group": rg_name,
                    "location": vm.location,
                    "size": vm.hardware_profile.vm_size if vm.hardware_profile else 'Unknown',
                    "os_type": vm.storage_profile.os_disk.os_type if vm.storage_profile and vm.storage_profile.os_disk else 'Unknown', # Add OS type
                    "avg_cpu_percent": None
                }
                avg_cpu = None
                metric_name = "Percentage CPU"
                try:
                    metrics_data = monitor_client.metrics.list(
                        resource_uri=vm.id,
                        timespan=f"{(datetime.now() - timedelta(days=lookback_days)).isoformat()}/{datetime.now().isoformat()}",
                        interval='P1D',
                        metricnames=metric_name,
                        aggregation="Average"
                    )

                    if metrics_data and metrics_data.value:
                        time_series = metrics_data.value[0].timeseries
                        if time_series and time_series[0].data:
                            valid_points = [d.average for d in time_series[0].data if d.average is not None]
                            if valid_points:
                                avg_cpu = sum(valid_points) / len(valid_points)
                                vm_info["avg_cpu_percent"] = avg_cpu
                                logger.debug(f"VM {vm.name} avg CPU: {avg_cpu:.2f}%")
                            else:
                                 logger.warning(f"No valid data points found for metric '{metric_name}' for VM {vm.name} in the timespan.")
                        else:
                             logger.warning(f"No time series data found for metric '{metric_name}' for VM {vm.name} in the timespan.")
                    else:
                         logger.warning(f"No metric data returned for '{metric_name}' for VM {vm.name}.")

                except HttpResponseError as metric_error:
                     # Handle specific errors like rate limiting or invalid dimensions
                     if metric_error.status_code == 429: # Too Many Requests
                         logger.warning(f"Metrics query for VM {vm.name} throttled. Skipping.")
                         console.print(f"  - [yellow]Throttled:[/yellow] Skipping metrics for VM {vm.name}.")
                     else:
                         # Log other HTTP errors more visibly
                         logger.warning(f"Could not get metrics for VM {vm.name}. Error: {metric_error}", exc_info=True)
                         console.print(f"  - [yellow]Warning:[/yellow] Could not get metrics for VM {vm.name}.")
                except Exception as metric_error: # Catch other potential errors during metric processing
                     logger.warning(f"Error processing metrics for VM {vm.name}: {metric_error}", exc_info=True)
                     console.print(f"  - [yellow]Warning:[/yellow] Error processing metrics for VM {vm.name}.")


                if avg_cpu is not None:
                    if avg_cpu < cpu_threshold_percent:
                        console.print(f"  - [bold yellow]Low Usage:[/bold yellow] VM {vm.name} (Avg CPU: {avg_cpu:.1f}%) is below threshold ({cpu_threshold_percent}%).")
                        underutilized_vms.append(vm_info)
                    else:
                        logger.info(f"VM {vm.name} CPU usage OK (Avg: {avg_cpu:.1f}%)")
                else:
                    console.print(f"  - [dim]No CPU data for VM:[/dim] {vm.name}")

            except Exception as e: # Catch errors in the outer loop for a specific VM
                 logger.error(f"Error processing VM {vm.name}: {e}", exc_info=True)
                 console.print(f"  [red]Error:[/red] Could not process VM {vm.name}. Check logs.")

        console.print("\n--- VM Usage Analysis Summary ---")
        if underutilized_vms:
            console.print(f"  :warning: Found {len(underutilized_vms)} running VM(s) with avg CPU < {cpu_threshold_percent}%.")
        else:
            console.print(f"  :heavy_check_mark: No running VMs found with avg CPU < {cpu_threshold_percent}%.")

    except Exception as e:
        logger.error(f"Error checking for underutilized VMs: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for underutilized VMs:[/] {e}")
    return underutilized_vms

def find_low_usage_app_service_plans(credential, subscription_id, cpu_threshold_percent, lookback_days, console: Console):
    """Finds App Service Plans (non-Free/Shared) with low average CPU utilization."""
    logger = logging.getLogger()
    logger.info(f"📉 Checking App Service Plans (Basic tier+) for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...")
    console.print(f"\n📉 Checking App Service Plans (Basic tier+) for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...")
    low_usage_plans = []
    monitor_client = None # Initialize outside try block
    try:
        web_client = WebSiteManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)

        # Get the correct timespan format
        timespan = _get_iso8601_timespan(lookback_days)
        logger.debug(f"Using timespan for ASP metrics: {timespan}")

        plans = list(web_client.app_service_plans.list())
        plans_to_check = []
        for plan in plans:
             # Filter out Free and Shared tiers
             if plan.sku and plan.sku.tier and plan.sku.tier.lower() not in ['free', 'shared', 'dynamic']: # Also exclude Consumption/Dynamic
                 plans_to_check.append(plan)

        if not plans_to_check:
            console.print("  ℹ No App Service Plans found in relevant tiers (Basic or higher) to analyze.")
            return []

        console.print(f"  - Found {len(plans_to_check)} App Service Plans in relevant tiers to analyze...")

        for plan in plans_to_check:
            plan_resource_uri = plan.id # Store URI for error messages
            plan_name = plan.name # Store name for error messages
            plan_details = None # Initialize plan_details for the current plan iteration
            try:
                rg_name = plan.resource_group # Assumes RG is available on plan object

                plan_details = {
                     "name": plan.name,
                     "id": plan.id,
                     "resource_group": rg_name,
                     "location": plan.location,
                     "tier": plan.sku.tier,
                     "sku": plan.sku.name,
                     "avg_cpu_percent": None
                 }
                avg_cpu = None
                metric_name = "CpuPercentage" # Metric name for ASP CPU
                try:
                    metrics_data = monitor_client.metrics.list(
                        resource_uri=plan_resource_uri,
                        timespan=f"{(datetime.now() - timedelta(days=lookback_days)).isoformat()}/{datetime.now().isoformat()}",
                        interval='P1D',
                        metricnames=metric_name,
                        aggregation="Average"
                    )

                    if metrics_data and metrics_data.value:
                        time_series = metrics_data.value[0].timeseries
                        if time_series and time_series[0].data:
                            valid_points = [d.average for d in time_series[0].data if d.average is not None]
                            if valid_points:
                                avg_cpu = sum(valid_points) / len(valid_points)
                                plan_details["avg_cpu_percent"] = avg_cpu
                                logger.debug(f"ASP {plan_name} avg CPU: {avg_cpu:.2f}%")
                            else:
                                 logger.warning(f"No valid data points found for metric '{metric_name}' for ASP {plan_name} in the timespan.")
                        else:
                             logger.warning(f"No time series data found for metric '{metric_name}' for ASP {plan_name} in the timespan.")
                    else:
                         logger.warning(f"No metric data returned for '{metric_name}' for ASP {plan_name}.")

                except HttpResponseError as metric_error:
                     if metric_error.status_code == 429: # Too Many Requests
                         logger.warning(f"Metrics query for ASP {plan_name} throttled. Skipping.")
                         console.print(f"  - [yellow]Throttled:[/yellow] Skipping metrics for ASP {plan_name}.")
                     else:
                         # Log other HTTP errors more visibly
                         logger.warning(f"Could not get metrics for ASP {plan_name}. Error: {metric_error}", exc_info=True)
                         console.print(f"  - [yellow]Warning:[/yellow] Could not get metrics for ASP {plan_name}.")
                except Exception as metric_error: # Catch other potential errors during metric processing
                     # Use plan_name which is guaranteed to be defined here
                     logger.warning(f"Error processing metrics for ASP {plan_name}: {metric_error}", exc_info=True)
                     console.print(f"  - [yellow]Warning:[/yellow] Error processing metrics for ASP {plan_name}.")


                if avg_cpu is not None:
                    if avg_cpu < cpu_threshold_percent:
                         console.print(f"  - [bold yellow]Low Usage:[/bold yellow] ASP {plan_name} (Avg CPU: {avg_cpu:.1f}%) is below threshold ({cpu_threshold_percent}%).")
                         low_usage_plans.append(plan_details)
                    else:
                         logger.info(f"ASP {plan_name} CPU usage OK (Avg: {avg_cpu:.1f}%)")
                else:
                    # Check if plan_details was populated before printing
                    name_to_print = plan_name if plan_name else "Unknown Plan"
                    console.print(f"  - [dim]No CPU data for ASP:[/dim] {name_to_print}")

            except Exception as e: # Catch errors in the outer loop for a specific plan
                 # Use plan_name which is guaranteed to be defined here
                 logger.error(f"Error processing ASP {plan_name}: {e}", exc_info=True)
                 console.print(f"  [red]Error:[/red] Could not process ASP {plan_name}. Check logs.")


        console.print("\n--- App Service Plan Usage Analysis Summary ---")
        if low_usage_plans:
            console.print(f"  :warning: Found {len(low_usage_plans)} ASP(s) with avg CPU < {cpu_threshold_percent}%.")
        else:
            console.print(f"  :heavy_check_mark: No ASPs found with avg CPU < {cpu_threshold_percent}%.")

    except Exception as e:
        # Catch potential errors fetching the initial list of plans
        logger.error(f"Error listing or processing App Service Plans: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for low usage App Service Plans:[/] {e}")
    return low_usage_plans

def find_low_dtu_sql_databases(credential, subscription_id, dtu_threshold_percent, lookback_days, console: Console):
    """Finds SQL Databases (DTU-based model) with low average DTU utilization."""
    logger = logging.getLogger()
    logger.info(f"📉 Checking SQL Databases (DTU model) for avg DTU < {dtu_threshold_percent}% over the last {lookback_days} days...")
    console.print(f"\n📉 Checking SQL Databases (DTU model) for avg DTU < {dtu_threshold_percent}% over the last {lookback_days} days...")
    low_dtu_dbs = []
    monitor_client = None # Initialize outside try block
    try:
        sql_client = SqlManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)

        # Get the correct timespan format
        timespan = _get_iso8601_timespan(lookback_days)
        logger.debug(f"Using timespan for SQL DTU metrics: {timespan}")

        servers = list(sql_client.servers.list())
        dbs_to_check = []

        # Iterate through servers to find databases
        for server in servers:
            # Extract resource group name from server ID
            try:
                rg_name = server.id.split('/')[4]
            except IndexError:
                logger.warning(f"Could not parse resource group for SQL server {server.name}. Skipping databases on this server.")
                continue

            databases = list(sql_client.databases.list_by_server(resource_group_name=rg_name, server_name=server.name))
            for db in databases:
                # Check if it's a DTU-based database
                # Look at currentSku or requestedSku. DTU models are like Basic, Standard, Premium
                # vCore models often have tier 'GeneralPurpose', 'BusinessCritical', 'Hyperscale'
                # Elastic pools are handled separately or ignored for now.
                is_dtu_model = False
                if db.current_sku and db.current_sku.tier and db.current_sku.tier.lower() in ['basic', 'standard', 'premium']:
                    is_dtu_model = True
                elif hasattr(db, 'requested_sku') and db.requested_sku and db.requested_sku.tier and db.requested_sku.tier.lower() in ['basic', 'standard', 'premium']:
                     is_dtu_model = True

                # Also check for elastic pool - skip those for now
                if db.elastic_pool_id:
                     logger.debug(f"Skipping database {db.name} as it's in an elastic pool.")
                     continue # Skip elastic pool databases for this specific check

                if is_dtu_model:
                    # Get server location if DB location is None (shouldn't happen often)
                    db_location = db.location if db.location else server.location
                    dbs_to_check.append((db, rg_name, server.name, db_location))

        if not dbs_to_check:
            console.print("  ℹ No SQL Databases (DTU model) found to check metrics for.")
            return []

        console.print(f"  - Found {len(dbs_to_check)} SQL Databases (DTU model) to analyze...")

        for db, rg_name, server_name, location in dbs_to_check:
            db_resource_uri = db.id # For error reporting
            db_name = db.name # For error reporting
            db_details = None # Initialize
            try:
                 db_details = {
                     "name": db.name,
                     "id": db.id,
                     "resource_group": rg_name,
                     "server_name": server_name,
                     "location": location,
                     "tier": db.current_sku.tier if db.current_sku else 'Unknown',
                     "sku": db.current_sku.name if db.current_sku else 'Unknown',
                     "family": db.current_sku.family if db.current_sku else None, # Family might be needed for pricing
                     "capacity": db.current_sku.capacity if db.current_sku else None, # Capacity (DTUs)
                     "avg_dtu_percent": None
                 }
                 avg_dtu = None
                 # Metric name for DTU percentage
                 metric_name = "dtu_consumption_percent"

                 try:
                     metrics_data = monitor_client.metrics.list(
                         resource_uri=db_resource_uri,
                         timespan=f"{(datetime.now() - timedelta(days=lookback_days)).isoformat()}/{datetime.now().isoformat()}",
                         interval='P1D',
                         metricnames=metric_name,
                         aggregation="Average"
                     )

                     if metrics_data and metrics_data.value:
                         time_series = metrics_data.value[0].timeseries
                         if time_series and time_series[0].data:
                             valid_points = [d.average for d in time_series[0].data if d.average is not None]
                             if valid_points:
                                 avg_dtu = sum(valid_points) / len(valid_points)
                                 db_details["avg_dtu_percent"] = avg_dtu
                                 logger.debug(f"SQL DB (DTU) {db_name} on {server_name} avg DTU: {avg_dtu:.2f}%")
                             else:
                                  logger.warning(f"No valid data points found for metric '{metric_name}' for SQL DB (DTU) {db_name} on {server_name} in the timespan.")
                         else:
                              logger.warning(f"No time series data found for metric '{metric_name}' for SQL DB (DTU) {db_name} on {server_name} in the timespan.")
                     else:
                          logger.warning(f"No metric data returned for '{metric_name}' for SQL DB (DTU) {db_name} on {server_name}.")

                 except HttpResponseError as metric_error:
                      if metric_error.status_code == 429: # Too Many Requests
                          logger.warning(f"Metrics query for SQL DB (DTU) {db_name} on {server_name} throttled. Skipping.")
                          console.print(f"  - [yellow]Throttled:[/yellow] Skipping metrics for SQL DB {db_name} on {server_name}.")
                      else:
                          # Log other HTTP errors
                          logger.warning(f"Could not get metrics for SQL DB (DTU) {db_name} on {server_name}. Error: {metric_error}", exc_info=True)
                          console.print(f"  - [yellow]Warning:[/yellow] Could not get metrics for DTU SQL DB {db_name} on {server_name}.")
                 except Exception as metric_error:
                     logger.warning(f"Error processing metrics for SQL DB (DTU) {db_name} on {server_name}: {metric_error}", exc_info=True)
                     console.print(f"  - [yellow]Warning:[/yellow] Error processing metrics for DTU SQL DB {db_name} on {server_name}.")

                 if avg_dtu is not None:
                     if avg_dtu < dtu_threshold_percent:
                         console.print(f"  - [bold yellow]Low Usage:[/bold yellow] SQL DB {db_name} on {server_name} (Avg DTU: {avg_dtu:.1f}%) is below threshold ({dtu_threshold_percent}%).")
                         low_dtu_dbs.append(db_details)
                     else:
                         logger.info(f"SQL DB (DTU) {db_name} on {server_name} DTU usage OK (Avg: {avg_dtu:.1f}%)")
                 else:
                     console.print(f"  - [dim]No DTU data for SQL DB:[/dim] {db_name} on {server_name}")

            except Exception as e: # Catch errors in the outer loop for a specific DB
                 logger.error(f"Error processing SQL DB (DTU) {db_name} on {server_name}: {e}", exc_info=True)
                 console.print(f"  [red]Error:[/red] Could not process SQL DB {db_name} on {server_name}. Check logs.")

        console.print("\n--- SQL DTU Database Usage Analysis Summary ---")
        if low_dtu_dbs:
            console.print(f"  :warning: Found {len(low_dtu_dbs)} SQL DB(s) (DTU model) with avg DTU < {dtu_threshold_percent}%.")
        else:
            console.print(f"  :heavy_check_mark: No SQL DBs (DTU model) found with avg DTU < {dtu_threshold_percent}%.")

    except Exception as e:
        logger.error(f"Error checking for low DTU SQL databases: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for low DTU SQL Databases:[/] {e}")
    return low_dtu_dbs

def find_low_cpu_sql_vcore_databases(credential, subscription_id, cpu_threshold_percent=SQL_VCORE_LOW_CPU_THRESHOLD_PERCENT, lookback_days=METRIC_LOOKBACK_DAYS, console=None):
    """Find SQL Databases using the vCore-based model that have low average CPU utilization over the specified lookback period."""
    logger = logging.getLogger()
    logger.info("Starting analysis of SQL vCore databases for low CPU usage...")
    if console:
        console.print("[cyan]Analyzing SQL vCore databases for low CPU usage...[/]")

    try:
        # Initialize clients
        sql_client = SqlManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)

        # Get all SQL servers
        servers = list(sql_client.servers.list())
        if not servers:
            logger.info("No SQL servers found in the subscription.")
            return []

        low_cpu_dbs = []
        for server in servers:
            try:
                # Extract the resource group from the server ID
                # Format: /subscriptions/{sub}/resourceGroups/{rg}/providers/...
                resource_group_name = server.id.split('/')[4] if server.id and len(server.id.split('/')) > 4 else None
                
                if not resource_group_name:
                    logger.warning(f"Could not extract resource group name from server ID: {server.id}")
                    continue
                    
                # Get all databases for this server
                databases = list(sql_client.databases.list_by_server(resource_group_name, server.name))
                for db in databases:
                    try:
                        # Check if it's a vCore-based model
                        if not hasattr(db, 'sku') or not db.sku or not hasattr(db.sku, 'name') or not db.sku.name:
                            continue
                            
                        sku_name = db.sku.name.lower()
                        if not any(prefix in sku_name for prefix in ['bc_', 'gp_', 'hs_']):
                            continue

                        # Get CPU metrics
                        metrics_data = monitor_client.metrics.list(
                            resource_uri=db.id,
                            timespan=f"{(datetime.now() - timedelta(days=lookback_days)).isoformat()}/{datetime.now().isoformat()}",
                            interval='P1D',
                            metricnames='cpu_percent',
                            aggregation='Average'
                        )

                        if not metrics_data.value:
                            logger.warning(f"No CPU metrics found for database {db.name} in server {server.name}")
                            continue

                        avg_cpu = metrics_data.value[0].timeseries[0].data[-1].average if metrics_data.value[0].timeseries[0].data else None
                        if avg_cpu is None:
                            logger.warning(f"Could not calculate average CPU for database {db.name} in server {server.name}")
                            continue

                        if avg_cpu < cpu_threshold_percent:
                            low_cpu_dbs.append({
                                'id': db.id,
                                'name': db.name,
                                'resource_group': resource_group_name,
                                'location': server.location,
                                'sku': db.sku.name,
                                'tier': db.sku.tier if hasattr(db.sku, 'tier') else 'Unknown',
                                'avg_cpu_percent': avg_cpu
                            })
                            logger.info(f"Found low CPU vCore database: {db.name} (Avg CPU: {avg_cpu:.1f}%)")

                    except Exception as db_error:
                        logger.error(f"Error processing database {db.name} in server {server.name}: {db_error}")
                        continue

            except Exception as server_error:
                logger.error(f"Error processing server {server.name}: {server_error}")
                continue

        return low_cpu_dbs

    except Exception as e:
        logger.error(f"Error in find_low_cpu_sql_vcore_databases: {e}")
        return []

def find_idle_application_gateways(credential, subscription_id, lookback_days, idle_connection_threshold, console: Console):
    """Finds Application Gateways with average current connections below a threshold."""
    logger = logging.getLogger()
    logger.info(f"🚥 Checking Application Gateways for avg current connections < {idle_connection_threshold} over the last {lookback_days} days...")
    console.print(f"\n🚥 Checking Application Gateways for avg current connections < {idle_connection_threshold} over the last {lookback_days} days...")
    idle_gateways = []
    monitor_client = None # Initialize outside try block
    try:
        network_client = NetworkManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)

        # Get the correct timespan format
        timespan = _get_iso8601_timespan(lookback_days)
        logger.debug(f"Using timespan for App Gateway metrics: {timespan}")

        gateways = list(network_client.application_gateways.list_all())

        if not gateways:
            console.print("  ℹ No Application Gateways found to analyze.")
            return []

        console.print(f"  - Found {len(gateways)} Application Gateways to analyze...")

        for gw in gateways:
            gw_resource_uri = gw.id # For error reporting
            gw_name = gw.name # For error reporting
            gw_details = None # Initialize
            try:
                 # Extract RG name safely
                 rg_name = None
                 try:
                     rg_name = gw.id.split('/')[4]
                 except IndexError:
                     logger.warning(f"Could not parse resource group for App Gateway {gw_name}. Skipping metrics check.")
                     continue

                 gw_details = {
                     "name": gw.name,
                     "id": gw.id,
                     "resource_group": rg_name,
                     "location": gw.location,
                     "tier": gw.sku.tier if gw.sku else 'Unknown',
                     "sku": gw.sku.name if gw.sku else 'Unknown',
                     "avg_current_connections": None
                 }
                 avg_connections = None
                 # Metric for current connections (adjust if needed based on exact metric name)
                 # Common names: 'CurrentConnections', 'TotalRequests', 'Throughput'
                 # Let's use 'CurrentConnections' as a likely candidate for 'idle'
                 metric_name = "CurrentConnections"

                 try:
                     metrics_data = monitor_client.metrics.list(
                         resource_uri=gw_resource_uri,
                         timespan=timespan, # Use correct timespan format
                         interval="PT1H",
                         metricnames=metric_name,
                         aggregation="Average"
                     )

                     if metrics_data and metrics_data.value:
                         time_series = metrics_data.value[0].timeseries
                         if time_series and time_series[0].data:
                             valid_points = [d.average for d in time_series[0].data if d.average is not None]
                             if valid_points:
                                 avg_connections = sum(valid_points) / len(valid_points)
                                 gw_details["avg_current_connections"] = avg_connections
                                 logger.debug(f"App Gateway {gw_name} avg connections: {avg_connections:.2f}")
                             else:
                                  logger.warning(f"No valid data points found for metric '{metric_name}' for App Gateway {gw_name} in the timespan.")
                         else:
                              logger.warning(f"No time series data found for metric '{metric_name}' for App Gateway {gw_name} in the timespan.")
                     else:
                          logger.warning(f"No metric data returned for '{metric_name}' for App Gateway {gw_name}.")

                 except HttpResponseError as metric_error:
                      if metric_error.status_code == 429: # Too Many Requests
                          logger.warning(f"Metrics query for App Gateway {gw_name} throttled. Skipping.")
                          console.print(f"  - [yellow]Throttled:[/yellow] Skipping metrics for App Gateway {gw_name}.")
                      else:
                          # Log other HTTP errors
                          logger.warning(f"Could not get metrics for App Gateway {gw_name}. Error: {metric_error}", exc_info=True)
                          console.print(f"  - [yellow]Warning:[/yellow] Could not get metrics for App Gateway {gw_name}.")
                 except Exception as metric_error:
                     logger.warning(f"Error processing metrics for App Gateway {gw_name}: {metric_error}", exc_info=True)
                     console.print(f"  - [yellow]Warning:[/yellow] Error processing metrics for App Gateway {gw_name}.")

                 if avg_connections is not None:
                     if avg_connections < idle_connection_threshold:
                         console.print(f"  - [bold yellow]Idle:[/bold yellow] App Gateway {gw_name} (Avg Connections: {avg_connections:.1f}) is below threshold ({idle_connection_threshold}).")
                         idle_gateways.append(gw_details)
                     else:
                          logger.info(f"App Gateway {gw_name} connection usage OK (Avg: {avg_connections:.1f})")
                 else:
                     console.print(f"  - [dim]No connection data for App Gateway:[/dim] {gw_name}")

            except Exception as e: # Catch errors in the outer loop for a specific Gateway
                logger.error(f"Error processing App Gateway {gw_name}: {e}", exc_info=True)
                console.print(f"  [red]Error:[/red] Could not process App Gateway {gw_name}. Check logs.")

        console.print("\n--- Application Gateway Usage Analysis Summary ---")
        if idle_gateways:
            console.print(f"  :warning: Found {len(idle_gateways)} Application Gateway(s) with avg connections < {idle_connection_threshold}.")
        else:
            console.print(f"  :heavy_check_mark: No Application Gateways found with avg connections < {idle_connection_threshold}.")

    except Exception as e:
        logger.error(f"Error checking for idle Application Gateways: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for idle Application Gateways:[/] {e}")
    return idle_gateways

def find_low_usage_web_apps(credential, subscription_id, cpu_threshold_percent, lookback_days, console: Console):
    """Finds Web Apps (running on Basic+ plans) with low average CPU utilization."""
    logger = logging.getLogger()
    logger.info(f"💻 Checking Web Apps (on Basic+ plans) for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...")
    console.print(f"\n💻 Checking Web Apps (on Basic+ plans) for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...")
    low_usage_apps = []
    monitor_client = None # Initialize outside try block
    try:
        web_client = WebSiteManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)

        # Get the correct timespan format
        timespan = _get_iso8601_timespan(lookback_days)
        logger.debug(f"Using timespan for Web App metrics: {timespan}")

        web_apps = list(web_client.web_apps.list())
        apps_to_check = []
        plan_cache = {} # Cache plan details to avoid redundant lookups

        console.print(f"  - Found {len(web_apps)} total Web Apps. Analyzing those on Basic+ plans...")
        # Filter apps based on their App Service Plan tier
        for app in web_apps:
            # Check if app has a valid server farm ID
            if not app.server_farm_id:
                 logger.debug(f"Skipping Web App {app.name} as it has no associated App Service Plan ID.")
                 continue

            plan_id = app.server_farm_id
            plan_info = plan_cache.get(plan_id)

            if not plan_info:
                try:
                    # Extract plan RG and name from ID
                    parts = plan_id.split('/')
                    plan_rg = parts[4]
                    plan_name = parts[8]
                    plan = web_client.app_service_plans.get(plan_rg, plan_name)
                    if plan and plan.sku and plan.sku.tier:
                        plan_info = {'tier': plan.sku.tier.lower(), 'name': plan.name}
                        plan_cache[plan_id] = plan_info
                    else:
                        plan_cache[plan_id] = {'tier': 'unknown', 'name': plan_name} # Cache unknown tier
                        logger.warning(f"Could not determine tier for plan {plan_name} ({plan_id}). Skipping apps on this plan.")
                        continue
                except Exception as plan_err:
                    logger.error(f"Error fetching details for plan {plan_id} for app {app.name}: {plan_err}", exc_info=True)
                    plan_cache[plan_id] = {'tier': 'error', 'name': 'Unknown'} # Cache error state
                    console.print(f"  [yellow]Warning:[/yellow] Could not get plan details for app {app.name}. Skipping.")
                    continue

            # Check if the plan tier is relevant (Basic or higher)
            if plan_info and plan_info.get('tier') not in ['free', 'shared', 'dynamic', 'unknown', 'error']:
                apps_to_check.append((app, plan_info)) # Append app and its cached plan info

        if not apps_to_check:
            console.print("  ℹ No Web Apps found running on relevant App Service Plans (Basic or higher).")
            return []

        console.print(f"  - Found {len(apps_to_check)} Web Apps (on Basic+ plans) to analyze...")

        for app, plan_info in apps_to_check:
            app_resource_uri = app.id # For error reporting
            app_name = app.name # For error reporting
            app_details = None # Initialize
            try:
                # Extract RG name safely
                rg_name = None
                try:
                    rg_name = app.id.split('/')[4]
                except IndexError:
                    logger.warning(f"Could not parse resource group for Web App {app_name}. Skipping metrics check.")
                    continue

                app_details = {
                     "name": app.name,
                     "id": app.id,
                     "resource_group": rg_name,
                     "location": app.location,
                     "plan_name": plan_info.get('name', 'Unknown'),
                     "plan_tier": plan_info.get('tier', 'Unknown').capitalize(), # Capitalize tier for display
                     "avg_cpu_percent": None
                }
                avg_cpu = None
                # Metrics for Web Apps can be 'CpuTime' (total seconds) or 'AverageCpuPercentage' (often requires specific diagnostics settings)
                # Let's try 'CpuTime' and calculate percentage based on plan capacity if needed, or use 'AverageCpuPercentage' if available
                # Preferred metric name: 'AverageCpuPercentage' (if enabled via Diagnostics) or 'CpuPercentage'
                metric_name = "CpuPercentage" # Common metric for Web App CPU %

                try:
                    metrics_data = monitor_client.metrics.list(
                        resource_uri=app_resource_uri,
                        timespan=timespan, # Use correct timespan format
                        interval="PT1H",
                        metricnames=metric_name,
                        aggregation="Average"
                    )
                    # Process metrics_data if successful
                    if metrics_data and metrics_data.value:
                         valid_points = [d.average for d in metrics_data.value[0].timeseries[0].data if d.average is not None]
                         if valid_points:
                             avg_cpu = sum(valid_points) / len(valid_points)
                             app_details["avg_cpu_percent"] = avg_cpu
                             logger.debug(f"Web App {app_name} avg CPU: {avg_cpu:.2f}%")
                         else:
                              logger.warning(f"No valid data points found for metric '{metric_name}' for Web App {app_name} in the timespan.")
                    else:
                         logger.warning(f"No metric data returned for '{metric_name}' for Web App {app_name}.")

                except HttpResponseError as metric_error:
                     # Check for specific "Metric configuration not found" error
                     is_metric_not_found_error = (
                         metric_error.status_code == 400 and
                         metric_error.error and
                         hasattr(metric_error.error, 'message') and
                         metric_error.error.message and
                         "failed to find metric configuration" in metric_error.error.message.lower()
                     )

                     if is_metric_not_found_error:
                         # Log a concise warning and continue gracefully
                         logger.warning(f"Metric '{metric_name}' not found for Web App {app_name}. It might need to be enabled in Diagnostics settings.")
                         console.print(f"  - [yellow]Warning:[/yellow] Metric '{metric_name}' not found for Web App {app_name}. (Enable in Diagnostics?)")
                     elif metric_error.status_code == 429: # Too Many Requests
                         logger.warning(f"Metrics query for Web App {app_name} throttled. Skipping.")
                         console.print(f"  - [yellow]Throttled:[/yellow] Skipping metrics for Web App {app_name}.")
                     else:
                         # Log other HTTP errors with traceback for debugging
                         logger.warning(f"Could not get metrics for Web App {app_name}. Error: {metric_error}", exc_info=True)
                         console.print(f"  - [yellow]Warning:[/yellow] Could not get metrics for Web App {app_name} (Error: {metric_error.status_code}). Check logs.")
                except Exception as metric_error:
                    # Log other unexpected errors during metric processing
                    logger.warning(f"Error processing metrics for Web App {app_name}: {metric_error}", exc_info=True)
                    console.print(f"  - [yellow]Warning:[/yellow] Error processing metrics for Web App {app_name}. Check logs.")


                if avg_cpu is not None:
                    if avg_cpu < cpu_threshold_percent:
                        console.print(f"  - [bold yellow]Low Usage:[/bold yellow] Web App {app_name} (Avg CPU: {avg_cpu:.1f}%) is below threshold ({cpu_threshold_percent}%).")
                        low_usage_apps.append(app_details)
                    else:
                         logger.info(f"Web App {app_name} CPU usage OK (Avg: {avg_cpu:.1f}%)")
                else:
                    console.print(f"  - [dim]No CPU data for Web App:[/dim] {app_name}")

            except Exception as e: # Catch errors in the outer loop for a specific App
                logger.error(f"Error processing Web App {app_name}: {e}", exc_info=True)
                console.print(f"  [red]Error:[/red] Could not process Web App {app_name}. Check logs.")

        console.print("\n--- Web App Usage Analysis Summary ---")
        if low_usage_apps:
            console.print(f"  :warning: Found {len(low_usage_apps)} Web App(s) (on Basic+ plans) with avg CPU < {cpu_threshold_percent}%.")
        else:
            console.print(f"  :heavy_check_mark: No running Web Apps (on Basic+ plans) found with avg CPU < {cpu_threshold_percent}%.")

    except Exception as e:
        logger.error(f"Error checking for low usage Web Apps: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for low usage Web Apps:[/] {e}")
    return low_usage_apps

def find_orphaned_nsgs(credential, subscription_id, console: Console):
    """Finds Network Security Groups not associated with any NIC or subnet."""
    logger = logging.getLogger()
    logger.info("🛡 Checking for orphaned Network Security Groups (NSGs)...")
    console.print("\n🛡 Checking for orphaned Network Security Groups (NSGs)...") # Keep simple print
    orphaned_nsgs = []
    try:
        network_client = NetworkManagementClient(credential, subscription_id)
        all_nsgs = list(network_client.network_security_groups.list_all())
        all_nics = list(network_client.network_interfaces.list_all())
        all_subnets = []
        vnets = list(network_client.virtual_networks.list_all())
        for vnet in vnets:
            subnets = list(network_client.subnets.list(vnet.id.split('/')[4], vnet.name))
            all_subnets.extend(subnets)

        all_nsg_ids = set(nsg.id for nsg in all_nsgs)
        associated_nsg_ids = set(nic.network_security_group.id for nic in all_nics if nic.network_security_group)
        associated_nsg_ids.update(subnet.network_security_group.id for subnet in all_subnets if subnet.network_security_group)

        orphaned_nsg_ids = all_nsg_ids - associated_nsg_ids

        if not orphaned_nsg_ids:
            console.print("  :heavy_check_mark: No orphaned NSGs found.")
        else:
            console.print(f"  :warning: Found {len(orphaned_nsg_ids)} potentially orphaned NSG(s).")
            # Retrieve details for orphaned NSGs
            for nsg in all_nsgs:
                if nsg.id in orphaned_nsg_ids:
                    orphaned_nsgs.append({
                        "name": nsg.name,
                        "id": nsg.id,
                        "resource_group": nsg.id.split('/')[4],
                        "location": nsg.location
                    })

        return orphaned_nsgs

    except Exception as e:
        logging.error(f"Error checking for orphaned NSGs: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for orphaned NSGs:[/bold red] {e}")
        return []

def find_orphaned_route_tables(credential, subscription_id, console: Console):
    """Finds Route Tables not associated with any subnet."""
    logger = logging.getLogger()
    logger.info("🗺 Checking for orphaned Route Tables...")
    console.print("\n🗺 Checking for orphaned Route Tables...") # Keep simple print
    orphaned_rts = []
    try:
        network_client = NetworkManagementClient(credential, subscription_id)
        all_route_tables = list(network_client.route_tables.list_all())
        all_subnets = []
        vnets = list(network_client.virtual_networks.list_all())
        for vnet in vnets:
            subnets = list(network_client.subnets.list(vnet.id.split('/')[4], vnet.name))
            all_subnets.extend(subnets)

        all_route_table_ids = set(rt.id for rt in all_route_tables)
        associated_route_table_ids = set(subnet.route_table.id for subnet in all_subnets if subnet.route_table)

        orphaned_route_table_ids = all_route_table_ids - associated_route_table_ids

        if not orphaned_route_table_ids:
            console.print("  :heavy_check_mark: No orphaned Route Tables found.")
        else:
            console.print(f"  :warning: Found {len(orphaned_route_table_ids)} potentially orphaned Route Table(s).")
            # Retrieve details for orphaned route tables
            for rt in all_route_tables:
                if rt.id in orphaned_route_table_ids:
                    orphaned_rts.append({
                        "name": rt.name,
                        "id": rt.id,
                        "resource_group": rt.id.split('/')[4],
                        "location": rt.location
                    })

        return orphaned_rts

    except Exception as e:
        logging.error(f"Error checking for orphaned Route Tables: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for orphaned Route Tables:[/bold red] {e}")
        return [] 