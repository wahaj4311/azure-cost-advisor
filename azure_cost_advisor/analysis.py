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
        cost_client = CostManagementClient(credential, subscription_id) # Pass sub_id here
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
            try:
                # Extract RG name safely
                try:
                     rg_name = vm.id.split('/')[4]
                except IndexError:
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
                    stopped_vms.append({
                        "name": vm.name, 
                        "id": vm.id, 
                        "resource_group": rg_name,
                        "location": vm.location
                    })
            except Exception as iv_error:
                 # Log specific error for instance view failure
                 logging.warning(f"Could not get instance view for VM {vm.name} in RG {rg_name}. Error: {iv_error}", exc_info=True)
                 console.print(f"  [yellow]Warning:[/][dim] Could not get instance view for VM {vm.name}. Skipping status check.[/]")

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
    console.print(f"\n📉 Checking running VMs for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...") # Keep simple print
    low_cpu_vms = []
    try:
        compute_client = ComputeManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)
        all_vms = list(compute_client.virtual_machines.list_all())
        for vm in all_vms:
            try:
                rg_name = vm.id.split('/')[4]
                instance_view = compute_client.virtual_machines.instance_view(rg_name, vm.name)
                power_state = "unknown"
                for status in instance_view.statuses:
                    if status.code and status.code.startswith("PowerState/"):
                        power_state = status.code.split('/')[-1]
                        break
                if power_state == "running":
                    vm_details = {
                        "name": vm.name, 
                        "id": vm.id, 
                        "resource_group": rg_name,
                        "location": vm.location, 
                        "avg_cpu_percent": None,
                        "size": vm.hardware_profile.vm_size if vm.hardware_profile else "Unknown" # Add VM size
                    }
                    
                    if vm_details['avg_cpu_percent'] is None:
                        avg_cpu = None
                        metric_name = "Percentage CPU"
                        try:
                            metrics_data = monitor_client.metrics.list(
                                resource_uri=vm.id,
                                timespan=f"PT{lookback_days}D",
                                interval="PT1H",
                                metricnames=metric_name,
                                aggregation="Average" 
                            )
                            if metrics_data.value:
                                metric = metrics_data.value[0]
                                if metric.timeseries and metric.timeseries[0].data:
                                    total_cpu = 0
                                    count = 0
                                    for point in metric.timeseries[0].data:
                                        if point.average is not None:
                                            total_cpu += point.average
                                            count += 1
                                    if count > 0:
                                        avg_cpu = total_cpu / count
                        except Exception as metric_error:
                            logging.warning(f"Could not get metrics for VM {vm.name}. Error: {metric_error}", exc_info=True)
                            console.print(f"  - [yellow]Warning:[/yellow] Could not get metrics for VM {vm.name}.")

                        if avg_cpu is not None:
                            vm_details['avg_cpu_percent'] = avg_cpu
                            if avg_cpu < cpu_threshold_percent:
                                low_cpu_vms.append(vm_details)
                                console.print(f"  - [yellow]Low CPU VM:[/yellow] {vm_details['name']} (Avg CPU: {vm_details['avg_cpu_percent']:.2f}%, Size: {vm_details['size']})")
                            else:
                                logging.info(f"VM {vm_details['name']} CPU usage OK (Avg: {vm_details['avg_cpu_percent']:.2f}%)")
                                # console.print(f"  - [green]OK CPU VM:[/green] {vm_details['name']} (Avg CPU: {vm_details['avg_cpu_percent']:.2f}%)") # Too verbose?
                        else:
                             logging.warning(f"No valid CPU metric data found for VM {vm_details['name']} in the specified timespan.")
                             console.print(f"  - [dim]No CPU data for VM:[/dim] {vm_details['name']}")

            except Exception as vm_error:
                logging.warning(f"Error processing VM {vm.name}: {vm_error}", exc_info=True)
                console.print(f"  - [yellow]Warning:[/yellow] Error processing VM {vm.name}.")

        console.print("\n--- VM Usage Analysis Summary ---")
        if not low_cpu_vms:
            console.print(f"  :heavy_check_mark: No running VMs found with avg CPU < {cpu_threshold_percent}%.")
        else:
            console.print(f"  :warning: Found {len(low_cpu_vms)} running VM(s) with avg CPU < {cpu_threshold_percent}%.")

        return low_cpu_vms 

    except Exception as e:
        logging.error(f"Error checking for underutilized VMs: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for underutilized VMs:[/bold red] {e}")
        return [] 

def find_low_usage_app_service_plans(credential, subscription_id, cpu_threshold_percent, lookback_days, console: Console):
    """Finds App Service Plans (Basic tier+) with low average CPU usage."""
    logger = logging.getLogger()
    logger.info(f"📉 Checking App Service Plans (Basic tier+) for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...")
    console.print(f"\n📉 Checking App Service Plans (Basic tier+) for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...") # Keep simple print
    low_usage_asps = []
    try:
        web_client = WebSiteManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)
        plans = list(web_client.app_service_plans.list())
        for plan in plans:
            # Filter out Free/Shared/Consumption plans
            if plan.sku and plan.sku.tier and plan.sku.tier.lower() not in ['free', 'shared', 'dynamic']: 
                 low_usage_asps.append(plan)
            else:
                 logging.info(f"Skipping metric check for plan {plan.name} (Tier: {plan.sku.tier if plan.sku else 'Unknown'})")
                 # console.print(f"  - [dim]Skipping plan {plan.name} (Tier: {plan.sku.tier if plan.sku else 'Unknown'})[/dim]")

        if not low_usage_asps:
            console.print("  :information_source: No App Service Plans found in relevant tiers (Basic+) to check metrics for.")
            return []
        else:
             console.print(f"  - Found {len(low_usage_asps)} App Service Plans in relevant tiers to analyze...")

        # --- Query Metrics for Relevant Plans --- 
        processed_count = 0
        for plan in low_usage_asps:
            processed_count += 1
            if processed_count % 5 == 0:
                console.status(f"[cyan]Querying CPU metrics ({processed_count}/{len(low_usage_asps)})...[/]")

            plan_resource_uri = plan.id
            avg_cpu = None
            metric_name = "CpuPercentage" # Metric name for ASP CPU
            try:
                metrics_data = monitor_client.metrics.list(
                    resource_uri=plan_resource_uri,
                    timespan=f"PT{lookback_days}D",
                    interval="PT1H",
                    metricnames=metric_name,
                    aggregation="Average" 
                )
                if metrics_data.value:
                    metric = metrics_data.value[0]
                    if metric.timeseries and metric.timeseries[0].data:
                        total_cpu = 0
                        count = 0
                        for point in metric.timeseries[0].data:
                            if point.average is not None:
                                total_cpu += point.average
                                count += 1
                        if count > 0:
                            avg_cpu = total_cpu / count

                plan_details = {
                    "name": plan.name, 
                    "id": plan.id, 
                    "resource_group": plan.id.split('/')[4],
                    "location": plan.location, 
                    "avg_cpu_percent": avg_cpu,
                    "sku": plan.sku.name if plan.sku else "Unknown",
                    "tier": plan.sku.tier if plan.sku else "Unknown"
                }

                if avg_cpu is not None:
                    if avg_cpu < cpu_threshold_percent:
                        low_usage_asps.append(plan_details)
                        console.print(f"  - [yellow]Low CPU ASP:[/yellow] {plan_details['name']} (Avg CPU: {avg_cpu:.2f}%, SKU: {plan_details['sku']})")
                    else:
                        logging.info(f"ASP {plan_details['name']} CPU usage OK (Avg: {avg_cpu:.2f}%)")
                        # console.print(f"  - [green]OK CPU ASP:[/green] {plan_details['name']} (Avg CPU: {avg_cpu:.2f}%)")
                else:
                     logging.warning(f"No valid CPU metric data found for ASP {plan_details['name']} in the specified timespan.")
                     console.print(f"  - [dim]No CPU data for ASP:[/dim] {plan_details['name']}")

            except Exception as metric_error:
                # Handle throttling specifically if possible
                if "429" in str(metric_error) or "throttled" in str(metric_error).lower():
                     logging.warning(f"Metrics query for ASP {plan_details['name']} throttled. Skipping.")
                     console.print(f"  - [yellow]Throttled:[/yellow] Skipping metrics for ASP {plan_details['name']}.")
                else:
                     logging.warning(f"Could not get metrics for ASP {plan_details['name']}. Error: {metric_error}", exc_info=True)
                     console.print(f"  - [yellow]Warning:[/yellow] Could not get metrics for ASP {plan_details['name']}.")

        console.print("\n--- App Service Plan Usage Analysis Summary ---")
        if not low_usage_asps:
            console.print(f"  :heavy_check_mark: No ASPs (Basic+) found with avg CPU < {cpu_threshold_percent}%.")
        else:
            console.print(f"  :warning: Found {len(low_usage_asps)} ASP(s) (Basic+) with avg CPU < {cpu_threshold_percent}%.")

        return low_usage_asps

    except Exception as e:
        logging.error(f"Error checking for low usage App Service Plans: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for low usage App Service Plans:[/bold red] {e}")
        return []

def find_low_dtu_sql_databases(credential, subscription_id, dtu_threshold_percent, lookback_days, console: Console):
    """Finds SQL Databases (DTU model) with low average DTU usage."""
    logger = logging.getLogger()
    logger.info(f"📉 Checking SQL Databases (DTU model) for avg DTU < {dtu_threshold_percent}% over the last {lookback_days} days...")
    console.print(f"\n📉 Checking SQL Databases (DTU model) for avg DTU < {dtu_threshold_percent}% over the last {lookback_days} days...") # Keep simple print
    low_dtu_dbs = []
    dtu_dbs_to_check = [] # Initialize the list to check

    try:
        sql_client = SqlManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)
        
        servers = list(sql_client.servers.list())
        for server in servers:
            try:
                rg_name = server.id.split('/')[4]
                databases = list(sql_client.databases.list_by_server(rg_name, server.name))
                for db in databases:
                    # Check if it's a DTU model database
                    if db.sku and db.sku.tier and db.sku.tier.lower() in ['basic', 'standard', 'premium']:
                        dtu_dbs_to_check.append({'db': db, 'rg': rg_name})
            except Exception as db_list_error:
                 logging.warning(f"Could not list databases for server {server.name}. Error: {db_list_error}", exc_info=True)
                 console.print(f"  - [yellow]Warning:[/yellow] Could not list databases for server {server.name}.")
        
        if not dtu_dbs_to_check: # Check if the filtered list is empty
            console.print("  :information_source: No SQL Databases (DTU model) found to check metrics for.")
            return []
        else:
             console.print(f"  - Found {len(dtu_dbs_to_check)} SQL Databases (DTU model) to analyze...")

        # --- Query Metrics for Relevant DTU Databases ---
        for db_info in dtu_dbs_to_check:
            db = db_info['db']
            rg_name = db_info['rg']
            db_resource_uri = db.id
            avg_dtu = None
            metric_name = "dtu_consumption_percent"

            try:
                metrics_data = monitor_client.metrics.list(
                    resource_uri=db_resource_uri,
                    timespan=f"PT{lookback_days}D",
                    interval="PT1H",
                    metricnames=metric_name,
                    aggregation="Average"
                )
                if metrics_data.value:
                    metric = metrics_data.value[0]
                    if metric.timeseries and metric.timeseries[0].data:
                        total_dtu = 0
                        count = 0
                        for point in metric.timeseries[0].data:
                            if point.average is not None:
                                total_dtu += point.average
                                count += 1
                        if count > 0:
                            avg_dtu = total_dtu / count
                
                db_details = {
                    "name": db.name,
                    "id": db.id,
                    "resource_group": rg_name,
                    "location": db.location,
                    "avg_dtu_percent": avg_dtu,
                    "tier": db.sku.tier if db.sku else "Unknown",
                    "sku": db.sku.name if db.sku else "Unknown",
                    "family": db.sku.family if db.sku else "Unknown",
                    "capacity": db.sku.capacity if db.sku else "Unknown"
                }

                if avg_dtu is not None:
                    if avg_dtu < dtu_threshold_percent:
                        low_dtu_dbs.append(db_details)
                        console.print(f"  - [yellow]Low DTU DB:[/yellow] {db_details['name']} (Avg DTU: {avg_dtu:.2f}%, Tier: {db_details['tier']})")
                    else:
                        logging.info(f"SQL DB (DTU) {db_details['name']} DTU usage OK (Avg: {avg_dtu:.2f}%)")
                else:
                    logging.warning(f"No valid DTU metric data found for DB {db_details['name']} in the specified timespan.")
                    console.print(f"  - [dim]No DTU data for DB:[/dim] {db_details['name']}")

            except Exception as metric_error:
                # Handle throttling
                if "429" in str(metric_error) or "throttled" in str(metric_error).lower():
                     logging.warning(f"Metrics query for SQL DB {db.name} on {rg_name} throttled. Skipping.")
                     console.print(f"  - [yellow]Throttled:[/yellow] Skipping metrics for SQL DB {db.name} on {rg_name}.")
                else:
                     logging.warning(f"Could not get metrics for SQL DB {db.name} on {rg_name}. Error: {metric_error}", exc_info=True)
                     console.print(f"  - [yellow]Warning:[/yellow] Could not get metrics for SQL DB {db.name} on {rg_name}.")

        console.print("\n--- SQL DTU Database Usage Analysis Summary ---")
        if not low_dtu_dbs:
            console.print(f"  :heavy_check_mark: No SQL DBs (DTU model) found with avg DTU < {dtu_threshold_percent}%.")
        else:
            console.print(f"  :warning: Found {len(low_dtu_dbs)} SQL DB(s) (DTU model) with avg DTU < {dtu_threshold_percent}%.")
        
        return low_dtu_dbs

    except Exception as e:
        logging.error(f"Error checking for low DTU SQL Databases: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for low DTU SQL Databases:[/] {e}")
        return []

def find_low_cpu_sql_vcore_databases(credential, subscription_id, cpu_threshold_percent, lookback_days, console: Console):
    """Finds SQL Databases (vCore model) with low average CPU usage."""
    logger = logging.getLogger()
    logger.info(f"📉 Checking SQL Databases (vCore model) for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...")
    console.print(f"\n📉 Checking SQL Databases (vCore model) for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...") # Keep simple print
    low_cpu_dbs = []
    vcore_dbs_to_check = [] # Initialize the list to check

    try:
        sql_client = SqlManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)
        
        servers = list(sql_client.servers.list())
        for server in servers:
             try:
                rg_name = server.id.split('/')[4]
                databases = list(sql_client.databases.list_by_server(rg_name, server.name))
                for db in databases:
                    # Check if it's a vCore model database (e.g., GeneralPurpose, BusinessCritical, Hyperscale)
                    if db.sku and db.sku.tier and db.sku.tier.lower() not in ['basic', 'standard', 'premium', 'system']: # Exclude DTU and system
                        vcore_dbs_to_check.append({'db': db, 'rg': rg_name})
             except Exception as db_list_error:
                 logging.warning(f"Could not list databases for server {server.name} (vCore check). Error: {db_list_error}", exc_info=True)
                 console.print(f"  - [yellow]Warning:[/yellow] Could not list databases for server {server.name} (vCore check).")

        if not vcore_dbs_to_check: # Check if the filtered list is empty
            console.print("  :information_source: No SQL Databases (vCore model) found to check metrics for.")
            return []
        else:
             console.print(f"  - Found {len(vcore_dbs_to_check)} SQL Databases (vCore model) to analyze...")

        # --- Query Metrics for Relevant vCore Databases ---
        for db_info in vcore_dbs_to_check:
            db = db_info['db']
            rg_name = db_info['rg']
            db_resource_uri = db.id
            avg_cpu = None
            metric_name = "cpu_percent"

            try:
                metrics_data = monitor_client.metrics.list(
                    resource_uri=db_resource_uri,
                    timespan=f"PT{lookback_days}D",
                    interval="PT1H",
                    metricnames=metric_name,
                    aggregation="Average"
                )
                if metrics_data.value:
                    metric = metrics_data.value[0]
                    if metric.timeseries and metric.timeseries[0].data:
                        total_cpu = 0
                        count = 0
                        for point in metric.timeseries[0].data:
                            if point.average is not None:
                                total_cpu += point.average
                                count += 1
                        if count > 0:
                            avg_cpu = total_cpu / count

                db_details = {
                    "name": db.name,
                    "id": db.id,
                    "resource_group": rg_name,
                    "location": db.location,
                    "avg_cpu_percent": avg_cpu,
                    "tier": db.sku.tier if db.sku else "Unknown",
                    "sku": db.sku.name if db.sku else "Unknown",
                    "family": db.sku.family if db.sku else "Unknown",
                    "capacity": db.sku.capacity if db.sku else "Unknown"
                }

                if avg_cpu is not None:
                    if avg_cpu < cpu_threshold_percent:
                        low_cpu_dbs.append(db_details)
                        console.print(f"  - [yellow]Low CPU vCore DB:[/yellow] {db_details['name']} (Avg CPU: {avg_cpu:.2f}%, Tier: {db_details['tier']})")
                    else:
                        logging.info(f"SQL DB (vCore) {db_details['name']} CPU usage OK (Avg: {avg_cpu:.2f}%)")
                else:
                    logging.warning(f"No valid CPU metric data found for vCore DB {db_details['name']} in the specified timespan.")
                    console.print(f"  - [dim]No CPU data for vCore DB:[/dim] {db_details['name']}")

            except Exception as metric_error:
                # Handle throttling
                if "429" in str(metric_error) or "throttled" in str(metric_error).lower():
                     logging.warning(f"Metrics query for SQL DB (vCore) {db.name} on {rg_name} throttled. Skipping.")
                     console.print(f"  - [yellow]Throttled:[/yellow] Skipping metrics for vCore SQL DB {db.name} on {rg_name}.")
                else:
                     logging.warning(f"Could not get metrics for SQL DB (vCore) {db.name} on {rg_name}. Error: {metric_error}", exc_info=True)
                     console.print(f"  - [yellow]Warning:[/yellow] Could not get metrics for vCore SQL DB {db.name} on {rg_name}.")

        console.print("\n--- SQL vCore Database Usage Analysis Summary ---")
        if not low_cpu_dbs:
             console.print(f"  :heavy_check_mark: No SQL DBs (vCore model) found with avg CPU < {cpu_threshold_percent}%.")
        else:
             console.print(f"  :warning: Found {len(low_cpu_dbs)} SQL DB(s) (vCore model) with avg CPU < {cpu_threshold_percent}%.")

        return low_cpu_dbs

    except Exception as e:
        logging.error(f"Error checking for low CPU vCore SQL Databases: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for low CPU vCore SQL Databases:[/] {e}")
        return []

# --- Add Idle Application Gateway Detection ---
def find_idle_application_gateways(credential, subscription_id, lookback_days, idle_connection_threshold, console: Console):
    """Finds Application Gateways with low average connections over a period."""
    logger = logging.getLogger()
    logger.info(f"🚥 Checking Application Gateways for avg current connections < {idle_connection_threshold} over the last {lookback_days} days...")
    console.print(f"\n🚥 Checking Application Gateways for avg current connections < {idle_connection_threshold} over the last {lookback_days} days...")
    idle_gateways = []

    try:
        network_client = NetworkManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)

        # --- Calculate timespan in ISO 8601 format ---
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=lookback_days)
        # Format specifically for Azure Monitor API (YYYY-MM-DDTHH:MM:SSZ)
        timespan_iso = f"{start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        # ---

        gateways = list(network_client.application_gateways.list_all())

        if not gateways:
             console.print("  :information_source: No Application Gateways found in the subscription.")
             return []
        else:
             gateways_to_check = gateways # Assign the list to check
             console.print(f"  - Found {len(gateways_to_check)} Application Gateways to analyze...")

        # --- Query Metrics for Gateways ---
        for gateway in gateways_to_check:
            gw_resource_uri = gateway.id
            avg_connections = None
            metric_name = "CurrentConnections" # Or Throughput? Connections is more direct for 'idle'

            try:
                metrics_data = monitor_client.metrics.list(
                    resource_uri=gw_resource_uri,
                    timespan=timespan_iso, # Use ISO 8601 format
                    interval="PT1H",
                    metricnames=metric_name,
                    aggregation="Average"
                )
                if metrics_data.value:
                    metric = metrics_data.value[0]
                    if metric.timeseries and metric.timeseries[0].data:
                        total_conn = 0
                        count = 0
                        for point in metric.timeseries[0].data:
                            if point.average is not None:
                                total_conn += point.average
                                count += 1
                        if count > 0:
                            avg_connections = total_conn / count
                
                gw_details = {
                    "name": gateway.name,
                    "id": gateway.id,
                    "resource_group": gateway.id.split('/')[4],
                    "location": gateway.location,
                    "avg_connections": avg_connections,
                    "sku": gateway.sku.name if gateway.sku else "Unknown",
                    "tier": gateway.sku.tier if gateway.sku else "Unknown"
                }

                if avg_connections is not None:
                    if avg_connections < idle_connection_threshold:
                        idle_gateways.append(gw_details)
                        console.print(f"  - [yellow]Idle App Gateway:[/yellow] {gw_details['name']} (Avg Connections: {avg_connections:.2f}, SKU: {gw_details['sku']})")
                    else:
                        logging.info(f"App Gateway {gw_details['name']} connection usage OK (Avg: {avg_connections:.2f})")
                else:
                     logging.warning(f"No valid connection metric data found for App Gateway {gw_details['name']} in the specified timespan.")
                     console.print(f"  - [dim]No connection data for App Gateway:[/dim] {gw_details['name']}")
            
            except Exception as metric_error:
                # Handle throttling
                if "429" in str(metric_error) or "throttled" in str(metric_error).lower():
                     logging.warning(f"Metrics query for App Gateway {gateway.name} throttled. Skipping.")
                     console.print(f"  - [yellow]Throttled:[/yellow] Skipping metrics for App Gateway {gateway.name}.")
                else:
                     logging.warning(f"Could not get metrics for App Gateway {gateway.name}. Error: {metric_error}", exc_info=True)
                     console.print(f"  - [yellow]Warning:[/yellow] Could not get metrics for App Gateway {gateway.name}.")

        console.print("\n--- Application Gateway Usage Analysis Summary ---")
        if not idle_gateways:
             console.print(f"  :heavy_check_mark: No Application Gateways found with avg connections < {idle_connection_threshold}.")
        else:
             console.print(f"  :warning: Found {len(idle_gateways)} Application Gateway(s) with avg connections < {idle_connection_threshold}.")

        return idle_gateways

    except Exception as e:
        logging.error(f"Error checking for idle Application Gateways: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for idle Application Gateways:[/] {e}")
        return []

# --- Add Low Usage Web App Detection ---
def find_low_usage_web_apps(credential, subscription_id, cpu_threshold_percent, lookback_days, console: Console):
    """Finds individual Web Apps (on Basic+ plans) with low average CPU usage."""
    logger = logging.getLogger()
    logger.info(f"💻 Checking Web Apps (on Basic+ plans) for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...")
    console.print(f"\n💻 Checking Web Apps (on Basic+ plans) for avg CPU < {cpu_threshold_percent}% over the last {lookback_days} days...") # Keep simple print
    low_usage_apps = []
    apps_to_check = [] # Initialize list to check

    try:
        web_client = WebSiteManagementClient(credential, subscription_id)
        monitor_client = MonitorManagementClient(credential, subscription_id)
        
        apps = list(web_client.web_apps.list())
        plans = {plan.id.lower(): plan for plan in web_client.app_service_plans.list()}

        for app in apps:
            try:
                plan_id = app.server_farm_id
                if plan_id and plan_id.lower() in plans:
                    plan = plans[plan_id.lower()]
                    # Check if the plan is in a relevant tier
                    if plan.sku and plan.sku.tier and plan.sku.tier.lower() not in ['free', 'shared', 'dynamic']:
                        apps_to_check.append({'app': app, 'plan': plan}) # Store app and its plan
                else:
                    logging.debug(f"App {app.name} has no plan ID or plan not found in cache. Skipping.")
            except Exception as plan_check_error:
                logging.warning(f"Error checking plan for app {app.name}: {plan_check_error}", exc_info=True)

        if not apps_to_check: # Check filtered list
            console.print("  :information_source: No running Web Apps (on Basic+ plans) found to check metrics for.")
            return []
        else:
             console.print(f"  - Found {len(apps_to_check)} Web Apps (on Basic+ plans) to analyze...")

        # --- Query Metrics for Relevant Web Apps ---
        for app_info in apps_to_check:
            app = app_info['app']
            plan = app_info['plan'] # Get the associated plan
            app_resource_uri = app.id
            avg_cpu_percent = None
            metric_name = "CpuTime" # For Web Apps, it's often CpuTime (total seconds), need conversion or use Percentage CPU if available

            try:
                # Attempt Percentage CPU first as it's simpler
                metrics_data = monitor_client.metrics.list(
                    resource_uri=app_resource_uri,
                    timespan=f"PT{lookback_days}D",
                    interval="PT1H",
                    metricnames="Percentage CPU", # Try this metric first
                    aggregation="Average"
                )
                
                if metrics_data.value:
                    metric = metrics_data.value[0]
                    if metric.timeseries and metric.timeseries[0].data:
                        total_cpu = 0
                        count = 0
                        for point in metric.timeseries[0].data:
                             if point.average is not None:
                                total_cpu += point.average
                                count += 1
                        if count > 0:
                            avg_cpu_percent = total_cpu / count
                
                # Fallback to CpuTime if Percentage CPU not found or no data
                if avg_cpu_percent is None:
                    logging.debug(f"Metric 'Percentage CPU' not found for app {app.name}. Trying 'CpuTime'.")
                    metrics_data_cpu_time = monitor_client.metrics.list(
                        resource_uri=app_resource_uri,
                        timespan=f"PT{lookback_days}D",
                        interval="PT1H",
                        metricnames="CpuTime", 
                        aggregation="Total" # Total CPU seconds per hour
                    )
                    if metrics_data_cpu_time.value:
                        metric = metrics_data_cpu_time.value[0]
                        if metric.timeseries and metric.timeseries[0].data:
                            total_cpu_seconds = 0
                            total_intervals = 0
                            for point in metric.timeseries[0].data:
                                if point.total is not None:
                                    total_cpu_seconds += point.total
                                    total_intervals += 1
                            if total_intervals > 0:
                                # Estimate %: (Total CPU seconds / (Total Intervals * 3600 seconds/hour)) * 100
                                # This needs # cores, which isn't easily available. 
                                # Approximate based on time active? Or just report low absolute CpuTime?
                                # For now, let's flag if average hourly CpuTime is very low (e.g., < 36 seconds = 1% of an hour)
                                avg_cpu_seconds_per_hour = total_cpu_seconds / total_intervals
                                if avg_cpu_seconds_per_hour < 36: # Arbitrary threshold for low absolute usage
                                     avg_cpu_percent = 0.5 # Assign a low % value for flagging
                                else:
                                     avg_cpu_percent = 100 # Assign high value if not very low absolute

                app_details = {
                    "name": app.name,
                    "id": app.id,
                    "resource_group": app.id.split('/')[4],
                    "location": app.location,
                    "avg_cpu_percent": avg_cpu_percent,
                    "plan_name": plan.name,
                    "plan_tier": plan.sku.tier if plan.sku else "Unknown",
                }

                if avg_cpu_percent is not None:
                    if avg_cpu_percent < cpu_threshold_percent:
                        low_usage_apps.append(app_details)
                        console.print(f"  - [yellow]Low CPU Web App:[/yellow] {app_details['name']} (Avg CPU: {avg_cpu_percent:.2f}%, Plan: {app_details['plan_name']})")
                    else:
                        logging.info(f"Web App {app_details['name']} CPU usage OK (Avg: {avg_cpu_percent:.2f}%)")
                else:
                    logging.warning(f"No valid CPU metric data found for Web App {app_details['name']} in the specified timespan.")
                    console.print(f"  - [dim]No CPU data for Web App:[/dim] {app_details['name']}")

            except Exception as metric_error:
                # Handle throttling
                if "429" in str(metric_error) or "throttled" in str(metric_error).lower():
                     logging.warning(f"Metrics query for Web App {app.name} throttled. Skipping.")
                     console.print(f"  - [yellow]Throttled:[/yellow] Skipping metrics for Web App {app.name}.")
                else:
                     logging.warning(f"Could not get metrics for Web App {app.name}. Error: {metric_error}", exc_info=True)
                     console.print(f"  - [yellow]Warning:[/yellow] Could not get metrics for Web App {app.name}.")


        # ... Summary printing ...
        console.print("\n--- Web App Usage Analysis Summary ---")
        if not low_usage_apps:
            console.print(f"  :heavy_check_mark: No running Web Apps (on Basic+ plans) found with avg CPU < {cpu_threshold_percent}%.")
        else:
            console.print(f"  :warning: Found {len(low_usage_apps)} running Web App(s) (on Basic+ plans) with avg CPU < {cpu_threshold_percent}%.")

        return low_usage_apps

    except Exception as e:
        logging.error(f"Error checking for low usage Web Apps: {e}", exc_info=True)
        console.print(f"[bold red]Error checking for low usage Web Apps:[/] {e}")
        return []

# --- Add Orphaned NSG Detection ---
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

# --- Add Orphaned Route Table Detection ---
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