import logging
import time

from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.web import WebSiteManagementClient
from azure.core.exceptions import ResourceNotFoundError

# Rich for console output
from rich.console import Console

# Initialize console for potential standalone use or if passed
_console = Console()

# --- Action Functions ---

def delete_resource(credential, subscription_id, resource_id, resource_type, resource_name, clients, console: Console = _console, wait_for_completion=False, force_cleanup=False):
    """Generic function to delete a resource using appropriate client, with optional force flag."""
    logger = logging.getLogger()
    log_prefix = f"ACTION - DELETE - {resource_type} - {resource_name} ({resource_id})"
    confirm = False

    logger.debug(f"{log_prefix}: Checking deletion confirmation status (force_cleanup={force_cleanup}).")

    if force_cleanup:
        console.print(f":warning: [bold red]--force-cleanup enabled.[/] Preparing to delete {resource_type} '{resource_name}' non-interactively...")
        confirm = True
    else:
        choice = console.input(f":question: Delete {resource_type} '{resource_name}' ([dim]{resource_id}[/dim])? [[bold bright_red]y[/]/N]: ").lower()
        confirm = choice == 'y'

    if confirm:
        logger.info(f"{log_prefix}: User confirmed deletion (or --force-cleanup used). Initiating.")
        console.print(f"  Attempting deletion of {resource_type} '{resource_name}'...")
        poller = None
        start_time = time.time()
        try:
            # Extract RG name safely
            try:
                rg_name = resource_id.split('/')[4]
            except IndexError:
                if resource_type == "Empty Resource Group":
                    rg_name = None # Not needed for RG deletion
                    logger.debug(f"{log_prefix}: Resource is an RG, RG name extraction not needed.")
                else:
                    logger.error(f"{log_prefix}: Could not parse resource group from ID. Cannot delete.")
                    console.print(f"  [bold red]Error:[/bold red] Could not parse resource group from ID for {resource_name}. Cannot delete.")
                    return False

            logger.debug(f"{log_prefix}: Using Resource Group '{rg_name}' (if applicable).")

            # Determine the correct client and delete method based on resource type string
            if resource_type == "Unattached Disk":
                logger.debug(f"{log_prefix}: Calling compute_client.disks.begin_delete...")
                poller = clients['compute'].disks.begin_delete(rg_name, resource_name)
            elif resource_type == "Unused Public IP":
                logger.debug(f"{log_prefix}: Calling network_client.public_ip_addresses.begin_delete...")
                poller = clients['network'].public_ip_addresses.begin_delete(rg_name, resource_name)
            elif resource_type == "Empty Resource Group":
                 logger.debug(f"{log_prefix}: Calling resource_client.resource_groups.begin_delete...")
                 poller = clients['resource'].resource_groups.begin_delete(resource_name) # RG name is resource_name
            elif resource_type == "Empty App Service Plan":
                 logger.debug(f"{log_prefix}: Calling web_client.app_service_plans.begin_delete...")
                 poller = clients['web'].app_service_plans.begin_delete(rg_name, resource_name)
            elif resource_type == "Old Disk Snapshot":
                 logger.debug(f"{log_prefix}: Calling compute_client.snapshots.begin_delete...")
                 poller = clients['compute'].snapshots.begin_delete(rg_name, resource_name)
            elif resource_type == "Orphaned Network Security Group": # Added Type
                 logger.debug(f"{log_prefix}: Calling network_client.network_security_groups.begin_delete...")
                 poller = clients['network'].network_security_groups.begin_delete(rg_name, resource_name)
            elif resource_type == "Orphaned Route Table": # Added Type
                 logger.debug(f"{log_prefix}: Calling network_client.route_tables.begin_delete...")
                 poller = clients['network'].route_tables.begin_delete(rg_name, resource_name)
            # Add other resource types here as needed
            else:
                logger.warning(f"{log_prefix}: Deletion logic for resource type '{resource_type}' not implemented.")
                console.print(f"  [yellow]Warning:[/yellow] Deletion for resource type '{resource_type}' not implemented.")
                return False

            logger.info(f"{log_prefix}: Deletion initiated. Initial poller state: {poller.status()}")

            if wait_for_completion and poller:
                logger.info(f"{log_prefix}: Waiting for deletion to complete (wait_for_completion=True)...")
                console.print(f"  Waiting for deletion of {resource_name} to complete...")
                start_wait = time.time()
                with console.status("[cyan]Waiting for operation...[/]"):
                     poller.result() # This blocks until completion
                end_wait = time.time()
                wait_duration = end_wait - start_wait
                final_status = poller.status()
                logger.info(f"{log_prefix}: Deletion completed. Final Poller State: {final_status} (Wait time: {wait_duration:.2f}s)")
                console.print(f"  ✅ Deletion of {resource_name} completed ({final_status}).")
            elif poller:
                 # Log status even if not waiting
                 current_status = poller.status()
                 logger.info(f"{log_prefix}: Deletion initiated, not waiting for completion (poller status: {current_status}).")
                 console.print(f"  ✅ Deletion of {resource_name} initiated (status: {current_status}).")

            duration = time.time() - start_time
            logger.info(f"{log_prefix}: Deletion request processed successfully. Total time: {duration:.2f}s")
            return True # Indicate success
        except ResourceNotFoundError:
             duration = time.time() - start_time
             log_msg = f"{log_prefix}: Resource not found (perhaps already deleted?). Operation took {duration:.2f}s."
             logger.warning(log_msg)
             console.print(f"  [yellow]Warning:[/yellow] {resource_type} '{resource_name}' not found. Skipping.")
             return False # Indicate failure/skip
        except Exception as e:
            duration = time.time() - start_time
            final_status = f"failed ({e})" if not poller else f"{poller.status()} ({e})"
            log_msg = f"{log_prefix}: Error during deletion process. Final status: {final_status}. Operation took {duration:.2f}s."
            logger.error(log_msg, exc_info=True)
            console.print(f"  [bold red]Error deleting {resource_name}:[/bold red] {e}")
            return False # Indicate failure
    else:
        logger.info(f"{log_prefix}: User skipped deletion.")
        console.print(f"  Skipping deletion of {resource_type} '{resource_name}'.")
        return False # Indicate skip

def deallocate_vm(credential, subscription_id, rg_name, vm_name, compute_client, console: Console = _console, wait_for_completion=False, force_cleanup=False):
    """Deallocates a VM after confirmation, with optional force flag."""
    logger = logging.getLogger()
    log_prefix = f"ACTION - DEALLOCATE - VM - {vm_name} (RG: {rg_name})"
    confirm = False

    logger.debug(f"{log_prefix}: Checking deallocation confirmation status (force_cleanup={force_cleanup}).")

    if force_cleanup:
        console.print(f":warning: [bold red]--force-cleanup enabled.[/] Preparing to deallocate VM '{vm_name}' non-interactively...")
        confirm = True
    else:
        choice = console.input(f":question: Deallocate VM '{vm_name}' in RG '{rg_name}'? (Keeps disks) [[bold bright_red]y[/]/N]: ").lower()
        confirm = choice == 'y'

    if confirm:
        logger.info(f"{log_prefix}: User confirmed deallocation (or --force-cleanup used). Initiating.")
        console.print(f"  Attempting deallocation of VM '{vm_name}'...")
        poller = None
        start_time = time.time()
        try:
            logger.debug(f"{log_prefix}: Calling compute_client.virtual_machines.begin_deallocate...")
            poller = compute_client.virtual_machines.begin_deallocate(rg_name, vm_name)
            logger.info(f"{log_prefix}: Deallocation initiated. Initial poller state: {poller.status()}")

            if wait_for_completion and poller:
                logger.info(f"{log_prefix}: Waiting for deallocation to complete (wait_for_completion=True)...")
                console.print(f"  Waiting for deallocation of {vm_name} to complete...")
                start_wait = time.time()
                with console.status("[cyan]Waiting for operation...[/]"):
                    poller.result() # Blocks until complete
                end_wait = time.time()
                wait_duration = end_wait - start_wait
                final_status = poller.status()
                logger.info(f"{log_prefix}: Deallocation completed. Final Poller State: {final_status} (Wait time: {wait_duration:.2f}s)")
                console.print(f"  ✅ Deallocation of {vm_name} completed ({final_status}).")
            elif poller:
                 # Log status even if not waiting
                 current_status = poller.status()
                 logger.info(f"{log_prefix}: Deallocation initiated, not waiting for completion (poller status: {current_status}).")
                 console.print(f"  ✅ Deallocation of {vm_name} initiated (status: {current_status}).")

            duration = time.time() - start_time
            logger.info(f"{log_prefix}: Deallocation request processed successfully. Total time: {duration:.2f}s")
            return True # Indicate success
        except ResourceNotFoundError:
             duration = time.time() - start_time
             log_msg = f"{log_prefix}: VM not found (perhaps already deleted/deallocated?). Operation took {duration:.2f}s."
             logger.warning(log_msg)
             console.print(f"  [yellow]Warning:[/yellow] VM '{vm_name}' not found. Skipping.")
             return False # Indicate failure/skip
        except Exception as e:
            duration = time.time() - start_time
            final_status = f"failed ({e})" if not poller else f"{poller.status()} ({e})"
            log_msg = f"{log_prefix}: Error during deallocation process. Final status: {final_status}. Operation took {duration:.2f}s."
            logger.error(log_msg, exc_info=True)
            console.print(f"  [bold red]Error deallocating {vm_name}:[/bold red] {e}")
            return False # Indicate failure
    else:
        logger.info(f"{log_prefix}: User skipped deallocation.")
        console.print(f"  Skipping deallocation of VM '{vm_name}'.")
        return False # Indicate skip

# --- Main Cleanup Orchestration ---
def perform_interactive_cleanup(credential, subscription_id, findings_dfs: dict, console: Console = _console, wait_for_completion=False, force_cleanup=False):
    """Iterates through findings DataFrames and prompts for cleanup actions."""
    logger = logging.getLogger() # Get logger instance
    console.print("\n[bold cyan]--- Interactive Cleanup ---[/]")
    if force_cleanup:
        console.print(":warning: [bold red]--force-cleanup flag is active! Actions will proceed without confirmation.[/] :warning:")
    else:
        console.print(":warning: [yellow]You will be prompted to confirm each action. Deletions may take time.[/] :warning:")
    if wait_for_completion:
        console.print(":hourglass: [cyan]Waiting for each cleanup operation to complete...[/]")

    # Initialize clients needed for actions within this function scope
    try:
        logger.debug("Initializing Azure management clients for cleanup.")
        clients = {
            'resource': ResourceManagementClient(credential, subscription_id),
            'compute': ComputeManagementClient(credential, subscription_id),
            'network': NetworkManagementClient(credential, subscription_id),
            'web': WebSiteManagementClient(credential, subscription_id)
        }
        logger.debug("Azure management clients initialized successfully.")
    except Exception as client_error:
        logger.error(f"Failed to initialize Azure clients for cleanup: {client_error}", exc_info=True)
        console.print(f"[bold red]Error initializing Azure clients:[/bold red] {client_error}. Cleanup aborted.")
        return

    # Define mapping from DataFrame key to resource type string used in delete_resource
    # This mapping ensures we call delete_resource with the correct type
    cleanup_map = {
        'unattached_disks': 'Unattached Disk',
        'unused_public_ips': 'Unused Public IP',
        'empty_rgs': 'Empty Resource Group',
        'empty_asps': 'Empty App Service Plan',
        'old_snapshots': 'Old Disk Snapshot',
        'orphaned_nsgs': 'Orphaned Network Security Group',
        'orphaned_rts': 'Orphaned Route Table'
        # Add other deletable resource types here
    }

    total_actions_attempted = 0
    total_actions_succeeded = 0
    total_actions_skipped = 0
    total_actions_failed = 0

    # Iterate through findings that have a corresponding action
    for finding_key, resource_type_str in cleanup_map.items():
        df = findings_dfs.get(finding_key)
        if df is not None and not df.empty:
            console.print(f"\n🧹 [bold]Checking {resource_type_str}s for cleanup...[/]")
            # Ensure necessary columns exist
            if 'ID' not in df.columns or 'Name' not in df.columns:
                logger.warning(f"Skipping cleanup for {finding_key}: DataFrame missing required 'ID' or 'Name' column.")
                console.print(f"  [yellow]Warning:[/yellow] Cannot perform cleanup for {finding_key}, missing ID or Name column.")
                continue

            logger.info(f"Processing {len(df)} potential {resource_type_str}(s) for deletion.")
            for index, row in df.iterrows():
                try:
                     resource_id = row['ID']
                     resource_name = row['Name']
                     if not resource_id or not resource_name:
                         logger.warning(f"Skipping row {index} for {finding_key}: Missing ID ('{resource_id}') or Name ('{resource_name}').")
                         console.print(f"  [yellow]Warning:[/yellow] Skipping item at index {index} due to missing ID/Name.")
                         continue

                     logger.debug(f"Attempting deletion for {resource_type_str} '{resource_name}' (ID: {resource_id}).")
                     total_actions_attempted += 1
                     # Call the generic delete function
                     success = delete_resource(
                         credential=credential,
                         subscription_id=subscription_id,
                         resource_id=resource_id,
                         resource_type=resource_type_str,
                         resource_name=resource_name,
                         clients=clients,
                         console=console,
                         wait_for_completion=wait_for_completion,
                         force_cleanup=force_cleanup
                     )
                     # Tally results based on return value (True=Success/Initiated, False=Skipped/Failed)
                     # Note: This doesn't distinguish between user skip and actual failure here,
                     # but delete_resource logs the specifics.
                     if success:
                         total_actions_succeeded += 1
                     # else: # A False return could be skip or failure - logged internally
                     #     total_actions_failed += 1 # Or track skips separately if needed

                except KeyError as e:
                    logger.error(f"Missing expected column {e} in DataFrame for {finding_key} at row {index} during cleanup.", exc_info=True)
                    console.print(f"  [red]Error:[/red] Internal error processing cleanup for {finding_key} - missing column {e}.")
                    total_actions_failed += 1
                except Exception as e:
                    # Log the specific resource name/ID if available
                    r_name = row.get('Name', 'Unknown')
                    r_id = row.get('ID', 'Unknown')
                    logger.error(f"Unexpected error processing row {index} (Name: {r_name}, ID: {r_id}) for {finding_key} during cleanup: {e}", exc_info=True)
                    console.print(f"  [red]Error:[/red] Unexpected error during cleanup for {r_name}: {e}.")
                    total_actions_failed += 1

    # Specific action for Stopped VMs (Deallocate)
    stopped_vms_df = findings_dfs.get('stopped_vms')
    if stopped_vms_df is not None and not stopped_vms_df.empty:
        console.print("\n🧹 [bold]Checking Stopped VMs for deallocation...[/]")
        if 'Resource Group' not in stopped_vms_df.columns or 'Name' not in stopped_vms_df.columns:
             logger.warning("Skipping deallocation: DataFrame missing required 'Resource Group' or 'Name' column.")
             console.print("  [yellow]Warning:[/yellow] Cannot perform deallocation, missing Resource Group or Name column.")
        else:
            logger.info(f"Processing {len(stopped_vms_df)} potential Stopped VM(s) for deallocation.")
            for index, row in stopped_vms_df.iterrows():
                try:
                    rg_name = row['Resource Group']
                    vm_name = row['Name']
                    if not rg_name or not vm_name:
                         logger.warning(f"Skipping row {index} for stopped_vms: Missing Resource Group ('{rg_name}') or Name ('{vm_name}').")
                         console.print(f"  [yellow]Warning:[/yellow] Skipping item at index {index} due to missing RG/Name.")
                         continue

                    logger.debug(f"Attempting deallocation for VM '{vm_name}' in RG '{rg_name}'.")
                    total_actions_attempted += 1
                    success = deallocate_vm(
                        credential=credential,
                        subscription_id=subscription_id,
                        rg_name=rg_name,
                        vm_name=vm_name,
                        compute_client=clients['compute'],
                        console=console,
                        wait_for_completion=wait_for_completion,
                        force_cleanup=force_cleanup
                    )
                    if success:
                        total_actions_succeeded += 1
                    # else: # Skip or failure
                    #     total_actions_failed += 1

                except KeyError as e:
                    logger.error(f"Missing expected column {e} in DataFrame for stopped_vms at row {index} during deallocation.", exc_info=True)
                    console.print(f"  [red]Error:[/red] Internal error processing deallocation - missing column {e}.")
                    total_actions_failed += 1
                except Exception as e:
                     vm_n = row.get('Name', 'Unknown')
                     rg_n = row.get('Resource Group', 'Unknown')
                     logger.error(f"Unexpected error processing row {index} (Name: {vm_n}, RG: {rg_n}) for stopped_vms during deallocation: {e}", exc_info=True)
                     console.print(f"  [red]Error:[/red] Unexpected error during deallocation for {vm_n}: {e}.")
                     total_actions_failed += 1

    # Log summary of cleanup actions
    logger.info(f"Cleanup process finished. Attempted: {total_actions_attempted}, Succeeded/Initiated: {total_actions_succeeded}, Failed: {total_actions_failed}.") # Note: Skipped are implicitly not in Succeeded/Failed
    console.print(f"\n[bold cyan]--- Cleanup Summary ---[/]")
    console.print(f"  Actions Attempted: {total_actions_attempted}")
    console.print(f"  Actions Succeeded/Initiated: {total_actions_succeeded}")
    # console.print(f"  Actions Skipped by User: {total_actions_skipped}") # Requires more detailed tracking if needed
    console.print(f"  Actions Failed: {total_actions_failed}") 