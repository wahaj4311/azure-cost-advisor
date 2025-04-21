import pytest
from unittest.mock import MagicMock, PropertyMock
import logging

# Assuming the analysis functions are in azure_cost_advisor.analysis
# Adjust the import path if your structure is different
import azure_cost_advisor.analysis as analysis # Import the module itself
# Import all functions being tested at the top level
from azure_cost_advisor.analysis import find_unattached_disks, find_stopped_vms, find_unused_public_ips, find_empty_resource_groups 
# We also need Console for type hinting, but can mock its methods
from rich.console import Console

# --- Test Data Structures (Simulating Azure SDK Objects) ---

# Simple mock for SKU object
class MockSku:
    def __init__(self, name="Standard_LRS"):
        self.name = name

# Mock for Disk object - add attributes as needed by the function
class MockDisk:
    def __init__(self, name, id, location, size_gb, sku_name="Standard_LRS", managed_by=None, disk_state="Unattached"):
        self.name = name
        self.id = id
        self.location = location
        self.disk_size_gb = size_gb
        # Simulate the nested SKU object if sku attribute is accessed directly
        # Use PropertyMock if sku itself is accessed, or just set if sku.name is used
        self.sku = MockSku(name=sku_name) 
        # Properties checked by the function
        self.managed_by = managed_by
        self.disk_state = disk_state

# Mock VM Status object
class MockVMStatus:
    def __init__(self, code="PowerState/running"):
        self.code = code

# Mock VM Instance View object
class MockVMInstanceView:
    def __init__(self, statuses=None):
        # Default to a running status if none provided
        self.statuses = statuses if statuses is not None else [MockVMStatus("PowerState/running")]

# Mock Virtual Machine object
class MockVM:
    def __init__(self, name, id, location):
        self.name = name
        self.id = id
        self.location = location
        # Instance view will often be fetched separately, 
        # but can be pre-associated for simpler mocks if needed
        # self.instance_view = MockVMInstanceView() 

# Mock for IP Configuration (placeholder, presence indicates attachment)
class MockIPConfiguration:
    def __init__(self, id="some_nic_ip_config_id"):
        self.id = id

# Mock for NAT Gateway (placeholder, presence indicates attachment)
class MockNatGateway:
     def __init__(self, id="some_nat_gateway_id"):
        self.id = id

# Mock for Public IP Address object
class MockPublicIPAddress:
    def __init__(self, name, id, location, ip_address="1.2.3.4", sku_name="Standard", ip_configuration=None, nat_gateway=None):
        self.name = name
        self.id = id
        self.location = location
        self.ip_address = ip_address
        self.sku = MockSku(name=sku_name) if sku_name else None # Can be None for Basic SKU sometimes
        # Properties checked by the function
        self.ip_configuration = ip_configuration
        self.nat_gateway = nat_gateway

# Mock for Resource Graph Query Response
class MockArgQueryResponse:
    def __init__(self, data=None, total_records=0):
        # data should be a list of dictionaries, matching the 'project' clause of the KQL
        self.data = data if data is not None else []
        self.total_records = total_records

# --- Test Cases ---

def test_find_unattached_disks_positive_case(mocker):
    """Tests finding one unattached disk among others (using ARG mock)."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-123"
    mock_console = MagicMock(spec=Console)

    # Mock ARG response data - list of dicts matching the 'project' clause
    mock_arg_data = [
        {
            "name": "unattached_disk_1",
            "id": "/subscriptions/sub-123/resourceGroups/rg1/providers/Microsoft.Compute/disks/unattached_disk_1",
            "resourceGroup": "rg1",
            "location": "eastus",
            "sizeGb": 128,
            "skuName": "Premium_LRS"
        }
        # Note: Attached/Reserved disks are filtered out by the KQL query itself,
        # so we only need to return the ones that match the query criteria.
    ]
    mock_arg_response = MockArgQueryResponse(data=mock_arg_data, total_records=1)

    # Mock the ResourceGraphClient and its methods
    mock_arg_client_instance = MagicMock()
    mock_arg_client_instance.resources.return_value = mock_arg_response
    
    # Patch the ResourceGraphClient constructor 
    mocker.patch("azure_cost_advisor.analysis.ResourceGraphClient", return_value=mock_arg_client_instance)
    
    # Act
    findings = find_unattached_disks(mock_credential, mock_subscription_id, mock_console)

    # Assert
    # Verify ResourceGraphClient was called correctly
    analysis.ResourceGraphClient.assert_called_once_with(mock_credential)
    # Verify the .resources() method was called (can also check query content if needed)
    mock_arg_client_instance.resources.assert_called_once()
    # Check args of .resources() call - QueryRequest object
    call_args, call_kwargs = mock_arg_client_instance.resources.call_args
    query_request_arg = call_args[0]
    assert query_request_arg.subscriptions == [mock_subscription_id]
    assert "where properties.diskState == 'Unattached'" in query_request_arg.query
    assert "isnull(properties.managedBy)" in query_request_arg.query
    assert "project name, id, resourceGroup, location, sizeGb" in query_request_arg.query

    # Verify the console print methods were called 
    mock_console.print.assert_any_call("\n💾 Checking for unattached managed disks (using ARG)...")
    mock_console.print.assert_any_call("  :warning: Found 1 unattached disk(s).")

    # Verify the findings list
    assert len(findings) == 1
    found_disk = findings[0]
    assert found_disk['name'] == "unattached_disk_1"
    assert found_disk['id'] == mock_arg_data[0]['id']
    assert found_disk['resource_group'] == "rg1"
    assert found_disk['location'] == "eastus"
    assert found_disk['size_gb'] == 128
    assert found_disk['sku'] == "Premium_LRS"

def test_find_unattached_disks_negative_case(mocker):
    """Tests finding no unattached disks when ARG returns no results."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-123"
    mock_console = MagicMock(spec=Console)

    # Mock ARG response with no data
    mock_arg_response = MockArgQueryResponse(data=[], total_records=0)

    mock_arg_client_instance = MagicMock()
    mock_arg_client_instance.resources.return_value = mock_arg_response
    
    mocker.patch("azure_cost_advisor.analysis.ResourceGraphClient", return_value=mock_arg_client_instance)
    
    # Act
    findings = find_unattached_disks(mock_credential, mock_subscription_id, mock_console)

    # Assert
    mock_arg_client_instance.resources.assert_called_once()
    mock_console.print.assert_any_call("\n💾 Checking for unattached managed disks (using ARG)...")
    mock_console.print.assert_any_call("  :heavy_check_mark: No unattached managed disks found.")
    assert len(findings) == 0

def test_find_unattached_disks_api_error(mocker):
    """Tests behavior when the ARG API call raises an exception."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-123"
    mock_console = MagicMock(spec=Console)

    mock_arg_client_instance = MagicMock()
    # Simulate an API error on the .resources() call
    mock_arg_client_instance.resources.side_effect = Exception("Simulated ARG API error")
    
    mocker.patch("azure_cost_advisor.analysis.ResourceGraphClient", return_value=mock_arg_client_instance)
    
    # Get logger and patch error method
    logger_instance = logging.getLogger()
    mocker.patch.object(logger_instance, 'error') 

    # Act
    findings = find_unattached_disks(mock_credential, mock_subscription_id, mock_console)

    # Assert
    mock_arg_client_instance.resources.assert_called_once()
    # Check if the error message was logged and printed
    logger_instance.error.assert_called_once_with(
        "Error checking for unattached disks using ARG: Simulated ARG API error", 
        exc_info=True
    )
    mock_console.print.assert_any_call("  [bold red]Error checking for unattached disks (ARG):[/] Simulated ARG API error")
    # Function should return an empty list on error
    assert len(findings) == 0

# --- Tests for find_stopped_vms ---

def test_find_stopped_vms_positive_case(mocker):
    """Tests finding a VM that is stopped but not deallocated."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-456"
    mock_console = MagicMock(spec=Console)

    vm1_stopped = MockVM(
        name="stopped-vm-1", 
        id="/subscriptions/sub-456/resourceGroups/rg-A/providers/Microsoft.Compute/virtualMachines/stopped-vm-1", 
        location="eastus"
    )
    vm2_running = MockVM(
        name="running-vm-1", 
        id="/subscriptions/sub-456/resourceGroups/rg-A/providers/Microsoft.Compute/virtualMachines/running-vm-1", 
        location="eastus"
    )
    vm3_deallocated = MockVM(
        name="dealloc-vm-1", 
        id="/subscriptions/sub-456/resourceGroups/rg-B/providers/Microsoft.Compute/virtualMachines/dealloc-vm-1", 
        location="westus"
    )

    # Mock the list_all call
    mock_compute_client_instance = MagicMock()
    mock_compute_client_instance.virtual_machines.list_all.return_value = [
        vm1_stopped, vm2_running, vm3_deallocated
    ]

    # Mock the instance_view call - return different views based on vm name
    def mock_instance_view(resource_group_name, vm_name):
        if vm_name == "stopped-vm-1":
            return MockVMInstanceView(statuses=[MockVMStatus("provisioningState/succeeded"), MockVMStatus("PowerState/stopped")])
        elif vm_name == "running-vm-1":
            return MockVMInstanceView(statuses=[MockVMStatus("PowerState/running")])
        elif vm_name == "dealloc-vm-1":
            return MockVMInstanceView(statuses=[MockVMStatus("PowerState/deallocated")])
        else:
            raise Exception(f"Unexpected vm_name in mock_instance_view: {vm_name}")

    mock_compute_client_instance.virtual_machines.instance_view.side_effect = mock_instance_view
    
    # Patch the client constructor
    mocker.patch("azure_cost_advisor.analysis.ComputeManagementClient", return_value=mock_compute_client_instance)

    # Get the logger instance that the function will use
    # Important: Mock the specific logger methods *before* the function call
    logger_instance = logging.getLogger() # Get the root logger or specific if named
    mocker.patch.object(logger_instance, 'warning') # Patch the 'warning' method

    # Act
    findings = find_stopped_vms(mock_credential, mock_subscription_id, mock_console)

    # Assert
    analysis.ComputeManagementClient.assert_called_once_with(mock_credential, mock_subscription_id)
    mock_compute_client_instance.virtual_machines.list_all.assert_called_once()
    # Check instance_view was called for each VM
    assert mock_compute_client_instance.virtual_machines.instance_view.call_count == 3
    mock_compute_client_instance.virtual_machines.instance_view.assert_any_call(resource_group_name="rg-A", vm_name="stopped-vm-1")
    mock_compute_client_instance.virtual_machines.instance_view.assert_any_call(resource_group_name="rg-A", vm_name="running-vm-1")
    mock_compute_client_instance.virtual_machines.instance_view.assert_any_call(resource_group_name="rg-B", vm_name="dealloc-vm-1")

    # Verify console output
    mock_console.print.assert_any_call("\n🛑 Checking for stopped (not deallocated) VMs...")
    mock_console.print.assert_any_call("  :warning: Found 1 stopped VM(s) that are incurring compute costs.")
    
    # Verify findings
    assert len(findings) == 1
    found_vm = findings[0]
    assert found_vm['name'] == "stopped-vm-1"
    assert found_vm['id'] == vm1_stopped.id
    assert found_vm['resource_group'] == "rg-A"
    assert found_vm['location'] == "eastus"

def test_find_stopped_vms_negative_case(mocker):
    """Tests finding no stopped VMs when all are running or deallocated."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-456"
    mock_console = MagicMock(spec=Console)

    vm1_running = MockVM("running-vm-2", "/subs/sub-456/rgs/rg-C/prov/Microsoft.Compute/vms/running-vm-2", "westus")
    vm2_deallocated = MockVM("dealloc-vm-2", "/subs/sub-456/rgs/rg-C/prov/Microsoft.Compute/vms/dealloc-vm-2", "westus")

    mock_compute_client_instance = MagicMock()
    mock_compute_client_instance.virtual_machines.list_all.return_value = [vm1_running, vm2_deallocated]

    def mock_instance_view(resource_group_name, vm_name):
        if vm_name == "running-vm-2":
            return MockVMInstanceView(statuses=[MockVMStatus("PowerState/running")])
        elif vm_name == "dealloc-vm-2":
             return MockVMInstanceView(statuses=[MockVMStatus("PowerState/deallocated")])
        else:
             raise Exception(f"Unexpected vm_name: {vm_name}")
             
    mock_compute_client_instance.virtual_machines.instance_view.side_effect = mock_instance_view
    mocker.patch("azure_cost_advisor.analysis.ComputeManagementClient", return_value=mock_compute_client_instance)

    # Act
    findings = find_stopped_vms(mock_credential, mock_subscription_id, mock_console)

    # Assert
    mock_compute_client_instance.virtual_machines.list_all.assert_called_once()
    assert mock_compute_client_instance.virtual_machines.instance_view.call_count == 2
    mock_console.print.assert_any_call("\n🛑 Checking for stopped (not deallocated) VMs...")
    mock_console.print.assert_any_call("  :heavy_check_mark: No stopped (but not deallocated) VMs found.")
    assert len(findings) == 0

def test_find_stopped_vms_instance_view_error(mocker):
    """Tests that the function continues if instance_view fails for one VM."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-456"
    mock_console = MagicMock(spec=Console)

    vm1_stopped = MockVM("stopped-vm-ok", "/subs/sub-456/rgs/rg-D/prov/Microsoft.Compute/vms/stopped-vm-ok", "eastus")
    vm2_error = MockVM("error-vm", "/subs/sub-456/rgs/rg-D/prov/Microsoft.Compute/vms/error-vm", "eastus")

    mock_compute_client_instance = MagicMock()
    mock_compute_client_instance.virtual_machines.list_all.return_value = [vm1_stopped, vm2_error]

    def mock_instance_view(resource_group_name, vm_name):
        if vm_name == "stopped-vm-ok":
            return MockVMInstanceView(statuses=[MockVMStatus("PowerState/stopped")])
        elif vm_name == "error-vm":
            raise Exception("Simulated instance view error")
        else:
            raise Exception(f"Unexpected vm_name: {vm_name}")
             
    mock_compute_client_instance.virtual_machines.instance_view.side_effect = mock_instance_view

    # Patch the client constructor
    mocker.patch("azure_cost_advisor.analysis.ComputeManagementClient", return_value=mock_compute_client_instance)

    # Get the logger instance and patch its 'warning' method
    logger_instance = logging.getLogger()
    mocker.patch.object(logger_instance, 'warning')

    # Act
    findings = find_stopped_vms(mock_credential, mock_subscription_id, mock_console)

    # Assert
    assert mock_compute_client_instance.virtual_machines.instance_view.call_count == 2
    # Check warning was logged and printed
    logger_instance.warning.assert_called_once_with(
        "Could not get instance view for VM error-vm in RG rg-D. Error: Simulated instance view error", 
        exc_info=True
    )
    mock_console.print.assert_any_call("  [yellow]Warning:[/][dim] Could not get instance view for VM error-vm. Skipping status check.[/]")
    # Check that the valid stopped VM was still found
    assert len(findings) == 1
    assert findings[0]['name'] == "stopped-vm-ok"
    mock_console.print.assert_any_call("  :warning: Found 1 stopped VM(s) that are incurring compute costs.")

def test_find_stopped_vms_list_all_error(mocker):
    """Tests behavior when the list_all API call raises an exception."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-456"
    mock_console = MagicMock(spec=Console)

    mock_compute_client_instance = MagicMock()
    mock_compute_client_instance.virtual_machines.list_all.side_effect = Exception("Simulated list_all error")
    
    # Patch the client constructor
    mocker.patch("azure_cost_advisor.analysis.ComputeManagementClient", return_value=mock_compute_client_instance)

    # Get the logger instance and patch its 'error' method
    logger_instance = logging.getLogger()
    mocker.patch.object(logger_instance, 'error')

    # Act
    findings = find_stopped_vms(mock_credential, mock_subscription_id, mock_console)

    # Assert
    mock_compute_client_instance.virtual_machines.list_all.assert_called_once()
    logger_instance.error.assert_called_once_with(
        "Error checking for stopped VMs: Simulated list_all error", 
        exc_info=True
    )
    mock_console.print.assert_any_call("[bold red]Error checking for stopped VMs:[/] Simulated list_all error")
    assert len(findings) == 0

# --- Tests for find_unused_public_ips ---

def test_find_unused_public_ips_positive_case(mocker):
    """Tests finding an unused public IP."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-789"
    mock_console = MagicMock(spec=Console)

    ip1_unused = MockPublicIPAddress(
        name="unused-ip-1", 
        id="/subs/sub-789/rgs/rg-X/prov/Microsoft.Network/publicIPAddresses/unused-ip-1", 
        location="westeurope", 
        ip_address="10.0.0.1",
        sku_name="Standard",
        ip_configuration=None, # Unused condition 1
        nat_gateway=None # Unused condition 2
    )
    ip2_attached_nic = MockPublicIPAddress(
        name="attached-ip-nic", 
        id="/subs/sub-789/rgs/rg-X/prov/Microsoft.Network/publicIPAddresses/attached-ip-nic", 
        location="westeurope", 
        ip_address="10.0.0.2",
        sku_name="Standard",
        ip_configuration=MockIPConfiguration(), # Attached to NIC
        nat_gateway=None
    )
    ip3_attached_nat = MockPublicIPAddress(
        name="attached-ip-nat", 
        id="/subs/sub-789/rgs/rg-Y/prov/Microsoft.Network/publicIPAddresses/attached-ip-nat", 
        location="eastus", 
        ip_address="10.0.0.3",
        sku_name="Standard",
        ip_configuration=None, 
        nat_gateway=MockNatGateway() # Attached to NAT GW
    )
    ip4_unused_basic = MockPublicIPAddress(
        name="unused-ip-basic", 
        id="/subs/sub-789/rgs/rg-X/prov/Microsoft.Network/publicIPAddresses/unused-ip-basic", 
        location="westeurope", 
        ip_address="10.0.0.4",
        sku_name=None, # Simulates Basic SKU where sku object might be None
        ip_configuration=None, # Unused
        nat_gateway=None # Unused
    )


    # Mock the NetworkManagementClient and its methods
    mock_network_client_instance = MagicMock()
    mock_network_client_instance.public_ip_addresses.list_all.return_value = [
        ip1_unused, ip2_attached_nic, ip3_attached_nat, ip4_unused_basic
    ]
    
    # Patch the NetworkManagementClient constructor
    mocker.patch("azure_cost_advisor.analysis.NetworkManagementClient", return_value=mock_network_client_instance)
    
    # Act
    findings = find_unused_public_ips(mock_credential, mock_subscription_id, mock_console)

    # Assert
    analysis.NetworkManagementClient.assert_called_once_with(mock_credential, mock_subscription_id)
    mock_network_client_instance.public_ip_addresses.list_all.assert_called_once()
    
    # Verify console output
    mock_console.print.assert_any_call("\n🌐 Checking for unused Public IP Addresses...")
    mock_console.print.assert_any_call("  :warning: Found 2 unused Public IP(s).")

    # Verify findings
    assert len(findings) == 2
    # Check Standard IP
    found_ip1 = next(f for f in findings if f['name'] == "unused-ip-1")
    assert found_ip1['id'] == ip1_unused.id
    assert found_ip1['resource_group'] == "rg-X"
    assert found_ip1['location'] == "westeurope"
    assert found_ip1['ip_address'] == "10.0.0.1"
    assert found_ip1['sku'] == "Standard"
    # Check Basic IP (where sku was None)
    found_ip4 = next(f for f in findings if f['name'] == "unused-ip-basic")
    assert found_ip4['id'] == ip4_unused_basic.id
    assert found_ip4['resource_group'] == "rg-X"
    assert found_ip4['location'] == "westeurope"
    assert found_ip4['ip_address'] == "10.0.0.4"
    assert found_ip4['sku'] == "Basic" # Function defaults to Basic if sku is None

def test_find_unused_public_ips_negative_case(mocker):
    """Tests finding no unused public IPs when all are attached."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-789"
    mock_console = MagicMock(spec=Console)

    ip1_attached_nic = MockPublicIPAddress("attached-ip-1", "/subs/id/rg1/p/ip1", "eastus", ip_configuration=MockIPConfiguration())
    ip2_attached_nat = MockPublicIPAddress("attached-ip-2", "/subs/id/rg2/p/ip2", "westus", nat_gateway=MockNatGateway())
    
    mock_network_client_instance = MagicMock()
    mock_network_client_instance.public_ip_addresses.list_all.return_value = [ip1_attached_nic, ip2_attached_nat]
    
    mocker.patch("azure_cost_advisor.analysis.NetworkManagementClient", return_value=mock_network_client_instance)
    
    # Act
    findings = find_unused_public_ips(mock_credential, mock_subscription_id, mock_console)

    # Assert
    mock_network_client_instance.public_ip_addresses.list_all.assert_called_once()
    mock_console.print.assert_any_call("\n🌐 Checking for unused Public IP Addresses...")
    mock_console.print.assert_any_call("  :heavy_check_mark: No unused Public IP Addresses found.")
    assert len(findings) == 0

def test_find_unused_public_ips_api_error(mocker):
    """Tests behavior when the list_all API call raises an exception."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-789"
    mock_console = MagicMock(spec=Console)

    mock_network_client_instance = MagicMock()
    mock_network_client_instance.public_ip_addresses.list_all.side_effect = Exception("Simulated network API error")
    
    mocker.patch("azure_cost_advisor.analysis.NetworkManagementClient", return_value=mock_network_client_instance)
    
    # Act
    findings = find_unused_public_ips(mock_credential, mock_subscription_id, mock_console)

    # Assert
    mock_network_client_instance.public_ip_addresses.list_all.assert_called_once()
    mock_console.print.assert_any_call("[bold red]Error checking for unused Public IPs:[/] Simulated network API error")
    assert len(findings) == 0

# --- Tests for find_empty_resource_groups (Using ARG) ---

def test_find_empty_resource_groups_positive_case(mocker):
    """Tests finding an empty resource group."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-000"
    mock_console = MagicMock(spec=Console)

    # Mock ARG response data matching the 'project' clause
    mock_arg_data = [
        {
            "name": "empty-rg-1",
            "id": "/subscriptions/sub-000/resourceGroups/empty-rg-1",
            "location": "uksouth"
        }
        # Note: Non-empty RGs are filtered by the KQL query.
    ]
    mock_arg_response = MockArgQueryResponse(data=mock_arg_data, total_records=1)

    mock_arg_client_instance = MagicMock()
    mock_arg_client_instance.resources.return_value = mock_arg_response
    
    mocker.patch("azure_cost_advisor.analysis.ResourceGraphClient", return_value=mock_arg_client_instance)
    
    # Act
    findings = find_empty_resource_groups(mock_credential, mock_subscription_id, mock_console)

    # Assert
    analysis.ResourceGraphClient.assert_called_once_with(mock_credential)
    mock_arg_client_instance.resources.assert_called_once()
    # Verify query content
    call_args, call_kwargs = mock_arg_client_instance.resources.call_args
    query_request_arg = call_args[0]
    assert query_request_arg.subscriptions == [mock_subscription_id]
    assert "ResourceContainers" in query_request_arg.query
    assert "type == 'microsoft.resources/subscriptions/resourcegroups'" in query_request_arg.query
    assert "join kind=leftouter (Resources | summarize count() by resourceGroup)" in query_request_arg.query
    assert "where isnull(count_) or count_ == 0" in query_request_arg.query
    assert query_request_arg.query.strip().endswith("project name, id, location")

    # Verify console output
    mock_console.print.assert_any_call("\n🗑 Checking for empty Resource Groups (using ARG)...")
    mock_console.print.assert_any_call("  :warning: Found 1 empty Resource Group(s).")

    # Verify findings
    assert len(findings) == 1
    found_rg = findings[0]
    assert found_rg['name'] == "empty-rg-1"
    assert found_rg['id'] == mock_arg_data[0]['id']
    assert found_rg['location'] == "uksouth"

def test_find_empty_resource_groups_negative_case(mocker):
    """Tests finding no empty RGs when ARG returns no results."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-000"
    mock_console = MagicMock(spec=Console)

    mock_arg_response = MockArgQueryResponse(data=[], total_records=0)

    mock_arg_client_instance = MagicMock()
    mock_arg_client_instance.resources.return_value = mock_arg_response
    
    mocker.patch("azure_cost_advisor.analysis.ResourceGraphClient", return_value=mock_arg_client_instance)
    
    # Act
    findings = find_empty_resource_groups(mock_credential, mock_subscription_id, mock_console)

    # Assert
    mock_arg_client_instance.resources.assert_called_once()
    mock_console.print.assert_any_call("\n🗑 Checking for empty Resource Groups (using ARG)...")
    mock_console.print.assert_any_call("  :heavy_check_mark: No empty Resource Groups found.")
    assert len(findings) == 0

def test_find_empty_resource_groups_api_error(mocker):
    """Tests behavior when the ARG API call raises an exception."""
    # Arrange
    mock_credential = MagicMock()
    mock_subscription_id = "sub-000"
    mock_console = MagicMock(spec=Console)

    mock_arg_client_instance = MagicMock()
    mock_arg_client_instance.resources.side_effect = Exception("Simulated ARG error for RGs")
    
    mocker.patch("azure_cost_advisor.analysis.ResourceGraphClient", return_value=mock_arg_client_instance)
    
    logger_instance = logging.getLogger()
    mocker.patch.object(logger_instance, 'error') 

    # Act
    findings = find_empty_resource_groups(mock_credential, mock_subscription_id, mock_console)

    # Assert
    mock_arg_client_instance.resources.assert_called_once()
    logger_instance.error.assert_called_once_with(
        "Error checking for empty Resource Groups using ARG: Simulated ARG error for RGs", 
        exc_info=True
    )
    mock_console.print.assert_any_call("[bold red]Error checking for empty Resource Groups (ARG):[/] Simulated ARG error for RGs")
    assert len(findings) == 0

# TODO: Add tests for other analysis functions (find_stopped_vms, find_unused_public_ips, etc.) 