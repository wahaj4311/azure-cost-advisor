import logging
import requests
import json
import re # Import regex for flexible matching
from typing import List, Dict, Any, Optional, Tuple # Add typing
from rich.console import Console # Keep for potential future use or passthrough
from azure.mgmt.costmanagement import CostManagementClient # Added import

# Import constants from config module
# Assuming config.py is in the same directory or PYTHONPATH is set correctly
from .config import (
    RETAIL_PRICES_API_ENDPOINT, 
    HOURS_PER_MONTH,
    # DISK_SIZE_TO_TIER <<< Removed from import
)

# Moved from config.py: Mapping Disk Size (GB) to Performance Tiers (Premium, StandardSSD)
# Sizes based on Azure documentation: https://docs.microsoft.com/en-us/azure/virtual-machines/disks-types
DISK_SIZE_TO_TIER = {
    # Size: (Premium Tier, StandardSSD Tier)
    4:    ("P1", "E1"),
    8:    ("P2", "E2"),
    16:   ("P3", "E3"),
    32:   ("P4", "E4"),
    64:   ("P6", "E6"),
    128:  ("P10", "E10"),
    256:  ("P15", "E15"),
    512:  ("P20", "E20"),
    1024: ("P30", "E30"),
    2048: ("P40", "E40"),
    4096: ("P50", "E50"),
    8192: ("P60", "E60"),
    16384:("P70", "E70"),
    32767:("P80", "E80"),
}

# Initialize console for standalone use (e.g., testing) or if passed
_console = Console() 

# --- Pricing Cache --- 
# Cache for API results { filter_string: list_of_price_items }
price_cache: Dict[str, Optional[List[Dict[str, Any]]]] = {}

# --- Pricing Helper Functions --- 
def fetch_retail_prices(filter_string: str) -> Optional[List[Dict[str, Any]]]:
    """
    Fetches Azure retail prices using OData filter, with caching.
    Returns the raw list of items from the API response or None on error.
    """
    global price_cache
    logger = logging.getLogger()

    if filter_string in price_cache:
        logger.debug(f"Cache hit for filter: {filter_string}")
        return price_cache[filter_string] # Return cached list (or None if previous fetch failed)

    api_url = f"{RETAIL_PRICES_API_ENDPOINT}?$filter={filter_string}"
    items = None # Default to None

    try:
        logger.info(f"Querying Retail API: {api_url}")
        response = requests.get(api_url, timeout=30) # Add timeout
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        items = data.get('Items', [])
        logger.info(f"Filter '{filter_string}' returned {len(items)} items.")
        if len(items) > 10:
            logger.debug(f"First 10 items sample: {items[:10]}") # Log sample if many items
        elif items:
            logger.debug(f"Items returned: {items}")

        # Follow 'NextPageLink' if present to fetch all results for the filter
        next_page_link = data.get('NextPageLink')
        while next_page_link:
            logger.info(f"Following NextPageLink: {next_page_link}")
            response = requests.get(next_page_link, timeout=30)
            response.raise_for_status()
            data = response.json()
            new_items = data.get('Items', [])
            if new_items:
                logger.info(f"Fetched {len(new_items)} more items from next page.")
                items.extend(new_items)
                logger.debug(f"Sample of newly added items: {new_items[:5]}")
            next_page_link = data.get('NextPageLink')

        logger.info(f"Total items retrieved for filter '{filter_string}': {len(items)}")

    except requests.exceptions.Timeout:
         logger.error(f"Timeout error querying Azure Retail Prices API ({api_url})")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error querying Azure Retail Prices API ({api_url}): {e}")
    except json.JSONDecodeError as e:
         logger.error(f"Error parsing price API response ({api_url}): {e}")
    except Exception as e:
         logger.error(f"Unexpected error fetching price ({api_url}): {e}", exc_info=True)

    # Cache the result (list of items or None if error occurred)
    price_cache[filter_string] = items
    return items

def find_best_match(
    items: List[Dict[str, Any]],
    location: str, # For logging context
    resource_desc: str, # For logging context (e.g., "P10 Premium Disk")
    required_price_type: str = 'Consumption',
    required_unit: Optional[str] = None, # e.g., "Month", "Hour", "GB/Month"
    meter_name_pattern: Optional[str] = None, # Regex pattern
    sku_name_pattern: Optional[str] = None, # Regex pattern
    product_name_pattern: Optional[str] = None, # Regex pattern
    prefer_contains_meter: Optional[List[str]] = None, # List of strings to prefer if found in meterName
    avoid_contains_meter: Optional[List[str]] = None, # List of strings to avoid if found in meterName
    strict_unit_match: bool = True # If True, unit must match exactly (case-insensitive)
) -> Optional[Dict[str, Any]]:
    """
    Selects the best matching price item from a list based on criteria.

    Args:
        items: List of price items from the API.
        location: Azure location (for logging).
        resource_desc: Description of the resource being priced (for logging).
        required_price_type: Typically 'Consumption'.
        required_unit: Expected unit (e.g., 'Month', 'Hour', 'GB/Month'). Case-insensitive check.
        meter_name_pattern: Regex to match against meterName.
        sku_name_pattern: Regex to match against skuName.
        product_name_pattern: Regex to match against productName.
        prefer_contains_meter: Strings that increase score if found in meterName.
        avoid_contains_meter: Strings that decrease score significantly if found in meterName.
        strict_unit_match: If True, item's unit must exactly match required_unit (case-insensitive).

    Returns:
        The best matching price item dictionary or None.
    """
    logger = logging.getLogger()
    if not items:
        logger.debug(f"No items provided to find_best_match for {resource_desc} in {location}.")
        return None

    logger.debug(f"Finding best match for {resource_desc} in {location} from {len(items)} items. Criteria: priceType='{required_price_type}', unit='{required_unit}', meterPat='{meter_name_pattern}', skuPat='{sku_name_pattern}', prodPat='{product_name_pattern}', prefer='{prefer_contains_meter}', avoid='{avoid_contains_meter}'")

    candidates = []
    for item in items:
        retail_price = item.get('retailPrice', 0)
        price_type = item.get('priceType', '')
        unit = item.get('unitOfMeasure', '').lower()
        meter_name = item.get('meterName', '')
        sku_name = item.get('skuName', '')
        product_name = item.get('productName', '')

        # --- Initial Filtering ---
        # Must have a positive price
        if retail_price <= 0:
            logger.log(5, f"Skipping item (price <= 0): {item}") # Trace level logging
            continue

        # Must match required price type (case-insensitive)
        if price_type.lower() != required_price_type.lower():
            logger.log(5, f"Skipping item (priceType mismatch: '{price_type}' vs '{required_price_type}'): {item}")
            continue

        # --- Scoring ---
        score = 0

        # Unit Matching (crucial)
        unit_matches = False
        if required_unit:
            req_unit_lower = required_unit.lower()
            if strict_unit_match:
                if unit == req_unit_lower:
                    unit_matches = True
                    score += 100 # Strong positive signal
            else: # Non-strict: check if required unit is *part* of the item's unit
                if req_unit_lower in unit:
                    unit_matches = True
                    score += 50 # Positive signal
        else:
            unit_matches = True # No unit requirement specified

        if not unit_matches:
            logger.log(5, f"Skipping item (unit mismatch: '{unit}' vs '{required_unit}', strict={strict_unit_match}): {item}")
            continue # If unit doesn't match requirement, discard immediately

        # Pattern Matching (Regex)
        if meter_name_pattern and re.search(meter_name_pattern, meter_name, re.IGNORECASE):
            score += 10
        if sku_name_pattern and re.search(sku_name_pattern, sku_name, re.IGNORECASE):
            score += 10
        if product_name_pattern and re.search(product_name_pattern, product_name, re.IGNORECASE):
            score += 10

        # Prefer/Avoid Keywords in Meter Name
        if prefer_contains_meter:
            for keyword in prefer_contains_meter:
                if keyword.lower() in meter_name.lower():
                    score += 5
        if avoid_contains_meter:
            for keyword in avoid_contains_meter:
                if keyword.lower() in meter_name.lower():
                    score -= 100 # Strong negative signal

        # --- Candidate Selection ---
        if score > 0: # Only consider items with a positive score
            candidates.append({'item': item, 'score': score})
            logger.log(5, f"Adding candidate (Score: {score}): {item}")

    if not candidates:
        logger.warning(f"No suitable candidates found for {resource_desc} in {location} after filtering {len(items)} items.")
        return None

    # Sort candidates by score (highest first)
    candidates.sort(key=lambda x: x['score'], reverse=True)

    best_candidate = candidates[0]
    logger.debug(f"Selected best match for {resource_desc} in {location} (Score: {best_candidate['score']}): {best_candidate['item']}")
    if len(candidates) > 1:
        logger.debug(f"Other potential candidates for {resource_desc}: {[c['item'] for c in candidates[1:6]]}") # Log top few alternatives

    return best_candidate['item']

def estimate_monthly_cost(price_info: Optional[Dict[str, Any]], console: Console = _console) -> Tuple[Optional[float], Optional[str]]:
    """Estimates monthly cost from a price info object."""
    if not price_info or price_info.get('retailPrice') is None or price_info.get('retailPrice') <= 0:
         logger.debug(f"Cannot estimate monthly cost, invalid price_info: {price_info}")
         return None, None # Corrected indentation

    price = price_info['retailPrice']
    unit = price_info.get('unitOfMeasure', '').lower()
    currency = price_info.get('currencyCode', 'USD') # Default to USD if missing
    meter_name = price_info.get('meterName', 'Unknown Meter') # For logging
    
    monthly_cost = None
    estimated_unit_str = f"{currency} / Month" # Assume monthly unless overridden

    # Handle common pricing units for conversion
    if 'hour' in unit:
        # Check for specific patterns like "1 Hour", "10 Hours", "100 Hours" etc.
        match = re.match(r"(\d+)\s+hour", unit)
        hours_per_unit = 1.0
        if match:
            hours_per_unit = float(match.group(1))

        if hours_per_unit > 0:
             monthly_cost = (price / hours_per_unit) * HOURS_PER_MONTH
             logger.debug(f"Calculated hourly cost for '{meter_name}': {price}/{hours_per_unit} = {price/hours_per_unit:.6f} {currency}/Hour -> Monthly: {monthly_cost:.4f}")
        else:
            logger.warning(f"Could not parse hours_per_unit from unit '{unit}' for meter '{meter_name}'. Cannot estimate monthly cost accurately.")
            return price, f"{currency} / {unit}" # Return raw price and unit

    elif 'gb/month' in unit or unit == '1 gb/month':
        monthly_cost = price # Price is already per GB/Month
        estimated_unit_str = f"{currency} / GB / Month"
        logger.debug(f"Cost is per GB/Month for '{meter_name}': {price:.4f} {estimated_unit_str}")
    elif unit == '1 gb' or unit == 'gb': # Sometimes used for snapshots/storage
        monthly_cost = price # Treat as per GB/Month for estimation consistency
        estimated_unit_str = f"{currency} / GB / Month"
        logger.debug(f"Assuming cost is per GB/Month for unit '{unit}' for '{meter_name}': {price:.4f} {estimated_unit_str}")
    elif 'month' in unit: 
         # Check for patterns like "1 Month", "100 / Month"
         # If it's a fixed monthly price, just use it.
         monthly_cost = price
         logger.debug(f"Cost is fixed monthly for '{meter_name}': {price:.4f} {estimated_unit_str}")
    # Add other unit conversions if needed (e.g., per 10k transactions)
    else:
         logger.warning(f"Cannot estimate monthly cost for meter '{meter_name}' with unit '{unit}'. Reporting raw price: {price} {currency}")
         return price, f"{currency} / {unit}" # Return raw price and unit

    return monthly_cost, estimated_unit_str

# --- Specific Cost Estimators --- 
def estimate_disk_cost(sku_name: str, size_gb: int, location: str, console: Console = _console) -> float:
    """Estimates the monthly cost for a managed disk using refined matching."""
    logger = logging.getLogger()
    api_location = location.lower().replace(' ', '')
    resource_desc = f"Disk SKU:'{sku_name}' Size:{size_gb}GB Loc:'{location}'"
    
    if not sku_name:
        logger.warning(f"Cannot estimate cost for {resource_desc}: SKU name is missing.")
        return 0.0
    if not size_gb or size_gb <= 0:
        logger.warning(f"Cannot estimate cost for {resource_desc}: Invalid size_gb.")
        return 0.0
        
    sku_lower = sku_name.lower()
    redundancy = "LRS" # Default LRS
    if 'zrs' in sku_lower:
        redundancy = "ZRS"
        
    # --- Build Filter and Criteria ---
    # Start with a reasonably broad filter
    filter_base = f"armRegionName eq '{api_location}' and serviceName eq 'Managed Disks' and priceType eq 'Consumption'"
    filter_str = filter_base
    items = None
    best_match = None

    # Criteria for find_best_match
    meter_pattern = None
    sku_pattern = None
    product_pattern = None
    required_unit = None # Will be set based on disk type
    prefer = [redundancy] # Prefer LRS or ZRS as determined
    avoid = ['Snapshot', 'Burst', 'Transaction'] # Avoid unrelated meters
    tier_name = "Unknown"

    try:
        if 'premium_v2' in sku_lower:
             tier_name = "Premium SSD v2"
             # V2 is priced per GB + IOPS + Throughput - complex, return 0 for now
             logger.warning(f"Premium SSD v2 cost estimation ({resource_desc}) is complex and not fully implemented. Returning 0 cost.")
             return 0.0 # TODO: Implement proper V2 pricing if needed
             # filter_str = f"{filter_base} and contains(meterName, 'Premium v2')" # Example filter if needed
             # required_unit = "GB/Month" # Base size cost
             # meter_pattern = r'Premium v2.+Data Disk'
             # product_pattern = r'Premium SSD v2'

        elif 'premium' in sku_lower: # Premium v1 SSD
            tier_name = "Premium SSD"
            perf_tier = DISK_SIZE_TO_TIER.get(size_gb, (None, None))[0]
            if perf_tier:
                # Filter broadly by SKU name convention
                filter_str = f"{filter_base} and contains(skuName, 'Premium_{redundancy}')"
                items = fetch_retail_prices(filter_str)
                # Match specific tier
                meter_pattern = rf'{perf_tier}\s+{redundancy}\s+Disk' # e.g., P10 LRS Disk
                sku_pattern = rf'Premium_{redundancy}'
                product_pattern = r'Premium SSD Managed Disks'
                required_unit = "Month" # Premium SSDs are typically fixed price per month per tier
                resource_desc = f"{perf_tier} {tier_name} {redundancy} Disk ({size_gb}GB) in {location}"
                best_match = find_best_match(items, location, resource_desc,
                                             required_unit=required_unit,
                                             meter_name_pattern=meter_pattern,
                                             sku_name_pattern=sku_pattern,
                                             product_name_pattern=product_pattern,
                                             prefer_contains_meter=prefer,
                                             avoid_contains_meter=avoid)
            else:
                logger.warning(f"Could not map size {size_gb}GB to a Premium performance tier for {resource_desc}.")

        elif 'standardssd' in sku_lower:
            tier_name = "Standard SSD"
            perf_tier = DISK_SIZE_TO_TIER.get(size_gb, (None, None))[1]
            if perf_tier:
                 # Try specific E-tier first (fixed price per month)
                 filter_str = f"{filter_base} and contains(skuName, 'StandardSSD_{redundancy}')"
                 items = fetch_retail_prices(filter_str)
                 meter_pattern = rf'{perf_tier}\s+{redundancy}\s+Disk' # e.g., E10 LRS Disk
                 sku_pattern = rf'StandardSSD_{redundancy}'
                 product_pattern = r'Standard SSD Managed Disks'
                 required_unit = "Month"
                 resource_desc_tier = f"{perf_tier} {tier_name} {redundancy} Disk ({size_gb}GB) in {location}"
                 best_match = find_best_match(items, location, resource_desc_tier,
                                              required_unit=required_unit,
                                              meter_name_pattern=meter_pattern,
                                              sku_name_pattern=sku_pattern,
                                              product_name_pattern=product_pattern,
                                              prefer_contains_meter=prefer,
                                              avoid_contains_meter=avoid)

            # Fallback/Alternative: Standard SSD per GB pricing (if E-tier fails or isn't found)
            if not best_match:
                logger.debug(f"Specific {tier_name} tier lookup failed or returned no match for {resource_desc}. Trying per-GB SSD meter...")
                filter_str = f"{filter_base} and contains(skuName, 'StandardSSD_{redundancy}')" # Keep filter broad
                items = fetch_retail_prices(filter_str) # Re-fetch if needed (cache helps)
                meter_pattern = r'Standard SSD.+Disks' # Matches 'Standard SSD LRS Disks' etc.
                sku_pattern = rf'StandardSSD_{redundancy}'
                product_pattern = r'Standard SSD Managed Disks'
                required_unit = "GB/Month" # Per-GB pricing
                resource_desc_gb = f"{tier_name} {redundancy} per-GB Disk ({size_gb}GB) in {location}"
                best_match = find_best_match(items, location, resource_desc_gb,
                                             required_unit=required_unit,
                                             meter_name_pattern=meter_pattern,
                                             sku_name_pattern=sku_pattern,
                                             product_name_pattern=product_pattern,
                                             prefer_contains_meter=prefer,
                                             avoid_contains_meter=avoid)

        elif 'standard' in sku_lower: # Standard HDD (must check after standardssd)
            tier_name = "Standard HDD"
            filter_str = f"{filter_base} and contains(skuName, 'Standard_{redundancy}')" # Broad filter
            items = fetch_retail_prices(filter_str)
            meter_pattern = r'Standard HDD.+Managed Disks' # e.g., Standard HDD LRS Managed Disks
            sku_pattern = rf'Standard_{redundancy}'
            product_pattern = r'Standard HDD Managed Disks'
            required_unit = "GB/Month" # Per-GB pricing
            resource_desc = f"{tier_name} {redundancy} Disk ({size_gb}GB) in {location}"
            best_match = find_best_match(items, location, resource_desc,
                                         required_unit=required_unit,
                                         meter_name_pattern=meter_pattern,
                                         sku_name_pattern=sku_pattern,
                                         product_name_pattern=product_pattern,
                                         prefer_contains_meter=prefer,
                                         avoid_contains_meter=avoid + ['Data Stored']) # Avoid storage account meters if possible
            
        elif 'ultradisk' in sku_lower:
            tier_name = "Ultra Disk"
            logger.warning(f"Ultra Disk cost estimation ({resource_desc}) is complex and not implemented. Returning 0 cost.")
            return 0.0 # Pricing is complex (IOPS, throughput, size)
        else:
             logger.warning(f"Could not determine disk tier for SKU: {sku_name} ({resource_desc}). Cannot estimate cost accurately.")
             return 0.0

    except Exception as e:
         logger.error(f"Error during disk cost estimation process for {resource_desc}: {e}", exc_info=True)
         return 0.0 # Return 0 on error

    # --- Calculate Cost ---
    cost_per_unit, unit_str = estimate_monthly_cost(best_match, console)
    final_cost = 0.0
    
    if cost_per_unit is not None and best_match:
        unit = best_match.get('unitOfMeasure', '').lower()
        # If price is per GB/Month, multiply by size
        if 'gb/month' in unit or unit == '1 gb' or unit == 'gb':
            final_cost = cost_per_unit * size_gb
            logger.info(f"Estimated cost for {resource_desc} ({tier_name}, {unit_str}): {cost_per_unit:.6f} * {size_gb} GB = {final_cost:.4f}")
        # Otherwise, assume it's a fixed monthly cost for the tier (Premium SSD, specific E-tiers)
        else: 
            final_cost = cost_per_unit
            logger.info(f"Estimated fixed cost for {resource_desc} ({tier_name}, {unit_str}): {final_cost:.4f}")
    elif not best_match:
         logger.warning(f"Could not find suitable price info for {resource_desc} ({tier_name}) in {location} after filtering. Returning 0 cost.")
         final_cost = 0.0
        
    return final_cost


def estimate_public_ip_cost(sku_name: str, location: str, console: Console = _console) -> float:
    """Estimates the monthly cost for an unused Static Public IP address."""
    logger = logging.getLogger() 
    api_location = location.lower().replace(' ', '')
    sku_filter = sku_name if sku_name else 'Basic' # Default to Basic if missing
    resource_desc = f"Public IP SKU:'{sku_filter}' Loc:'{location}'"

    # Ensure SKU name matches typical meter capitalization (e.g., 'Standard', 'Basic')
    sku_cap = sku_filter.capitalize()

    # Filter broadly for Virtual Network service and IP addresses
    filter_str = f"armRegionName eq '{api_location}' and serviceName eq 'Virtual Network' and contains(meterName, 'Public IP Address') and priceType eq 'Consumption'"
    items = fetch_retail_prices(filter_str)

    # Criteria for matching: Static IPv4 for the correct SKU
    meter_pattern = rf'{sku_cap} Static IPv4 Public IP Address Hours'
    product_pattern = rf'{sku_cap} Public IP Addresses' # Product name is often simpler
    sku_pattern = rf'{sku_cap}' # Match Basic or Standard skuName
    required_unit = "Hour" # IPs are priced per hour
    avoid = ['Dynamic', 'IPv6', 'Prefix'] # Avoid non-static, IPv6, or prefix costs

    best_match = find_best_match(items, location, resource_desc,
                                 required_unit=required_unit,
                                 meter_name_pattern=meter_pattern,
                                 sku_name_pattern=sku_pattern,
                                 product_name_pattern=product_pattern,
                                 avoid_contains_meter=avoid)

    cost_per_unit, _ = estimate_monthly_cost(best_match, console) # Gets monthly cost
    final_cost = cost_per_unit if cost_per_unit is not None else 0.0

    if final_cost == 0.0 and best_match: # Log if match found but cost is zero
        logger.warning(f"Estimated monthly cost for {resource_desc} is 0, despite finding match: {best_match}")
    elif not best_match:
        logger.warning(f"Could not find suitable price info for {resource_desc} using filter: {filter_str}")

    logger.info(f"Estimated cost for {resource_desc}: {final_cost:.4f}")
    return final_cost

def estimate_snapshot_cost(size_gb: int, location: str, sku_name: Optional[str], console: Console = _console) -> float:
    """Estimates the monthly cost for a managed disk snapshot."""
    logger = logging.getLogger() 
    api_location = location.lower().replace(' ', '')
    resource_desc = f"Snapshot Size:{size_gb}GB Loc:'{location}' SKU:'{sku_name}'"

    if not size_gb or size_gb <= 0:
         logger.warning(f"Cannot estimate cost for {resource_desc}: Invalid size_gb.")
         return 0.0

    # Determine redundancy from SKU name (default LRS)
    redundancy = "LRS"
    if sku_name and 'zrs' in sku_name.lower():
            redundancy = "ZRS"
    # Standard_Snapshot implies LRS, Premium_Snapshot implies LRS unless ZRS specified? API is unclear.
    # Assume Standard pricing for snapshots unless Premium is explicitly mentioned (less common for snapshots)
    storage_type = "Standard"
    if sku_name and 'premium' in sku_name.lower():
        storage_type = "Premium" # Although premium snapshots often cost same as standard?

    # Filter broadly for Managed Disk snapshots
    filter_str = f"armRegionName eq '{api_location}' and serviceName eq 'Managed Disks' and contains(meterName, 'Snapshot') and priceType eq 'Consumption'"
    items = fetch_retail_prices(filter_str)

    # Criteria for matching: Per GB/Month cost for the correct redundancy
    meter_pattern = rf'{storage_type}.+{redundancy}.+Snapshot' # e.g., "Standard LRS Snapshot" or "Premium ZRS Snapshot"
    # skuName for snapshots is often just Standard_LRS, Standard_ZRS etc.
    sku_pattern = rf'{storage_type}_{redundancy}' if storage_type == 'Standard' else rf'{storage_type}' # Be flexible
    product_pattern = r'Managed Disk Snapshots'
    required_unit = "GB/Month" # Snapshots are priced per GB/Month
    prefer = [redundancy, storage_type]
    avoid = ['Premium Disk', 'Standard Disk'] # Avoid actual disk meters

    resource_desc = f"{storage_type} {redundancy} {resource_desc}"
    best_match = find_best_match(items, location, resource_desc,
                                 required_unit=required_unit,
                                 meter_name_pattern=meter_pattern,
                                 sku_name_pattern=sku_pattern,
                                 product_name_pattern=product_pattern,
                                 prefer_contains_meter=prefer,
                                 avoid_contains_meter=avoid)

    cost_per_gb, unit_str = estimate_monthly_cost(best_match, console)
    final_cost = 0.0

    if cost_per_gb is not None:
        final_cost = cost_per_gb * size_gb
        logger.info(f"Estimated cost for {resource_desc} ({unit_str}): {cost_per_gb:.6f} * {size_gb} GB = {final_cost:.4f}")
    elif not best_match:
         logger.warning(f"Could not find suitable price info for {resource_desc} using filter: {filter_str}")

    return final_cost


def estimate_app_service_plan_cost(sku_tier: str, sku_name: str, location: str, console: Console = _console) -> float:
    """Estimates the monthly cost for an App Service Plan."""
    logger = logging.getLogger()
    api_location = location.lower().replace(' ', '')
    resource_desc = f"ASP SKU:'{sku_name}' Tier:'{sku_tier}' Loc:'{location}'"

    if not sku_tier or not sku_name:
         logger.warning(f"Cannot estimate cost for {resource_desc}: Missing SKU tier or name.")
         return 0.0

    # Normalize sku_name (e.g., P1V2 -> P1v2) for consistency if needed, but API usually uses P1V2 format.
    # The API meterName often includes the instance size directly, e.g., "P1V2 App Service Hours"

    # Filter broadly for App Service plans
    filter_str = f"armRegionName eq '{api_location}' and serviceFamily eq 'Compute' and contains(serviceName, 'App Service') and contains(productName, 'App Service Plan') and priceType eq 'Consumption'"
    items = fetch_retail_prices(filter_str)

    # Criteria for matching
    # Meter name often directly contains the SKU like 'P1V2', 'B1', 'S1' etc.
    # Sometimes includes OS like 'Windows' or 'Linux' - try to be flexible
    meter_pattern = rf'{sku_name}\s+(Windows|Linux)?\s*App Service Hours' # Match SKU name + optional OS + 'App Service Hours'
    # skuName in API might be like 'Premium V2' or 'Basic'. Tier gives 'PremiumV2', 'Basic'.
    sku_pattern = rf'{sku_tier}' # Match the tier name (e.g., Basic, PremiumV2)
    product_pattern = r'App Service Plan'
    required_unit = "Hour" # ASPs are priced per hour
    prefer = [sku_name] # Prefer meters containing the exact SKU name (e.g., P1V2)
    # Avoid isolated plans or other non-standard ASP types unless specifically requested
    avoid = ['Isolated', 'ASE', 'Functions', 'Stamp Fee']

    resource_desc = f"{sku_name} {resource_desc}"
    best_match = find_best_match(items, location, resource_desc,
                                 required_unit=required_unit,
                                 meter_name_pattern=meter_pattern,
                                 sku_name_pattern=sku_pattern,
                                 product_name_pattern=product_pattern,
                                 prefer_contains_meter=prefer,
                                 avoid_contains_meter=avoid)

    cost_per_unit, _ = estimate_monthly_cost(best_match, console) # Gets monthly cost
    final_cost = cost_per_unit if cost_per_unit is not None else 0.0

    if final_cost == 0.0 and best_match:
        logger.warning(f"Estimated monthly cost for {resource_desc} is 0, despite finding match: {best_match}")
    elif not best_match:
         logger.warning(f"Could not find suitable price info for {resource_desc} using filter: {filter_str}")

    logger.info(f"Estimated cost for {resource_desc}: {final_cost:.4f}")
    return final_cost


def estimate_sql_database_cost(sku_tier: Optional[str], sku_name: Optional[str], family: Optional[str], capacity: Optional[int], location: str, console: Console = _console) -> float:
    """Estimates the monthly cost for an Azure SQL Database (DTU or vCore)."""
    # This is complex due to DTU vs vCore, serverless, hyperscale, license included/AHB etc.
    # Providing a basic estimation for common DTU/vCore models.
    logger = logging.getLogger()
    api_location = location.lower().replace(' ', '')
    resource_desc = f"SQL DB SKU:'{sku_name}' Tier:'{sku_tier}' Family:'{family}' Cap:'{capacity}' Loc:'{location}'"

    if not sku_tier or not sku_name:
        logger.warning(f"Cannot estimate cost for {resource_desc}: Missing SKU tier or name.")
        return 0.0

    tier_lower = sku_tier.lower()
    filter_str = f"armRegionName eq '{api_location}' and serviceFamily eq 'Databases' and contains(serviceName, 'SQL Database') and priceType eq 'Consumption'"
    # Further refine filter based on tier if possible
    if 'vcore' in tier_lower or (family and 'vcore' in family.lower()):
        filter_str += " and contains(meterName, 'vCore')"
    elif 'dtu' in tier_lower:
         filter_str += " and (contains(meterName, 'DTU') or contains(productName, 'DTU'))"

    items = fetch_retail_prices(filter_str)

    meter_pattern = None
    sku_pattern = sku_name # Match the specific SKU like 'GP_Gen5_8' or 'Basic'
    product_pattern = None
    required_unit = None # DTU is often /Month, vCore /Hour
    prefer = []
    avoid = ['Serverless', 'Hyperscale', 'Data Stored', 'Backup', 'Failover']

    if 'vcore' in tier_lower or (family and 'vcore' in family.lower()):
        # vCore: Usually priced per vCore-Hour. Need capacity.
        required_unit = "Hour"
        product_pattern = r'SQL Database Single/Elastic Pool General Purpose - vCore' # Example, need variations
        meter_pattern = rf'{sku_name}.+vCore Hours' # e.g., GP_Gen5_8 vCore Hours
        prefer = [sku_name, 'vCore']
        if capacity:
            resource_desc = f"{sku_name} ({capacity} vCores) {resource_desc}"
        else:
             logger.warning(f"vCore capacity missing for {resource_desc}, cost might be inaccurate.")


    elif 'dtu' in tier_lower:
        # DTU: Often priced per DTU pack per month (e.g., "10 DTUs", "Basic DTU")
        required_unit = "Month" # Or sometimes Hour for specific meters? Be flexible
        # Meter name might be "Basic DTUs", "100 DTUs", "S1 DTUs" etc.
        meter_pattern = rf'({sku_name}|{capacity})\s+DTU' # Try matching SKU name (Basic) or capacity (100) + DTU
        product_pattern = r'(DTU|Basic|Standard|Premium)' # Match product names containing DTU or tier
        prefer = [sku_name] if sku_name else []
        if capacity:
             prefer.append(str(capacity))
             resource_desc = f"{sku_name} ({capacity} DTUs) {resource_desc}"


    else:
        logger.warning(f"Could not determine pricing model (DTU/vCore) for {resource_desc}. Cannot estimate cost.")
        return 0.0

    best_match = find_best_match(items, location, resource_desc,
                                 required_unit=required_unit,
                                 meter_name_pattern=meter_pattern,
                                 sku_name_pattern=sku_pattern,
                                 product_name_pattern=product_pattern,
                                 prefer_contains_meter=prefer,
                                 avoid_contains_meter=avoid,
                                 strict_unit_match=False) # Be flexible with units for SQL DB


    cost_per_unit, _ = estimate_monthly_cost(best_match, console) # Gets monthly cost
    final_cost = cost_per_unit if cost_per_unit is not None else 0.0

     # Adjust for vCore capacity if priced per vCore-hour
    if best_match and 'vcore' in tier_lower and capacity and capacity > 0 and best_match.get('unitOfMeasure','').lower() == '1 hour':
         # estimate_monthly_cost already converted per-hour to monthly, assume price is for 1 vCore
         final_cost = final_cost * capacity
         logger.info(f"Adjusting vCore cost for {capacity} cores: {cost_per_unit:.4f} * {capacity} = {final_cost:.4f}")

    if final_cost == 0.0 and best_match:
        logger.warning(f"Estimated monthly cost for {resource_desc} is 0, despite finding match: {best_match}")
    elif not best_match:
        logger.warning(f"Could not find suitable price info for {resource_desc} using filter: {filter_str}")

    logger.info(f"Estimated cost for {resource_desc}: {final_cost:.4f}")
    return final_cost

# --- Cost Management API Integration (Existing - Minor adjustments might be needed) ---
# Keep the existing function to get overall cost breakdown, as it uses a different API.

def get_cost_data(credential, subscription_id, cost_mgmt_client: CostManagementClient, console: Console = _console) -> Tuple[Dict, float, str]:
    """
    Gets cost data aggregated by service name for the current billing month.
    Uses Azure Cost Management API.
    Returns: (costs_by_service, total_cost, currency_code)
    """
    logger = logging.getLogger()
    logger.info("Fetching cost data using Cost Management API...")

    # TODO: Determine current billing period start/end dates dynamically if needed
    # For simplicity, using a fixed time period (e.g., last 30 days or current month) might suffice
    # Example: Use current month to date
    from datetime import datetime, timedelta
    today = datetime.utcnow()
    first_day_of_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # Cost management API might be slightly delayed, consider looking back slightly more?
    # Or use a fixed lookback like 30 days
    start_date = first_day_of_month # Or today - timedelta(days=30)
    end_date = today

    logger.info(f"Querying costs from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    costs_by_service = {}
    total_cost = 0.0
    currency_code = "USD" # Default

    try:
        # Query definition for actual costs, grouped by service name
        query_definition = {
            "type": "ActualCost",
            "timeframe": "Custom",
            "time_period": {
                "from": start_date.isoformat() + "Z",
                "to": end_date.isoformat() + "Z"
            },
            "dataset": {
                "granularity": "None", # Aggregate over the whole period
                "aggregation": {
                "totalCost": {
                    "name": "Cost",
                    "function": "Sum"
                }
            },
                "grouping": [
                    {
                        "type": "Dimension",
                        "name": "ServiceName"
                    }
                ]
            }
        }

        scope = f"subscriptions/{subscription_id}"
        query_result = cost_mgmt_client.query.usage(scope=scope, parameters=query_definition)

        if query_result and query_result.rows:
            # Assuming columns are [Cost, Currency, ServiceName, ResourceGroup, UsageDate] - Check API response structure
            # Find indices - safer than hardcoding
            try:
                cost_idx = query_result.columns.index(next(c for c in query_result.columns if c.name.lower() == 'cost'))
                currency_idx = query_result.columns.index(next(c for c in query_result.columns if c.name.lower() == 'currency'))
                service_name_idx = query_result.columns.index(next(c for c in query_result.columns if c.name.lower() == 'servicename'))
            except (StopIteration, ValueError) as e:
                 logger.error(f"Could not find expected columns (Cost, Currency, ServiceName) in Cost Management API response: {query_result.columns}. Error: {e}")
                 return {}, 0.0, currency_code # Return empty if structure is wrong

            for row in query_result.rows:
                cost = float(row[cost_idx])
                currency = row[currency_idx]
                service_name = row[service_name_idx]

                if service_name is None: service_name = "Uncategorized" # Handle null service names

                costs_by_service[service_name] = costs_by_service.get(service_name, 0.0) + cost
                total_cost += cost
                currency_code = currency # Assume currency is consistent

            logger.info(f"Successfully processed cost data. Total Cost: {total_cost:.2f} {currency_code}. Breakdown by service: {costs_by_service}")
            
        else:
             logger.warning("Cost Management query returned no rows or empty result.")

    except Exception as e:
        logger.error(f"Error fetching cost data from Cost Management API: {e}", exc_info=True)
        # Optionally: Provide a more user-friendly message via console
        console.print(f"  [yellow]Warning:[/yellow] Failed to retrieve detailed cost breakdown from Azure Cost Management API: {e}")

    return costs_by_service, total_cost, currency_code 