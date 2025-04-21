import logging
import requests
import json
import re # Import regex for flexible matching
from typing import List, Dict, Any, Optional, Tuple # Add typing
from rich.console import Console # Keep for potential future use or passthrough
from azure.mgmt.costmanagement import CostManagementClient # Added import
from collections import defaultdict
from datetime import datetime, timedelta, timezone

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
    """Estimates the monthly cost of an Azure Managed Disk using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating cost for disk: sku={sku_name}, size={size_gb}GB, location={location}")

    # Determine disk type (Premium SSD, Standard SSD, Standard HDD)
    is_premium = 'premium' in sku_name.lower()
    is_ssd = 'ssd' in sku_name.lower()
    is_hdd = 'hdd' in sku_name.lower() or not (is_premium or is_ssd) # Assume HDD if not SSD
    is_ultra = 'ultra' in sku_name.lower() # Ultra disks have complex pricing, skip for now

    if is_ultra:
        logger.warning(f"Ultra Disk cost estimation is not supported yet (SKU: {sku_name}). Returning 0.")
        return 0.0

    # Find the closest matching tier based on size for SSDs
    disk_tier = None
    if size_gb > 0 and (is_premium or is_ssd):
        closest_size = min(DISK_SIZE_TO_TIER.keys(), key=lambda k: abs(k - size_gb) if k >= size_gb else float('inf'))
        if closest_size >= size_gb:
            tier_map = DISK_SIZE_TO_TIER[closest_size]
            disk_tier = tier_map[0] if is_premium else tier_map[1] # 0 for Premium, 1 for Standard SSD
            logger.debug(f"Mapped size {size_gb}GB to closest tier size {closest_size}GB -> Tier: {disk_tier}")
        else:
             logger.warning(f"Could not map disk size {size_gb}GB to a standard tier for {sku_name}. Pricing might be inaccurate.")

    # --- Build Filter ---
    filter_parts = [
        f"armRegionName eq '{location}'",
        f"priceType eq 'Consumption'",
        "contains(meterName, '/Month')" # Base disks are priced per month
    ]

    meter_name_pattern = None
    product_name_pattern = None
    service_name = 'Managed Disks' # Default service name

    if is_premium and disk_tier:
        # Premium SSD (e.g., P10)
        filter_parts.append(f"skuName eq '{disk_tier}'")
        filter_parts.append("serviceName eq 'Managed Disks'")
        # meterName example: "P10 LRS Disk"
        meter_name_pattern = re.escape(disk_tier) + r'\s+(LRS|ZRS)?\s+Disk'
        product_name_pattern = r'Premium SSD'
    elif is_ssd and disk_tier:
        # Standard SSD (e.g., E10)
        filter_parts.append(f"skuName eq '{disk_tier}'")
        filter_parts.append("serviceName eq 'Managed Disks'")
        # meterName example: "E10 LRS Disk"
        meter_name_pattern = re.escape(disk_tier) + r'\s+(LRS|ZRS)?\s+Disk'
        product_name_pattern = r'Standard SSD'
    elif is_hdd:
        # Standard HDD - Priced per GB/Month + Transactions
        # We only estimate the storage cost here.
        filter_parts.append("serviceName eq 'Storage'") # Uses Storage service
        filter_parts.append("contains(productName, 'Standard HDD Managed Disks')")
        filter_parts.append("contains(meterName, 'LRS Disk')") # e.g., S4 LRS Disk, S10 LRS Disk...
        # We need to find the *per GB* price, not a specific tier price
        meter_name_pattern = r'Standard HDD Managed Disks LRS Storage' # Match the GB meter
        product_name_pattern = r'Standard HDD Managed Disks'
        # Adjust unit requirement for HDD
        required_unit = 'GB/Month'
    else:
        logger.warning(f"Could not determine pricing strategy for disk SKU '{sku_name}' (Tier: {disk_tier}). Returning 0.")
        return 0.0

    filter_string = " and ".join(filter_parts)
    items = fetch_retail_prices(filter_string)

    if not items:
        logger.warning(f"No price items found for disk: {sku_name} ({disk_tier}) in {location}. Filter: {filter_string}")
        return 0.0

    # --- Find Best Match ---
    # HDD needs specific unit handling
    best_match = find_best_match(
        items,
        location=location,
        resource_desc=f"{sku_name} ({disk_tier}) Disk",
        required_unit = '1 GB/Month' if is_hdd else '1/Month', # Match "1/Month" or "1 GB/Month"
        meter_name_pattern=meter_name_pattern,
        product_name_pattern=product_name_pattern,
        strict_unit_match=True # Units should be exact for disks
    )

    if not best_match:
        logger.warning(f"Could not find best price match for disk: {sku_name} ({disk_tier}) in {location}.")
        return 0.0

    # --- Calculate Cost ---
    monthly_cost, unit_str = estimate_monthly_cost(best_match)

    if monthly_cost is None:
        logger.error(f"Failed to estimate monthly cost from matched price for disk {sku_name}.")
        return 0.0

    # Adjust HDD cost based on actual size
    if is_hdd:
        if unit_str and 'gb' in unit_str.lower():
            final_cost = monthly_cost * size_gb
            logger.info(f"Estimated Standard HDD cost for {size_gb}GB: {monthly_cost:.6f} {unit_str} * {size_gb} GB = {final_cost:.2f} {best_match.get('currencyCode', 'USD')}/Month")
            return final_cost
        else:
             logger.error(f"Standard HDD price found, but unit '{unit_str}' was not per GB. Cannot calculate final cost for {sku_name}.")
             return 0.0 # Cannot calculate accurately
    else:
        # Premium/Standard SSD cost is per disk tier
        logger.info(f"Estimated Disk cost for {sku_name} ({disk_tier}) in {location}: {monthly_cost:.2f} {unit_str}")
        return monthly_cost


def estimate_public_ip_cost(sku_name: str, location: str, console: Console = _console) -> float:
    """Estimates the monthly cost of an Azure Public IP address using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating cost for Public IP: sku={sku_name}, location={location}")

    # Determine IP type (Basic vs Standard)
    ip_type = "Standard" if "standard" in sku_name.lower() else "Basic"

    # --- Build Filter ---
    # Public IPs often fall under "Networking" or sometimes more specific services
    filter_parts = [
        f"armRegionName eq '{location}'",
        f"priceType eq 'Consumption'",
        f"(serviceName eq 'Networking' or serviceName eq 'Virtual Network')", # Broaden service search
        f"contains(productName, 'IP Address')", # Ensure it relates to IP Addresses
        f"skuName eq '{ip_type}'" # Match Basic or Standard SKU
    ]

    filter_string = " and ".join(filter_parts)
    items = fetch_retail_prices(filter_string)

    if not items:
        logger.warning(f"No price items found for {ip_type} Public IP in {location}. Filter: {filter_string}")
        return 0.0

    # --- Find Best Match ---
    # Look for meters related to IP address usage, often priced per hour
    best_match = find_best_match(
        items,
        location=location,
        resource_desc=f"{ip_type} Public IP",
        required_unit = '1 Hour', # IPs are often hourly
        # Meter names vary, e.g., "Standard Public IP address", "Basic Public IP address hours"
        meter_name_pattern=rf'{ip_type}.*IP Address.*Hour',
        product_name_pattern=r'Public IP Address',
        strict_unit_match=True
    )

    if not best_match:
        # Retry with less strict meter name pattern if first attempt fails
        logger.debug(f"Retrying Public IP price match for {ip_type} in {location} with broader meter pattern.")
        best_match = find_best_match(
            items,
            location=location,
            resource_desc=f"{ip_type} Public IP",
            required_unit = '1 Hour',
            meter_name_pattern=r'IP Address.*Hour', # Simpler pattern
            product_name_pattern=r'Public IP Address',
            strict_unit_match=True
        )

    if not best_match:
         logger.warning(f"Could not find best price match for {ip_type} Public IP in {location}.")
         return 0.0

    # --- Calculate Cost ---
    monthly_cost, unit_str = estimate_monthly_cost(best_match)

    if monthly_cost is None:
        logger.error(f"Failed to estimate monthly cost from matched price for {ip_type} Public IP.")
        return 0.0

    logger.info(f"Estimated Public IP cost for {ip_type} in {location}: {monthly_cost:.2f} {unit_str}")
    return monthly_cost

def estimate_snapshot_cost(size_gb: int, location: str, sku_name: Optional[str], console: Console = _console) -> float:
    """Estimates the monthly cost of a Managed Disk Snapshot using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating cost for Snapshot: size={size_gb}GB, location={location}, sku={sku_name}")

    # Determine snapshot type (Standard HDD, Standard SSD, Premium SSD based)
    # Default to Standard HDD if SKU is missing or unknown
    storage_type = "Standard HDD" # Default
    meter_name_keyword = "Standard Snapshot"
    if sku_name:
        sku_lower = sku_name.lower()
        if "premium" in sku_lower:
            storage_type = "Premium SSD"
            meter_name_keyword = "Premium Snapshot"
        elif "standard_ssd" in sku_lower or "standardssd" in sku_lower: # Allow for variations
            storage_type = "Standard SSD"
            meter_name_keyword = "Standard Snapshot" # SSD uses "Standard" keyword too
        # Standard HDD is the default if not premium/standard_ssd

    # Snapshots are usually priced per GB/Month
    required_unit = '1 GB/Month'

    # --- Build Filter ---
    # Snapshots pricing might be under "Storage" or "Managed Disks"
    filter_parts = [
        f"armRegionName eq '{location}'",
        f"priceType eq 'Consumption'",
        f"(serviceName eq 'Storage' or serviceName eq 'Managed Disks')",
        f"contains(productName, 'Managed Disk Snapshot')",
        f"contains(meterName, '{meter_name_keyword}')" # Match Standard or Premium meter
        # SKU for snapshots often relates to redundancy (LRS, ZRS), not perf tier directly
    ]
    if sku_name and ("zrs" in sku_name.lower()):
         filter_parts.append("contains(meterName, 'ZRS')")
    else:
         filter_parts.append("contains(meterName, 'LRS')") # Default to LRS

    filter_string = " and ".join(filter_parts)
    items = fetch_retail_prices(filter_string)

    if not items:
        logger.warning(f"No price items found for {storage_type} Snapshot in {location}. Filter: {filter_string}")
        return 0.0

    # --- Find Best Match ---
    # Meter names like "Standard LRS Snapshot", "Premium ZRS Snapshot"
    best_match = find_best_match(
        items,
        location=location,
        resource_desc=f"{storage_type} Snapshot ({sku_name})",
        required_unit=required_unit,
        meter_name_pattern=rf'{meter_name_keyword}.*(LRS|ZRS)', # Should contain LRS or ZRS
        product_name_pattern=r'Managed Disk Snapshot',
        strict_unit_match=True
    )

    if not best_match:
         logger.warning(f"Could not find best price match for {storage_type} Snapshot ({sku_name}) in {location}.")
         return 0.0

    # --- Calculate Cost ---
    monthly_cost_per_gb, unit_str = estimate_monthly_cost(best_match)

    if monthly_cost_per_gb is None:
        logger.error(f"Failed to estimate monthly cost from matched price for {storage_type} Snapshot.")
        return 0.0

    if unit_str and 'gb' in unit_str.lower():
        final_cost = monthly_cost_per_gb * size_gb
        logger.info(f"Estimated Snapshot cost for {size_gb}GB ({storage_type}, {sku_name}) in {location}: {monthly_cost_per_gb:.6f} {unit_str} * {size_gb} GB = {final_cost:.2f} {best_match.get('currencyCode', 'USD')}/Month")
        return final_cost
    else:
        logger.error(f"Snapshot price found, but unit '{unit_str}' was not per GB. Cannot calculate final cost for {storage_type} snapshot.")
        return 0.0

def estimate_app_service_plan_cost(sku_tier: str, sku_name: str, location: str, console: Console = _console) -> float:
    """Estimates the monthly cost of an App Service Plan using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating cost for App Service Plan: tier={sku_tier}, sku={sku_name}, location={location}")

    # --- Build Filter ---
    # ASP pricing falls under "App Service"
    filter_parts = [
        f"armRegionName eq '{location}'",
        f"priceType eq 'Consumption'",
        f"serviceName eq 'App Service'",
        # Try matching SKU name directly first
        f"(skuName eq '{sku_name}' or contains(meterName, '{sku_name}'))" # Match SKU or Meter containing SKU (e.g., P1v2)
    ]

    # Tier might help disambiguate (Free, Shared, Basic, Standard, Premium, PremiumV2, PremiumV3, Isolated, IsolatedV2)
    if sku_tier:
        filter_parts.append(f"contains(productName, '{sku_tier}')")

    filter_string = " and ".join(filter_parts)
    items = fetch_retail_prices(filter_string)

    if not items:
        logger.warning(f"No price items found for App Service Plan: {sku_tier} {sku_name} in {location}. Filter: {filter_string}")
        # Fallback: Broaden search without specific SKU name match in filter? Risky.
        return 0.0

    # --- Find Best Match ---
    # Look for meters related to the plan instance hours, e.g., "P1v2 App Service plan hours"
    # The exact meterName format can vary significantly.
    best_match = find_best_match(
        items,
        location=location,
        resource_desc=f"{sku_tier} {sku_name} App Service Plan",
        required_unit='1 Hour', # ASPs are typically billed hourly
        # Prefer meters containing the SKU name and "Hour"
        meter_name_pattern=rf'{sku_name}.*Hour',
        product_name_pattern=rf'{sku_tier}', # Match product containing tier
        prefer_contains_meter=[sku_name, 'App Service plan'],
        strict_unit_match=True
    )

    # Retry logic if initial match fails (e.g., for Basic tiers B1, B2, B3)
    if not best_match and sku_tier.lower() == 'basic':
        logger.debug(f"Retrying ASP price match for Basic tier {sku_name} with different patterns.")
        best_match = find_best_match(
            items,
            location=location,
            resource_desc=f"{sku_tier} {sku_name} App Service Plan",
            required_unit='1 Hour',
            meter_name_pattern=rf'{sku_tier}.*Hour', # Try matching tier in meter name
            product_name_pattern=r'App Service', # Broader product match
            prefer_contains_meter=[sku_name, 'Compute Hours'],
            strict_unit_match=True
        )

    if not best_match:
         logger.warning(f"Could not find best price match for App Service Plan: {sku_tier} {sku_name} in {location}.")
         return 0.0

    # --- Calculate Cost ---
    monthly_cost, unit_str = estimate_monthly_cost(best_match)

    if monthly_cost is None:
        logger.error(f"Failed to estimate monthly cost from matched price for ASP {sku_tier} {sku_name}.")
        return 0.0

    logger.info(f"Estimated App Service Plan cost for {sku_tier} {sku_name} in {location}: {monthly_cost:.2f} {unit_str}")
    return monthly_cost

def estimate_sql_database_cost(sku_tier: Optional[str], sku_name: Optional[str], family: Optional[str], capacity: Optional[int], location: str, console: Console = _console) -> float:
    """Estimates the monthly cost of an Azure SQL Database (DTU or vCore) using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating cost for SQL Database: tier={sku_tier}, sku={sku_name}, family={family}, capacity={capacity}, location={location}")

    # Determine if DTU or vCore model
    is_dtu_model = sku_tier is not None and sku_tier.lower() in ['basic', 'standard', 'premium']
    is_vcore_model = sku_tier is not None and sku_tier.lower() in ['general purpose', 'business critical', 'hyperscale']

    if not is_dtu_model and not is_vcore_model:
         # Try inferring from SKU name if tier is ambiguous/missing
        if sku_name and (sku_name.startswith('Basic') or sku_name.startswith('S') or sku_name.startswith('P')):
             is_dtu_model = True
        elif sku_name and (sku_name.startswith('GP_') or sku_name.startswith('BC_') or sku_name.startswith('HS_')):
             is_vcore_model = True
        else:
            logger.warning(f"Cannot determine if SQL DB is DTU or vCore model (tier={sku_tier}, sku={sku_name}). Cannot estimate cost.")
            return 0.0

    filter_parts = [
        f"armRegionName eq '{location}'",
        f"priceType eq 'Consumption'",
        f"serviceName eq 'SQL Database'"
    ]
    required_unit = None
    meter_name_pattern = None
    product_name_pattern = None
    prefer_meter = []

    # --- Build Filter based on Model ---
    if is_dtu_model:
        # DTU Model (Basic, Standard S0-S12, Premium P1-P15)
        required_unit = 'DTU/Month' if sku_tier.lower() == 'basic' else 'DTUs/Month' # Basic is DTU, others DTUs
        if sku_tier.lower() == 'basic':
            filter_parts.append("skuName eq 'Basic'")
            meter_name_pattern = r'Basic.*DTU'
            product_name_pattern = r'SQL Database Basic'
        elif sku_tier.lower() == 'standard':
             filter_parts.append(f"contains(skuName, '{sku_name}')") # e.g., S0, S1
             meter_name_pattern = rf'{sku_name}.*DTUs'
             product_name_pattern = r'SQL Database Standard'
        elif sku_tier.lower() == 'premium':
             filter_parts.append(f"contains(skuName, '{sku_name}')") # e.g., P1, P2
             meter_name_pattern = rf'{sku_name}.*DTUs'
             product_name_pattern = r'SQL Database Premium'
        else: # Should not happen based on initial check
            logger.error(f"Unexpected DTU tier: {sku_tier}")
            return 0.0

    elif is_vcore_model:
        # vCore Model (General Purpose, Business Critical, Hyperscale)
        # Pricing is per vCore Hour + Storage GB/Month. Estimate compute only here.
        required_unit = 'vCore/Hour'
        filter_parts.append(f"contains(skuName, '{family}')") # e.g., Gen5
        filter_parts.append(f"contains(meterName, 'vCore')")
        filter_parts.append("contains(meterName, 'Hour')")

        if sku_tier.lower() == 'general purpose':
            product_name_pattern = r'SQL Database General Purpose'
            prefer_meter.append('General Purpose')
        elif sku_tier.lower() == 'business critical':
            product_name_pattern = r'SQL Database Business Critical'
            prefer_meter.append('Business Critical')
        elif sku_tier.lower() == 'hyperscale':
            product_name_pattern = r'SQL Database Hyperscale'
            prefer_meter.append('Hyperscale')

        # Add hardware generation (family) preference if available
        if family:
             prefer_meter.append(family) # e.g., Gen5

    # --- Fetch and Match ---
    filter_string = " and ".join(filter_parts)
    items = fetch_retail_prices(filter_string)

    if not items:
        logger.warning(f"No price items found for SQL DB: {sku_tier} {sku_name} {family} in {location}. Filter: {filter_string}")
        return 0.0

    best_match = find_best_match(
        items,
        location=location,
        resource_desc=f"SQL DB {sku_tier} {sku_name} {family} {capacity}",
        required_unit=required_unit,
        meter_name_pattern=meter_name_pattern,
        product_name_pattern=product_name_pattern,
        prefer_contains_meter=prefer_meter,
        strict_unit_match=False # Units can vary slightly (DTU/DTUs, vCore Hour/vCore Hours)
    )

    if not best_match:
         logger.warning(f"Could not find best price match for SQL DB: {sku_tier} {sku_name} {family} in {location}.")
         return 0.0

    # --- Calculate Cost ---
    cost_per_unit, unit_str = estimate_monthly_cost(best_match)

    if cost_per_unit is None:
        logger.error(f"Failed to estimate monthly cost per unit for SQL DB {sku_tier} {sku_name}.")
        return 0.0

    final_cost = 0.0
    currency = best_match.get('currencyCode', 'USD')

    if capacity is not None and capacity > 0:
        if is_dtu_model and unit_str and 'dtu' in unit_str.lower():
            # DTU price is per DTU/Month or per 10 DTUs/Month etc.
            # We need to scale by the *actual* capacity (DTUs) of the database
            # Find number of DTUs per unit from meter name if possible
            unit_factor = 1.0
            meter_name = best_match.get('meterName', '')
            match = re.search(r'(\d+)\s+DTUs', meter_name)
            if match:
                unit_factor = float(match.group(1))
            elif 'DTU/' in unit_str: # Handles "DTU/Month"
                unit_factor = 1.0

            if unit_factor > 0:
                price_per_single_dtu = cost_per_unit / unit_factor
                final_cost = price_per_single_dtu * capacity
                logger.info(f"Estimated SQL DB (DTU) cost: {cost_per_unit:.4f} {currency}/{unit_factor} DTUs/Month -> {price_per_single_dtu:.6f} /DTU * {capacity} DTUs = {final_cost:.2f} {currency}/Month")
            else:
                 logger.error(f"Could not determine unit factor for DTU meter '{meter_name}'. Cannot calculate cost.")

        elif is_vcore_model and unit_str and 'vcore' in unit_str.lower() and 'hour' in unit_str.lower():
            # vCore cost is per vCore/Hour. estimate_monthly_cost already converted to monthly per vCore.
            final_cost = cost_per_unit * capacity
            logger.info(f"Estimated SQL DB (vCore Compute) cost: {cost_per_unit:.4f} {currency}/vCore/Month * {capacity} vCores = {final_cost:.2f} {currency}/Month")
        else:
             logger.error(f"Could not match calculated cost unit '{unit_str}' to expected model ({'DTU' if is_dtu_model else 'vCore'}) or capacity {capacity}. Cannot calculate cost accurately.")
             # Return cost_per_unit as a fallback? Might be confusing.
             final_cost = cost_per_unit # Return the base cost per unit if scaling fails
    else:
         logger.warning(f"SQL DB capacity not provided or zero ({capacity}). Returning cost per base unit: {cost_per_unit:.2f} {unit_str}")
         final_cost = cost_per_unit # Return cost for 1 unit if capacity unknown

    return final_cost

def estimate_vm_cost(vm_size: str, location: str, os_type: str = 'Linux', console: Console = _console) -> float:
    """Estimates the monthly compute cost of an Azure VM using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating compute cost for VM: size={vm_size}, location={location}, os={os_type}")

    # --- Build Filter ---
    filter_parts = [
        f"armRegionName eq '{location}'",
        f"priceType eq 'Consumption'",
        f"serviceName eq 'Virtual Machines'",
        f"contains(skuName, '{vm_size}')", # Match the VM size (e.g., Standard_D2s_v3)
        "contains(meterName, 'Compute')" # Focus on compute meters
    ]
    # Add OS preference - Windows meters often include "Windows"
    os_filter = "not contains(meterName, 'Windows')" if os_type.lower() == 'linux' else "contains(meterName, 'Windows')"
    filter_parts.append(os_filter)

    filter_string = " and ".join(filter_parts)
    items = fetch_retail_prices(filter_string)

    # Fallback if no OS-specific meter found (e.g., for B-series)
    if not items and os_type.lower() == 'linux':
        logger.debug(f"No Linux-specific VM compute price found for {vm_size} in {location}. Retrying without OS filter.")
        filter_parts.pop() # Remove the OS filter
        filter_string = " and ".join(filter_parts)
        items = fetch_retail_prices(filter_string)

    if not items:
        logger.warning(f"No price items found for VM compute: {vm_size} ({os_type}) in {location}. Filter: {filter_string}")
        return 0.0

    # --- Find Best Match ---
    # Prefer meters ending in " Hour" or " Hours"
    best_match = find_best_match(
        items,
        location=location,
        resource_desc=f"{vm_size} ({os_type}) VM Compute",
        required_unit='1 Hour', # Compute is hourly
        meter_name_pattern=r'Compute Hour[s]?$', # Matches "Compute Hour" or "Compute Hours"
        sku_name_pattern=re.escape(vm_size), # Prefer exact SKU match
        # prefer_contains_meter = [os_type] if os_type.lower() == 'windows' else None,
        # avoid_contains_meter = ['Windows'] if os_type.lower() == 'linux' else None, # Handled by filter now
        strict_unit_match=True
    )

    # Softer match if strict one fails (e.g. Dv5 series have meter "DXXX Compute")
    if not best_match:
         logger.debug(f"Retrying VM compute price match for {vm_size} with broader meter pattern.")
         best_match = find_best_match(
             items,
             location=location,
             resource_desc=f"{vm_size} ({os_type}) VM Compute",
             required_unit='1 Hour',
             meter_name_pattern=r'Compute', # Just ensure it contains compute
             sku_name_pattern=re.escape(vm_size),
             strict_unit_match=True
         )

    if not best_match:
         logger.warning(f"Could not find best price match for VM compute: {vm_size} ({os_type}) in {location}.")
         return 0.0

    # --- Calculate Cost ---
    monthly_cost, unit_str = estimate_monthly_cost(best_match)

    if monthly_cost is None:
        logger.error(f"Failed to estimate monthly compute cost from matched price for VM {vm_size}.")
        return 0.0

    logger.info(f"Estimated VM compute cost for {vm_size} ({os_type}) in {location}: {monthly_cost:.2f} {unit_str}")
    return monthly_cost

def estimate_app_gateway_cost(sku_tier: str, sku_name: str, location: str, console: Console = _console) -> float:
    """Estimates the monthly cost of an Application Gateway instance using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating cost for App Gateway: tier={sku_tier}, sku={sku_name}, location={location}")

    # --- Build Filter ---
    # Tiers: Standard, Standard_v2, WAF, WAF_v2
    # SKU Names: Standard_Small, Standard_Medium, ..., WAF_Medium, ...
    filter_parts = [
        f"armRegionName eq '{location}'",
        f"priceType eq 'Consumption'",
        f"serviceName eq 'Application Gateway'",
        f"contains(skuName, '{sku_name}')" # Match specific SKU (e.g., Standard_Medium)
    ]
    # Add tier info to productName filter for better matching
    # Need to handle V2 tiers carefully in productName
    tier_keyword = sku_tier.replace("_", " ") # e.g., "Standard v2"
    filter_parts.append(f"contains(productName, '{tier_keyword}')")

    filter_string = " and ".join(filter_parts)
    items = fetch_retail_prices(filter_string)

    if not items:
        logger.warning(f"No price items found for App Gateway: {sku_tier} {sku_name} in {location}. Filter: {filter_string}")
        # Fallback: Try removing tier from product name filter?
        original_filter_parts = filter_parts.copy()
        filter_parts.pop()
        filter_string_fallback = " and ".join(filter_parts)
        logger.debug(f"Retrying App Gateway search with broader filter: {filter_string_fallback}")
        items = fetch_retail_prices(filter_string_fallback)
        if not items:
             logger.warning(f"Still no price items found for App Gateway {sku_name} in {location} after fallback.")
             return 0.0
        else:
             logger.info(f"Found {len(items)} items for App Gateway {sku_name} using fallback filter.")

    # --- Find Best Match ---
    # Look for meters like "Standard Medium Gateway Hours", "WAF Medium Capacity Unit Hour"
    # V2 SKUs are often priced per "Capacity Unit Hour"
    required_unit = '1 Hour'
    meter_name_pattern = None
    prefer_meter = [sku_name] # Prefer meter containing the SKU name

    if "_v2" in sku_tier.lower():
        meter_name_pattern = r'Capacity Unit Hour' # V2 uses capacity units
        prefer_meter.append('Capacity Unit')
    else:
        meter_name_pattern = r'Gateway Hour[s]?$' # V1 uses gateway hours
        prefer_meter.append('Gateway Hour')

    best_match = find_best_match(
        items,
        location=location,
        resource_desc=f"App Gateway {sku_tier} {sku_name}",
        required_unit=required_unit,
        meter_name_pattern=meter_name_pattern,
        sku_name_pattern=re.escape(sku_name),
        product_name_pattern=tier_keyword, # Match tier in product name
        prefer_contains_meter=prefer_meter,
        strict_unit_match=True
    )

    if not best_match:
         # Broader retry if specific pattern fails
         logger.debug(f"Retrying App Gateway price match for {sku_name} with broader meter pattern.")
         best_match = find_best_match(
            items,
            location=location,
            resource_desc=f"App Gateway {sku_tier} {sku_name}",
            required_unit=required_unit,
            meter_name_pattern=r'Hour', # Just find any hourly rate
            sku_name_pattern=re.escape(sku_name),
            prefer_contains_meter=[sku_name], # Still prefer SKU name
            strict_unit_match=True
         )

    if not best_match:
         logger.warning(f"Could not find best price match for App Gateway: {sku_tier} {sku_name} in {location}.")
         return 0.0

    # --- Calculate Cost ---
    # Note: V2 costs also include fixed price + capacity unit price. We estimate based on capacity unit price only.
    # This assumes 1 capacity unit for simplicity, actual cost depends on usage.
    monthly_cost, unit_str = estimate_monthly_cost(best_match)

    if monthly_cost is None:
        logger.error(f"Failed to estimate monthly cost from matched price for App Gateway {sku_tier} {sku_name}.")
        return 0.0

    logger.info(f"Estimated App Gateway cost for {sku_tier} {sku_name} in {location}: {monthly_cost:.2f} {unit_str} (Note: V2 cost is per capacity unit; Fixed cost not included)")
    return monthly_cost

def get_cost_data(credential, subscription_id, console: Console = _console) -> Tuple[Dict, float, str]:
    """Fetches actual cost data using the Cost Management API."""
    logger = logging.getLogger()
    logger.info("Fetching cost data using Cost Management API...")
    console.print("\n[bold blue]--- Fetching Cost Data ---[/]") # Added console print

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
        # TODO: Determine current billing period start/end dates dynamically if needed
        # For simplicity, using a fixed time period (e.g., last 30 days or current month) might suffice
        # Example: Use current month to date
        first_day_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # Cost management API might be slightly delayed, consider looking back slightly more?
        # Or use a fixed lookback like 30 days
        start_date = first_day_of_month # Or now - timedelta(days=30)
        end_date = now

        logger.info(f"Querying costs from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

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

        query_result = cost_client.query.usage(scope=scope, parameters=query_definition)

        if query_result and query_result.rows:
            # Assuming columns are [Cost, Currency, ServiceName, ResourceGroup, UsageDate] - Check API response structure
            # Find indices - safer than hardcoding
            try:
                cost_idx = query_result.columns.index(next(c for c in query_result.columns if c.name.lower() == 'cost'))
                currency_idx = query_result.columns.index(next(c for c in query_result.columns if c.name.lower() == 'currency'))
                service_name_idx = query_result.columns.index(next(c for c in query_result.columns if c.name.lower() == 'servicename'))
            except (StopIteration, ValueError) as e:
                 logger.error(f"Could not find expected columns (Cost, Currency, ServiceName) in Cost Management API response: {query_result.columns}. Error: {e}")
                 return {}, 0.0, currency # Return empty if structure is wrong

            for row in query_result.rows:
                cost = float(row[cost_idx])
                currency = row[currency_idx]
                service_name = row[service_name_idx]

                if service_name is None: service_name = "Uncategorized" # Handle null service names

                costs_by_type[service_name] = costs_by_type.get(service_name, 0.0) + cost
                total_cost += cost
                currency = currency # Assume currency is consistent

            logger.info(f"Successfully processed cost data. Total Cost: {total_cost:.2f} {currency}. Breakdown by service: {costs_by_type}")
            
        else:
             logger.warning("Cost Management query returned no rows or empty result.")

    except Exception as e:
        logger.error(f"Error fetching cost data from Cost Management API: {e}", exc_info=True)
        # Optionally: Provide a more user-friendly message via console
        console.print(f"  [yellow]Warning:[/yellow] Failed to retrieve detailed cost breakdown from Azure Cost Management API: {e}")

    return costs_by_type, total_cost, currency 