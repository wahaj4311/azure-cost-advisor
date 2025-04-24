import logging
import requests
import json
import re # Import regex for flexible matching
from typing import List, Dict, Any, Optional, Tuple # Add typing
from rich.console import Console # Keep for potential future use or passthrough
from azure.mgmt.costmanagement import CostManagementClient # Added import
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import math # For ceiling function

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

# --- Location Normalization Helper ---
# Simple cache for normalized locations
_location_normalization_cache = {}

def _normalize_location(location: str) -> str:
    """Converts location strings (e.g., 'westus3') to the canonical ARM format (e.g., 'West US 3')."""
    global _location_normalization_cache
    if not location:
        return ''
    
    # Check cache first
    if location in _location_normalization_cache:
        return _location_normalization_cache[location]

    # Basic normalization: lowercase, remove spaces/hyphens/underscores
    normalized_key = location.lower().replace(' ', '').replace('-', '').replace('_', '')

    # Known mappings (add more as needed based on API responses or common variants)
    # Prioritize common Azure locations
    mapping = {
        'eastus': 'East US',
        'eastus2': 'East US 2',
        'southcentralus': 'South Central US',
        'westus2': 'West US 2',
        'westus3': 'West US 3',
        'australiaeast': 'Australia East',
        'southeastasia': 'Southeast Asia',
        'northeurope': 'North Europe',
        'swedencentral': 'Sweden Central',
        'uksouth': 'UK South',
        'westeurope': 'West Europe',
        'centralus': 'Central US',
        'southafricanorth': 'South Africa North',
        'centralindia': 'Central India',
        'eastasia': 'East Asia',
        'japaneast': 'Japan East',
        'koreacentral': 'Korea Central',
        'canadacentral': 'Canada Central',
        'francecentral': 'France Central',
        'germanywestcentral': 'Germany West Central',
        'norwayeast': 'Norway East',
        'brazilsouth': 'Brazil South',
        'westus': 'West US',
        # Add more common locations...
    }

    normalized_location = mapping.get(normalized_key)

    if not normalized_location:
        # Fallback: Capitalize words if no direct map found
        # This handles simple cases like 'west us 3' -> 'West US 3'
        # but might not be perfect for all Azure locations.
        normalized_location = ' '.join(word.capitalize() for word in location.split())
        logger = logging.getLogger()
        logger.warning(f"Location '{location}' not in known normalization map. Attempting capitalization: '{normalized_location}'. Add to mapping if needed.")

    # Cache the result
    _location_normalization_cache[location] = normalized_location
    return normalized_location

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
    strict_unit_match: bool = True, # If True, unit must match exactly (case-insensitive)
    exact_sku_name: Optional[str] = None, # Prefer exact SKU match if provided
    exact_meter_name: Optional[str] = None # Prefer exact Meter match if provided
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
        exact_sku_name: Prefer an exact match on skuName (case-insensitive).
        exact_meter_name: Prefer an exact match on meterName (case-insensitive).

    Returns:
        The best matching price item dictionary or None.
    """
    logger = logging.getLogger()
    if not items:
        logger.debug(f"No items provided to find_best_match for {resource_desc} in {location}.")
        return None

    logger.debug(f"Finding best match for {resource_desc} in {location} from {len(items)} items. Criteria: priceType='{required_price_type}', unit='{required_unit}', meterPat='{meter_name_pattern}', skuPat='{sku_name_pattern}', prodPat='{product_name_pattern}', prefer='{prefer_contains_meter}', avoid='{avoid_contains_meter}', exactSKU='{exact_sku_name}', exactMeter='{exact_meter_name}'")

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

        # Exact Matches (Strong positive signals)
        if exact_sku_name and sku_name.lower() == exact_sku_name.lower():
            score += 500
        if exact_meter_name and meter_name.lower() == exact_meter_name.lower():
            score += 500

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

    # Sort candidates by score (highest first), then by price (lowest first) as tie-breaker
    candidates.sort(key=lambda x: (x['score'], -x['item'].get('retailPrice', float('inf'))), reverse=True)

    best_candidate = candidates[0]
    logger.debug(f"Selected best match for {resource_desc} in {location} (Score: {best_candidate['score']}): {best_candidate['item']}")
    if len(candidates) > 1:
        # Log top few alternatives with scores
        alt_info = [(c['score'], c['item'].get('meterName'), c['item'].get('retailPrice')) for c in candidates[1:min(6, len(candidates))]]
        logger.debug(f"Other potential candidates (score, meter, price): {alt_info}")

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

    # Normalize location for API query
    normalized_location = _normalize_location(location)
    if not normalized_location:
        logger.error(f"Could not normalize location '{location}' for disk cost estimation.")
        return 0.0
    
    logger.info(f"Estimating cost for Disk: sku={sku_name}, size={size_gb}GB, location={normalized_location} (Original: {location})")
    price = 0.0

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
        f"armRegionName eq '{normalized_location}'",
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
        logger.warning(f"No price items found for disk: {sku_name} ({disk_tier}) in {normalized_location}. Filter: {filter_string}")
        return 0.0

    # --- Find Best Match ---
    # HDD needs specific unit handling
    best_match = find_best_match(
        items,
        normalized_location,
        resource_desc=f"{sku_name} ({disk_tier}) Disk",
        required_unit = '1 GB/Month' if is_hdd else '1/Month', # Match "1/Month" or "1 GB/Month"
        meter_name_pattern=meter_name_pattern,
        product_name_pattern=product_name_pattern,
        strict_unit_match=True # Units should be exact for disks
    )

    if not best_match:
        logger.warning(f"Could not find best price match for disk: {sku_name} ({disk_tier}) in {normalized_location}.")
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
        logger.info(f"Estimated Disk cost for {sku_name} ({disk_tier}) in {normalized_location}: {monthly_cost:.2f} {unit_str}")
        return monthly_cost


def estimate_public_ip_cost(sku_name: str, location: str, console: Console = _console) -> float:
    """Estimates the monthly cost of an Azure Public IP address using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating cost for Public IP: sku={sku_name}, location={location}")

    # Normalize location for API query
    normalized_location = _normalize_location(location)
    if not normalized_location:
        logger.error(f"Could not normalize location '{location}' for Public IP cost estimation.")
        return 0.0

    logger.info(f"Estimating cost for Public IP: sku={sku_name}, location={normalized_location} (Original: {location})")
    price = 0.0
    sku_lower = sku_name.lower()
    is_basic = 'basic' in sku_lower
    is_standard = 'standard' in sku_lower
    is_global = 'global' in sku_lower # For Cross-region LB

    # Determine resource description and patterns
    if is_basic:
        resource_desc = "Basic Public IP Address"
        meter_pattern = "Basic IP Address Hour"
        sku_pattern = "Basic"
    elif is_standard and is_global:
        resource_desc = "Global Standard Public IP Address"
        meter_pattern = "Global Standard IP Address Hour"
        sku_pattern = "Global"
    elif is_standard:
        resource_desc = "Standard Public IP Address"
        meter_pattern = "Standard IP Address Hour"
        sku_pattern = "Standard"
    else:
        logger.warning(f"Unknown Public IP SKU: {sku_name}. Cannot estimate cost.")
        return 0.0

    # Build filter string
    filter_parts = [
        f"armRegionName eq '{normalized_location}'",
        f"priceType eq 'Consumption'",
        f"serviceName eq 'Networking'", # IPs are under Networking
        f"contains(meterName, 'IP Address')",
        f"contains(meterName, 'Hour')", # Usually priced per hour
    ]
    if is_basic:
         filter_parts.append("contains(meterName, 'Basic')")
    elif is_standard:
         filter_parts.append("contains(meterName, 'Standard')")
         if is_global:
             filter_parts.append("contains(meterName, 'Global')")

    filter_string = " and ".join(filter_parts)

    # Fetch and match prices
    items = fetch_retail_prices(filter_string)
    if items:
        best_match = find_best_match(
            items,
            normalized_location,
            resource_desc,
            required_unit="Hour", # Expect hourly price
            meter_name_pattern=meter_pattern,
            sku_name_pattern=sku_pattern,
            strict_unit_match=False # Allow units like '1 Hour', '10 Hour'
        )
        
        if best_match:
            price, _ = estimate_monthly_cost(best_match) # Converts hourly to monthly
            if price is not None:
                logger.info(f"Estimated monthly cost for {resource_desc}: {price:.2f}")
                return price
            else:
                 logger.warning(f"Could not estimate monthly cost from best match for {resource_desc}.")
        else:
             logger.warning(f"No matching price item found for {resource_desc} in {normalized_location}. Filter: {filter_string}")
    else:
        logger.warning(f"No price items returned for Public IP filter: {filter_string}")

    return 0.0

def estimate_snapshot_cost(size_gb: int, location: str, sku_name: Optional[str], console: Console = _console) -> float:
    """Estimates the monthly cost of a Managed Disk Snapshot using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating cost for Snapshot: size={size_gb}GB, location={location}, sku={sku_name}")

    # Normalize location for API query
    normalized_location = _normalize_location(location)
    if not normalized_location:
        logger.error(f"Could not normalize location '{location}' for Snapshot cost estimation.")
        return 0.0

    # Determine snapshot type based on SKU
    sku_lower = sku_name.lower() if sku_name else 'standard_lrs' # Default to Standard LRS
    storage_type = "Standard HDD" # Default
    sku_filter_part = "contains(meterName, 'Standard Snapshot')" # Default filter
    if 'premium' in sku_lower:
        storage_type = "Premium SSD"
        sku_filter_part = "contains(meterName, 'Premium Snapshot')"
    elif 'standardssd' in sku_lower:
        storage_type = "Standard SSD"
        sku_filter_part = "contains(meterName, 'Standard SSD Snapshot')"
    elif 'standard' in sku_lower: # Handles Standard_LRS and Standard_ZRS (ZRS more expensive)
        storage_type = "Standard HDD"
        if 'zrs' in sku_lower:
            storage_type = "Standard HDD ZRS"
            sku_filter_part = "contains(meterName, 'Standard ZRS Snapshot')"
        else:
            storage_type = "Standard HDD LRS"
            sku_filter_part = "contains(meterName, 'Standard Snapshot')" # LRS is usually the default
    else:
        logger.warning(f"Unknown snapshot SKU type: {sku_name}. Assuming Standard LRS.")
        storage_type = "Standard HDD LRS"
        sku_filter_part = "contains(meterName, 'Standard Snapshot')"

    logger.info(f"Estimating cost for {storage_type} Snapshot: size={size_gb}GB, location={normalized_location} (Original: {location})")
    price = 0.0
    resource_desc = f"{storage_type} Snapshot ({size_gb} GB)"
    required_unit = "GB/Month"

    # Build filter string
    filter_parts = [
        f"armRegionName eq '{normalized_location}'",
        f"priceType eq 'Consumption'",
        f"(serviceName eq 'Storage' or serviceName eq 'Managed Disks')", # Snapshots usually under Storage
        sku_filter_part, # Filter by snapshot type
        f"contains(meterName, 'GB')" # Ensure it's a per-GB price
    ]
    filter_string = " and ".join(filter_parts)

    # Fetch and match prices
    items = fetch_retail_prices(filter_string)
    if items:
        # Find the best match (usually only one price per GB/Month for snapshots)
        best_match = find_best_match(
            items,
            normalized_location,
            resource_desc,
            required_unit=required_unit,
            strict_unit_match=False # Allow '1 GB/Month' etc.
        )

        if best_match:
            price_per_gb, unit = estimate_monthly_cost(best_match)
            if price_per_gb is not None and unit and 'gb/month' in unit.lower():
                monthly_cost = price_per_gb * size_gb
                logger.info(f"Estimated monthly cost for {resource_desc}: {price_per_gb:.4f} {unit} * {size_gb} GB = {monthly_cost:.2f}/Month")
                return monthly_cost
            else:
                 logger.warning(f"Could not estimate monthly cost per GB from best match for {resource_desc}. Price: {best_match.get('retailPrice')}, Unit: {best_match.get('unitOfMeasure')}")
        else:
             logger.warning(f"No matching price item found for {resource_desc} in {normalized_location}. Filter: {filter_string}")
    else:
        logger.warning(f"No price items returned for Snapshot filter: {filter_string}")

    return 0.0

def estimate_app_service_plan_cost(sku_tier: str, sku_name: str, location: str, console: Console = _console) -> float:
    """Estimates the monthly cost of an App Service Plan using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating cost for App Service Plan: tier={sku_tier}, sku={sku_name}, location={location}")

    # Normalize location for API query
    normalized_location = _normalize_location(location)
    if not normalized_location:
        logger.error(f"Could not normalize location '{location}' for ASP cost estimation.")
        return 0.0

    tier_name = sku_tier.capitalize()
    # Extract size from SKU (e.g., B1 -> 1, P2v2 -> 2, I3 -> 3)
    size_match = re.search(r'(\d+)', sku_name)
    size_indicator = sku_name # Default to full SKU name if no number found
    if size_match:
         size_indicator = size_match.group(1)

    resource_desc = f"App Service Plan: {tier_name} {sku_name}"
    logger.info(f"Estimating cost for {resource_desc} in {normalized_location} (Original: {location})")
    required_unit = "Hour" # ASPs usually priced per hour

    # Build filter string - try to match tier and size indicator
    filter_parts = [
        f"armRegionName eq '{normalized_location}'",
        f"priceType eq 'Consumption'",
        f"serviceName eq 'App Service'",
        # Match the tier in productName - be flexible with naming (e.g., 'Basic Plan', 'Basic App Service')
        f"(contains(productName, '{tier_name} Plan') or contains(productName, '{tier_name} App Service'))", 
        # Use contains for skuName initially, then refine with find_best_match
        f"(skuName eq '{sku_name}' or contains(meterName, '{sku_name}') or contains(meterName, ' {size_indicator} '))" # Add space around size indicator in meter
    ]
    filter_string = " and ".join(filter_parts)

    items = fetch_retail_prices(filter_string)

    # Initial match attempt
    best_match = None
    if items:
        # Find the best match using more specific patterns
        best_match = find_best_match(
            items,
            normalized_location,
            resource_desc,
            required_unit=required_unit,
            strict_unit_match=False,
            meter_name_pattern=f"{sku_name}|{size_indicator}", # Look for SKU or size in meter
            product_name_pattern=tier_name, # Ensure tier matches product name
            exact_sku_name=sku_name, # Strongly prefer exact SKU match
            prefer_contains_meter=[sku_name, size_indicator]
        )

    # Fallback: If no match, broaden the product name search slightly
    if not best_match:
         logger.warning(f"Initial ASP price search failed for {resource_desc} in {normalized_location}. Broadening product name filter.")
         fallback_filter_parts = [
             f"armRegionName eq '{normalized_location}'",
             f"priceType eq 'Consumption'",
             f"serviceName eq 'App Service'",
             # Broader product name match (just the tier)
             f"contains(productName, '{tier_name}')", 
             f"(skuName eq '{sku_name}' or contains(meterName, '{sku_name}') or contains(meterName, ' {size_indicator} '))"
         ]
         filter_string_fallback = " and ".join(fallback_filter_parts)
         fallback_items = fetch_retail_prices(filter_string_fallback)
         if fallback_items:
              best_match = find_best_match(
                  fallback_items, # Use items from fallback query
                  normalized_location,
                  resource_desc + " (Fallback Filter)",
                  required_unit=required_unit,
                  strict_unit_match=False,
                  meter_name_pattern=f"{sku_name}|{size_indicator}",
                  product_name_pattern=tier_name,
                  exact_sku_name=sku_name,
                  prefer_contains_meter=[sku_name, size_indicator]
              )

    if best_match:
        price, _ = estimate_monthly_cost(best_match)
        if price is not None:
            logger.info(f"Estimated monthly cost for {resource_desc}: {price:.2f}")
            return price
        else:
             logger.warning(f"Could not estimate monthly cost from best match for {resource_desc}.")
    elif items is None: # If initial fetch failed
         logger.warning(f"Price API query failed for ASP filter: {filter_string}")
    else: # If fetch succeeded but no match found even after fallback
         logger.warning(f"No matching price item found for {resource_desc} in {normalized_location}. Initial Filter: {filter_string}, Fallback Filter: {filter_string_fallback if 'filter_string_fallback' in locals() else 'N/A'}")

    return 0.0

def estimate_sql_database_cost(sku_tier: Optional[str], sku_name: Optional[str], family: Optional[str], capacity: Optional[int], location: str, console: Console = _console) -> float:
    """Estimates the monthly cost of an Azure SQL Database (DTU or vCore) using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating cost for SQL Database: tier={sku_tier}, sku={sku_name}, family={family}, capacity={capacity}, location={location}")

    # Normalize location for API query
    normalized_location = _normalize_location(location)
    if not normalized_location:
        logger.error(f"Could not normalize location '{location}' for SQL DB cost estimation.")
        return 0.0

    # Normalize inputs
    tier_lower = sku_tier.lower() if sku_tier else ""
    sku_lower = sku_name.lower() if sku_name else ""
    family_lower = family.lower() if family else ""
    
    is_dtu = any(t in tier_lower for t in ['basic', 'standard', 'premium'])
    is_vcore = any(t in tier_lower for t in ['generalpurpose', 'businesscritical', 'hyperscale']) or 'gen' in family_lower

    resource_desc = f"SQL DB: Tier={sku_tier}, SKU={sku_name}, Family={family}, Capacity={capacity}"
    logger.info(f"Estimating cost for {resource_desc} in {normalized_location} (Original: {location})")
    total_monthly_cost = 0.0

    # --- Build Base Filter ---
    base_filter_parts = [
        f"armRegionName eq '{normalized_location}'",
        f"priceType eq 'Consumption'",
        f"serviceName eq 'SQL Database'"
    ]

    if is_dtu:
        # DTU Model Pricing (often includes compute + storage)
        resource_desc = f"SQL DB (DTU): {sku_tier} {sku_name}"
        required_unit = "Month" # Basic/Standard often priced per month including some storage
        if 'premium' in tier_lower:
             required_unit = "DTU/Month" # Premium often priced per DTU

        dtu_filter_parts = base_filter_parts + [
            f"(contains(meterName, '{sku_tier}') or contains(skuName, '{sku_name}'))",
            f"(contains(meterName, 'DTU') or contains(meterName, '{sku_tier}'))" # Match tier or DTU
        ]
        filter_string = " and ".join(dtu_filter_parts)
        items = fetch_retail_prices(filter_string)

        if items:
            best_match = find_best_match(
                items,
                normalized_location,
                resource_desc,
                required_unit=required_unit,
                strict_unit_match=False,
                meter_name_pattern=f"{sku_tier}|{sku_name}|DTU",
                product_name_pattern=sku_tier, # Match tier in product name
                prefer_contains_meter=[sku_name, sku_tier, 'DTU']
            )
            if best_match:
                price, unit = estimate_monthly_cost(best_match)
                if price is not None:
                    if unit and 'dtu/month' in unit.lower() and capacity:
                        total_monthly_cost = price * capacity
                        logger.info(f"Estimated DTU cost for {resource_desc}: {price:.4f} {unit} * {capacity} DTUs = {total_monthly_cost:.2f}/Month")
                    else:
                         # Assume price is total monthly cost (e.g., Basic/Standard includes DTUs)
                         total_monthly_cost = price
                         logger.info(f"Estimated monthly cost for {resource_desc}: {total_monthly_cost:.2f} {unit}")
                else:
                    logger.warning(f"Could not estimate monthly cost from best match for {resource_desc}.")
            else:
                logger.warning(f"No matching price item found for {resource_desc} in {normalized_location}. Filter: {filter_string}")
        else:
            logger.warning(f"No price items returned for SQL DTU filter: {filter_string}")

    elif is_vcore:
        # vCore Model Pricing (Compute + Storage separate)
        # 1. Estimate Compute Cost
        compute_desc = f"SQL DB (vCore Compute): {sku_tier} {family} {capacity} vCore"
        vcore_filter_parts = base_filter_parts + [
             f"contains(meterName, 'vCore')", # Look for vCore pricing
             f"contains(meterName, 'Hour')", # Usually per vCore hour
             f"contains(productName, '{sku_tier}')" # e.g., General Purpose - Provisioned
        ]
        if family: # Add family if available (e.g., Gen5)
            vcore_filter_parts.append(f"(contains(skuName, '{family}') or contains(meterName, '{family}'))")
        
        filter_string_compute = " and ".join(vcore_filter_parts)
        compute_items = fetch_retail_prices(filter_string_compute)
        compute_cost_per_month = 0.0

        if compute_items:
            best_match_compute = find_best_match(
                items=compute_items,
                location=normalized_location,
                resource_desc=compute_desc,
                required_unit="Hour",
                strict_unit_match=False,
                meter_name_pattern=f"vCore|{family if family else ''}",
                sku_name_pattern=family if family else None,
                product_name_pattern=sku_tier,
                prefer_contains_meter=['vCore', family if family else 'Gen5'] # Prefer Gen5 if family unknown
            )
            if best_match_compute:
                price, _ = estimate_monthly_cost(best_match_compute) # Converts hourly to monthly
                if price is not None and capacity:
                    compute_cost_per_month = price * capacity # Cost per vCore-hour * hours/month * num vCores
                    logger.info(f"Estimated vCore compute cost for {compute_desc}: {compute_cost_per_month:.2f}/Month")
                elif price is not None: # Handle Serverless where capacity might not apply directly
                    compute_cost_per_month = price
                    logger.info(f"Estimated vCore compute cost for {compute_desc} (assuming serverless or single unit): {compute_cost_per_month:.2f}/Month")
                else:
                     logger.warning(f"Could not estimate monthly compute cost from best match for {compute_desc}.")
            else:
                 logger.warning(f"No matching compute price item found for {compute_desc} in {normalized_location}. Filter: {filter_string_compute}")
        else:
            logger.warning(f"No price items returned for SQL vCore compute filter: {filter_string_compute}")
        
        total_monthly_cost += compute_cost_per_month

        # 2. Estimate Storage Cost (Simplification: Assume general LRS storage for now)
        #    A more accurate approach would need the actual storage size used.
        #    Let's skip explicit storage cost for now to avoid overcomplication without actual size.
        # storage_desc = f"SQL DB Storage ({sku_tier})"
        # storage_filter_parts = base_filter_parts + [
        #      f"contains(meterName, 'Data Stored')",
        #      f"contains(meterName, 'GB')"
        # ]
        # filter_string_storage = " and ".join(storage_filter_parts)
        # storage_items = fetch_retail_prices(filter_string_storage)
        # storage_cost_per_gb_month = 0.0
        # ... (find best match for storage, estimate cost per GB, multiply by assumed/actual size) ...
        # total_monthly_cost += storage_cost_per_gb_month * storage_size_gb

    else:
        logger.warning(f"Could not determine model (DTU/vCore) for SQL DB: Tier='{sku_tier}', SKU='{sku_name}', Family='{family}'. Cannot estimate cost.")
        return 0.0

    return total_monthly_cost

def estimate_vm_cost(vm_size: str, location: str, os_type: str = 'Linux', console: Console = _console) -> float:
    """Estimates the monthly compute cost of an Azure Virtual Machine using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating compute cost for VM: size={vm_size}, location={location}, os={os_type}")

    # Normalize location for API query
    normalized_location = _normalize_location(location)
    if not normalized_location:
        logger.error(f"Could not normalize location '{location}' for VM cost estimation.")
        return 0.0

    os_lower = os_type.lower() if os_type else 'linux'
    is_windows = 'windows' in os_lower

    # Extract VM series/size info (e.g., Standard_B2s -> B2s)
    size_parts = vm_size.split('_')
    size_indicator = size_parts[-1] if len(size_parts) > 1 else vm_size
    series_indicator = size_parts[1].split('s')[0] if len(size_parts) > 1 else vm_size # e.g., Standard_B2s_v3 -> B

    resource_desc = f"VM compute: {vm_size} ({os_type})"
    logger.info(f"Estimating {resource_desc} in {normalized_location} (Original: {location})")
    required_unit = "Hour"

    # Build filter string - broader initially
    filter_parts = [
        f"armRegionName eq '{normalized_location}'",
        f"priceType eq 'Consumption'",
        f"serviceName eq 'Virtual Machines'",
        # Match series (e.g., B series) or specific size indicator (e.g., B2s)
        # Use SERIES indicator for productName match
        f"(contains(productName, '{series_indicator} Series') or contains(skuName, '{size_indicator}') or contains(meterName, '{size_indicator}'))",
        f"contains(meterName, 'Compute')" # Ensure it's compute cost
    ]
    
    # Add OS-specific filter parts
    os_meter_filter = "not contains(meterName, 'Windows')" # Default to Linux
    os_product_filter = "not contains(productName, 'Windows')"
    if is_windows:
        os_meter_filter = "contains(meterName, 'Windows')"
        os_product_filter = "contains(productName, 'Windows')"
    
    filter_parts.append(os_meter_filter)
    # Add OS filter to product name too? Might be too restrictive
    # filter_parts.append(os_product_filter) 
    
    filter_string = " and ".join(filter_parts)

    items = fetch_retail_prices(filter_string)
    if items:
        # Use find_best_match to pinpoint the correct VM size and OS
        prefer_keywords = [size_indicator, 'Compute']
        avoid_keywords = ['Spot', 'Low Priority'] # Avoid spot instance pricing
        meter_pattern = f"{size_indicator}.*Compute"
        if is_windows:
            prefer_keywords.append('Windows')
            meter_pattern = f"{size_indicator}.*Windows.*Compute"
        else:
            avoid_keywords.append('Windows')

        best_match = find_best_match(
            items,
            normalized_location,
            resource_desc,
            required_unit=required_unit,
            strict_unit_match=False,
            meter_name_pattern=meter_pattern,
            exact_sku_name=vm_size, # Prefer exact SKU like Standard_B2s
            prefer_contains_meter=prefer_keywords,
            avoid_contains_meter=avoid_keywords
        )

        if best_match:
            price, _ = estimate_monthly_cost(best_match)
            if price is not None:
                logger.info(f"Estimated monthly compute cost for {resource_desc}: {price:.2f}")
                return price
            else:
                 logger.warning(f"Could not estimate monthly cost from best match for {resource_desc}.")
        else:
            # Try a slightly broader search if exact match failed?
            logger.warning(f"No matching price item found for {resource_desc} in {normalized_location}. Filter: {filter_string}")
            # Could try removing os_meter_filter and checking productName instead?
    else:
        logger.warning(f"No price items returned for VM filter: {filter_string}")

    return 0.0

def estimate_app_gateway_cost(sku_tier: str, sku_name: str, location: str, console: Console = _console) -> float:
    """Estimates the monthly cost of an Azure Application Gateway instance using the Retail Prices API."""
    logger = logging.getLogger()
    logger.info(f"Estimating cost for App Gateway: tier={sku_tier}, sku={sku_name}, location={location}")

    # Normalize location for API query
    normalized_location = _normalize_location(location)
    if not normalized_location:
        logger.error(f"Could not normalize location '{location}' for App Gateway cost estimation.")
        return 0.0

    # Normalize tier/sku for searching
    tier_lower = sku_tier.lower()
    sku_lower = sku_name.lower()
    size_indicator = sku_name # Default
    if 'small' in sku_lower:
        size_indicator = 'Small'
    elif 'medium' in sku_lower:
        size_indicator = 'Medium'
    elif 'large' in sku_lower:
        size_indicator = 'Large'
    
    is_v1 = 'v1' in sku_lower or tier_lower == 'standard' or tier_lower == 'waf'
    is_v2 = 'v2' in sku_lower or tier_lower == 'standard_v2' or tier_lower == 'waf_v2'

    resource_desc = f"App Gateway: {sku_tier} {sku_name}"
    logger.info(f"Estimating cost for {resource_desc} in {normalized_location} (Original: {location})")
    
    total_monthly_cost = 0.0

    # Base filter
    base_filter_parts = [
        f"armRegionName eq '{normalized_location}'",
        f"priceType eq 'Consumption'",
        f"serviceName eq 'Application Gateway'"
    ]

    # 1. Gateway Instance Hours (Primary cost component)
    instance_filter_parts = list(base_filter_parts)
    instance_meter_pattern = None
    instance_sku_pattern = sku_name
    instance_unit = "Hour"
    prefer_instance_meter = [size_indicator] if size_indicator != sku_name else [sku_name]
    
    if is_v2:
        instance_filter_parts.append("contains(meterName, 'Gateway Hour')") # v2 meter
        instance_filter_parts.append("contains(productName, 'v2')")
        instance_meter_pattern = "Gateway Hour"
        resource_desc_inst = f"{resource_desc} (v2 Instance Hours)"
    elif is_v1:
        instance_filter_parts.append(f"contains(meterName, '{size_indicator} Gateway')") # v1 meter pattern
        instance_meter_pattern = f"{size_indicator} Gateway"
        resource_desc_inst = f"{resource_desc} ({size_indicator} v1 Instance Hours)"
    else: # Unknown version
         logger.warning(f"Cannot determine App Gateway version from Tier='{sku_tier}', SKU='{sku_name}'. Skipping instance cost.")
         instance_filter_parts = None
         resource_desc_inst = resource_desc

    if instance_filter_parts:
        filter_string_inst = " and ".join(instance_filter_parts)
        instance_items = fetch_retail_prices(filter_string_inst)
        if instance_items:
            best_match_inst = find_best_match(
                items=instance_items,
                location=normalized_location,
                resource_desc=resource_desc_inst,
                required_unit=instance_unit,
                strict_unit_match=False,
                meter_name_pattern=instance_meter_pattern,
                sku_name_pattern=instance_sku_pattern,
                prefer_contains_meter=prefer_instance_meter
            )
            if best_match_inst:
                price, _ = estimate_monthly_cost(best_match_inst)
                if price is not None:
                    total_monthly_cost += price
                    logger.info(f"Estimated monthly instance cost for {resource_desc_inst}: {price:.2f}")
                else:
                    logger.warning(f"Could not estimate monthly instance cost from best match for {resource_desc_inst}.")
            else:
                logger.warning(f"No matching instance price item found for {resource_desc_inst} in {normalized_location}. Filter: {filter_string_inst}")
        else:
            logger.warning(f"No price items returned for App Gateway instance filter: {filter_string_inst}")

    # 2. Capacity Units (v2 only)
    if is_v2:
        cu_desc = f"{resource_desc} (v2 Capacity Unit Hours)"
        cu_filter_parts = base_filter_parts + [
            "contains(meterName, 'Capacity Unit Hour')",
            "contains(productName, 'v2')"
        ]
        filter_string_cu = " and ".join(cu_filter_parts)
        cu_items = fetch_retail_prices(filter_string_cu)
        if cu_items:
            best_match_cu = find_best_match(
                items=cu_items,
                location=normalized_location,
                resource_desc=cu_desc,
                required_unit="Hour",
                strict_unit_match=False,
                meter_name_pattern="Capacity Unit Hour"
            )
            if best_match_cu:
                price_cu, _ = estimate_monthly_cost(best_match_cu)
                if price_cu is not None:
                    # Assume minimum 1 CU? Cost depends on actual usage/scaling.
                    # For idle cost, estimate cost of 1 CU.
                    total_monthly_cost += price_cu
                    logger.info(f"Estimated monthly cost for 1 Capacity Unit hour for {cu_desc}: {price_cu:.2f}")
                    logger.warning("App Gateway v2 cost depends on actual Capacity Units used. Estimating cost for minimum (1 CU). Actual savings may vary.")
                else:
                     logger.warning(f"Could not estimate monthly CU cost from best match for {cu_desc}.")
            else:
                logger.warning(f"No matching CU price item found for {cu_desc} in {normalized_location}. Filter: {filter_string_cu}")
        else:
            logger.warning(f"No price items returned for App Gateway CU filter: {filter_string_cu}")

    # 3. Data Processed (Both v1 and v2, but highly variable)
    # Skipping data processed cost estimation as it depends on traffic.
    # logger.info(f"Data processed cost for App Gateway is variable and not included in this estimate.")

    logger.info(f"Total estimated base monthly cost for {resource_desc}: {total_monthly_cost:.2f}")
    return total_monthly_cost

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