import logging
import requests
import json
import re # Import regex for flexible matching
from typing import List, Dict, Any, Optional, Tuple, Set, TYPE_CHECKING
from rich.console import Console # Keep for potential future use or passthrough
from azure.mgmt.costmanagement import CostManagementClient # Added import
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import math # For ceiling function
import urllib.parse

# Import constants from config module
# Assuming config.py is in the same directory or PYTHONPATH is set correctly
from .config import (
    RETAIL_PRICES_API_ENDPOINT, 
    HOURS_PER_MONTH,
    # DISK_SIZE_TO_TIER <<< Removed from import
)

# Type hint for logger to avoid circular import if utils imports pricing
if TYPE_CHECKING:
    from logging import Logger

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

def _normalize_location(location: str, logger: Optional['Logger'] = None) -> str:
    """Converts location strings (e.g., 'westus3') to the canonical ARM format (e.g., 'West US 3')."""
    if not logger: logger = logging.getLogger() # Fallback if not passed
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
        logger.warning(f"Location '{location}' not in known normalization map. Attempting capitalization: '{normalized_location}'. Add to mapping if needed.")

    # Cache the result
    _location_normalization_cache[location] = normalized_location
    return normalized_location

# --- Pricing Cache --- 
# Cache for API results { filter_string: list_of_price_items }
price_cache: Dict[str, Optional[List[Dict[str, Any]]]] = {}

# A cache for failed filters that returned 400 errors so we don't repeat them
failed_filters: Set[str] = set()

# Added cache for failed filters and direct cache hits
_PRICE_CACHE = {}  # Cache for price queries {filter_string: api_response}
_FAILED_FILTERS = set()  # Cache for filters that have returned 400 errors

def fetch_retail_prices(filter_string: str, skip_token: str = None, api_version: str = '2023-01-01-preview', logger: Optional['Logger'] = None) -> Dict[str, Any]:
    """
    Fetches prices from the Azure Retail Prices API.

    Args:
        filter_string: OData filter expression for filtering results.
        skip_token: Optional token for pagination.
        api_version: API version to use.

    Returns:
        Dictionary containing the API response.
    """
    if not logger: logger = logging.getLogger() # Fallback
    # Check if this filter has failed before
    if filter_string in _FAILED_FILTERS:
        logger.warning(f"Skipping known failed filter: {filter_string}")
        return {"Items": [], "Count": 0, "NextPageLink": None}
    
    # Check cache first
    cache_key = f"{filter_string}|{skip_token}"
    if cache_key in _PRICE_CACHE:
        return _PRICE_CACHE[cache_key]

    # Properly escape the filter string for OData - properly encode spaces and special characters
    encoded_filter = urllib.parse.quote(filter_string)
    
    # Build URL
    api_url = "https://prices.azure.com/api/retail/prices"
    params = {
        'api-version': api_version,
        '$filter': filter_string  # We pass the unencoded filter as a param so requests can encode it properly
    }
    
    if skip_token:
        params['$skiptoken'] = skip_token

    try:
        logger.debug(f"Fetching prices with filter: {filter_string}")
        response = requests.get(api_url, params=params)
        
        # Handle non-200 responses
        if response.status_code != 200:
            logger.warning(f"API request failed with status {response.status_code}: {response.text}")
            if response.status_code == 400:
                # Remember this filter caused a 400 error
                _FAILED_FILTERS.add(filter_string)
                logger.warning(f"Added to failed filters: {filter_string}")
            return {"Items": [], "Count": 0, "NextPageLink": None}
        
        result = response.json()
        _PRICE_CACHE[cache_key] = result
        return result
    except Exception as e:
        logger.exception(f"Error fetching prices: {e}")
        return {"Items": [], "Count": 0, "NextPageLink": None}

# --- Pricing Helper Functions --- 
def find_best_match(
    items: List[Dict[str, Any]],
    location: str,
    resource_desc: str,
    required_price_type: str = "Consumption",
    required_unit: Optional[str] = None,
    product_name_pattern: Optional[str] = None, 
    sku_name_pattern: Optional[str] = None,
    meter_name_pattern: Optional[str] = None,
    exact_sku_name: Optional[str] = None,
    exact_meter_name: Optional[str] = None,
    prefer_contains_meter: Optional[List[str]] = None,
    avoid_contains_meter: Optional[List[str]] = None,
    strict_unit_match: bool = False,
    logger: Optional['Logger'] = None
) -> Optional[Dict[str, Any]]:
    """
    Finds the best matching pricing item from a list based on various criteria.
    
    Debug logging has been added to trace the filtering process and identify issues.

    Args:
        items: List of pricing items from API
        location: Location to match (normalized form)
        resource_desc: Description of resource for logging
        required_price_type: Price type to filter on (Consumption, Reservation, etc.)
        required_unit: Unit to require (Hour, Month, etc.), or None to skip unit filtering
        product_name_pattern: Regex pattern to match against productName
        sku_name_pattern: Regex pattern to match against skuName
        meter_name_pattern: Regex pattern to match against meterName
        exact_sku_name: Exact SKU name to prefer in scoring
        exact_meter_name: Exact meter name to prefer in scoring
        prefer_contains_meter: List of strings to prefer in meter name for scoring
        avoid_contains_meter: List of strings to avoid in meter name for scoring
        strict_unit_match: If True, requires exact unit match; if False, allows compatible units

    Returns:
        Best matching item or None if no suitable match found
    """
    if not logger: logger = logging.getLogger() # Fallback
    if not items:
        logger.warning(f"No items provided to find_best_match for {resource_desc}")
        return None

    logger.debug(f"Finding best match for {resource_desc} in {location} from {len(items)} candidates")
    logger.debug(f"Match criteria: price_type={required_price_type}, unit={required_unit}, " 
                f"product_pattern={product_name_pattern}, sku_pattern={sku_name_pattern}, " 
                f"meter_pattern={meter_name_pattern}")
    
    # Initialize counters for debug logging
    passed_filters = 0
    rejected_price_type = 0
    rejected_unit = 0
    rejected_product_pattern = 0
    rejected_sku_pattern = 0
    rejected_meter_pattern = 0
    rejected_negative_score = 0

    candidates = []
    
    for item in items:
        # Skip items with wrong price type
        item_price_type = item.get('priceType', '')
        if required_price_type and item_price_type != required_price_type:
            rejected_price_type += 1
            logger.debug(f"Skipping item with price type {item_price_type} != {required_price_type}: {item.get('skuName')}")
            continue
            
        # Skip items with wrong unit if required
        item_unit = item.get('unitOfMeasure', '')
        if required_unit and item_unit and not _is_compatible_unit(item_unit, required_unit, strict_unit_match):
            rejected_unit += 1
            logger.debug(f"Skipping item with unit {item_unit} not compatible with {required_unit}: {item.get('skuName')}")
            continue

        # Apply product name pattern filtering
        product_name = item.get('productName', '')
        if product_name_pattern and not re.search(product_name_pattern, product_name, re.IGNORECASE):
            rejected_product_pattern += 1
            logger.debug(f"Skipping item with product name not matching pattern {product_name_pattern}: {product_name}")
            continue

        # Apply SKU name pattern filtering
        sku_name = item.get('skuName', '')
        if sku_name_pattern and not re.search(sku_name_pattern, sku_name, re.IGNORECASE):
            rejected_sku_pattern += 1
            logger.debug(f"Skipping item with SKU name not matching pattern {sku_name_pattern}: {sku_name}")
            continue
            
        # Apply meter name pattern filtering
        meter_name = item.get('meterName', '')
        if meter_name_pattern and not re.search(meter_name_pattern, meter_name, re.IGNORECASE):
            rejected_meter_pattern += 1
            logger.debug(f"Skipping item with meter name not matching pattern {meter_name_pattern}: {meter_name}")
            continue
            
        # For items passing all filters, compute a relevance score
        score = 10.0  # Base score
        
        # Boost score for exact SKU match (highest priority)
        if exact_sku_name and sku_name.lower() == exact_sku_name.lower():
            score += 100.0
            logger.debug(f"Exact SKU match +100 points: {sku_name}")
        
        # Boost score for exact meter name match
        if exact_meter_name and meter_name.lower() == exact_meter_name.lower():
            score += 50.0
            logger.debug(f"Exact meter name match +50 points: {meter_name}")
        
        # Boost score for preferred meter contents
        if prefer_contains_meter:
            for keyword in prefer_contains_meter:
                if keyword and keyword.lower() in meter_name.lower():
                    score += 10.0
                    logger.debug(f"Preferred meter keyword match +10 points: {keyword}")
                    
        # Reduce score for avoided meter contents
        if avoid_contains_meter:
            for keyword in avoid_contains_meter:
                if keyword and keyword.lower() in meter_name.lower():
                    score -= 50.0
                    logger.debug(f"Avoided meter keyword match -50 points: {keyword}")
        
        # Skip items with negative scores (strongly avoided)
        if score <= 0:
            rejected_negative_score += 1
            logger.debug(f"Skipping item with negative relevance score: {item.get('skuName')}")
            continue
            
        # Add to candidates with computed score
        price = item.get('retailPrice', 0.0)
        candidates.append((item, score, price))
        passed_filters += 1
    
    # Log filter results
    logger.debug(f"Filter results: {passed_filters} passed, {rejected_price_type} rejected (price type), "
                f"{rejected_unit} rejected (unit), {rejected_product_pattern} rejected (product pattern), "
                f"{rejected_sku_pattern} rejected (SKU pattern), {rejected_meter_pattern} rejected (meter pattern), "
                f"{rejected_negative_score} rejected (negative score)")

    if not candidates:
        logger.warning(f"No matching candidates found for {resource_desc} after filtering {len(items)} items")
        # Log a sample of skipped items to help diagnose matching issues
        if items and len(items) > 0:
            sample_size = min(5, len(items))
            logger.debug(f"Sample of non-matching items (showing {sample_size} of {len(items)}):")
            for i in range(sample_size):
                item = items[i]
                logger.debug(f"  Item {i+1}: SKU={item.get('skuName', 'N/A')}, "
                           f"Product={item.get('productName', 'N/A')}, "
                           f"Meter={item.get('meterName', 'N/A')}, "
                           f"Unit={item.get('unitOfMeasure', 'N/A')}, "
                           f"Price Type={item.get('priceType', 'N/A')}")
        return None

    # Sort by score (descending) and price (ascending)
    candidates.sort(key=lambda x: (-x[1], x[2]))
    
    # Get the top candidate
    best_match = candidates[0][0]
    best_score = candidates[0][1]
    
    logger.debug(f"Best match for {resource_desc}: {best_match.get('skuName')} "
               f"(score: {best_score:.1f}, price: {best_match.get('retailPrice', 0.0):.4f})")
    
    # Log alternative candidates for reference
    if len(candidates) > 1:
        logger.debug(f"Alternative candidates (top 3 of {len(candidates)}):")
        for i in range(1, min(4, len(candidates))):
            alt_item, alt_score, alt_price = candidates[i]
            logger.debug(f"  Alternative {i}: {alt_item.get('skuName')} "
                       f"(score: {alt_score:.1f}, price: {alt_price:.4f})")
    
    return best_match

def estimate_monthly_cost(price_info: Optional[Dict[str, Any]], console: Console = _console, logger: Optional['Logger'] = None) -> Tuple[Optional[float], Optional[str]]:
    """Estimates monthly cost from a price info object."""
    if not logger: logger = logging.getLogger() # Fallback
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
def estimate_disk_cost(sku_name: str, size_gb: int, location: str, console: Console = _console, logger: Optional['Logger'] = None) -> float:
    """Estimates the monthly cost of an Azure Managed Disk using the Retail Prices API."""
    if not logger: logger = logging.getLogger() # Fallback
    logger.info(f"Estimating cost for disk: sku={sku_name}, size={size_gb}GB, location={location}")

    # Normalize location for API query
    normalized_location = _normalize_location(location, logger)
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
    items = fetch_retail_prices(filter_string, logger=logger)

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
    monthly_cost, unit_str = estimate_monthly_cost(best_match, logger)

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


def estimate_public_ip_cost(sku_name: str, location: str, console: Console = _console, logger: Optional['Logger'] = None) -> float:
    """Estimates the monthly cost of an Azure Public IP address using the Retail Prices API."""
    if not logger: logger = logging.getLogger() # Fallback
    logger.info(f"Estimating cost for Public IP: sku={sku_name}, location={location}")

    # Normalize location for API query
    normalized_location = _normalize_location(location, logger)
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
    items = fetch_retail_prices(filter_string, logger=logger)
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
            price, _ = estimate_monthly_cost(best_match, logger) # Converts hourly to monthly
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

def estimate_snapshot_cost(size_gb: int, location: str, sku_name: Optional[str], console: Console = _console, logger: Optional['Logger'] = None) -> float:
    """Estimates the monthly cost of a Managed Disk Snapshot using the Retail Prices API."""
    if not logger: logger = logging.getLogger() # Fallback
    logger.info(f"Estimating cost for Snapshot: size={size_gb}GB, location={location}, sku={sku_name}")

    # Normalize location for API query
    normalized_location = _normalize_location(location, logger)
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
    items = fetch_retail_prices(filter_string, logger=logger)
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
            price_per_gb, unit = estimate_monthly_cost(best_match, logger)
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

def estimate_app_service_plan_cost(tier: str, size: str, location: str, console: Console = _console, logger: Optional['Logger'] = None) -> float:
    """Estimates monthly cost for an App Service Plan."""
    if not logger: logger = logging.getLogger() # Fallback
    logger.debug(f"Estimating App Service Plan cost for {tier} {size} in {location}")
    
    # Normalize location
    location = _normalize_location(location, logger)
    
    # Parse possible pricing tier names from the input tier and size
    combined = f"{tier} {size}".lower()
    
    # Initial assumption based on tier + size (ex: Basic B1)
    tier_name = tier.title()
    size_name = size
    
    # Alternative names for tiers to try in case the initial matching fails
    alternative_names = []
    
    # Handle common App Service naming variations
    if tier.lower() == 'standard':
        alternative_names = [
            f"{tier_name} Plan",
            f"{tier_name} App Service Plan",
            f"{tier_name} Web App",
            f"App Service {tier_name}"
        ]
    elif tier.lower() == 'basic':
        alternative_names = [
            f"{tier_name} Plan",
            f"{tier_name} App Service Plan",
            f"{tier_name} Web App",
            f"App Service {tier_name}"
        ]
    elif tier.lower() == 'premium':
        alternative_names = [
            f"{tier_name} Plan",
            f"{tier_name} App Service Plan",
            f"{tier_name} Web App",
            f"Premium V2", 
            f"Premium V3"
        ]
    elif tier.lower() in ['free', 'shared']:
        alternative_names = [
            f"{tier_name} Plan",
            f"{tier_name} App Service Plan",
            f"{tier_name} Web App",
            f"App Service {tier_name}"
        ]
    
    # Try multiple search approaches
    all_items = []
    
    # First attempt: Using tier name in productName and exact sku match
    filter_string = f"serviceName eq 'App Service' and location eq '{location}'"
    response = fetch_retail_prices(filter_string, logger=logger)
    all_items = response.get('Items', [])
    
    if not all_items:
        logger.warning(f"No App Service items found for {location}, trying 'Azure App Service'")
        # Try alternative service name
        filter_string = f"serviceName eq 'Azure App Service' and location eq '{location}'"
        response = fetch_retail_prices(filter_string, logger=logger)
        all_items = response.get('Items', [])
    
    if all_items:
        logger.debug(f"Found {len(all_items)} App Service price items, searching for tier {tier_name} and size {size_name}")
        
        # Create SKU pattern from size
        sku_pattern = f"^{re.escape(size_name)}$"
        
        # Try each alternative product name
        best_match = None
        for product_name in [f"{tier_name} Plan"] + alternative_names:
            logger.debug(f"Searching for product name: {product_name}")
            
            # Try to find a match with this product name
            match = find_best_match(
                all_items,
                location,
                f"App Service Plan {tier} {size}",
                required_unit='Month',
                product_name_pattern=f"{re.escape(product_name)}",
                sku_name_pattern=sku_pattern
            )
            
            if match:
                best_match = match
                logger.debug(f"Found match with product name '{product_name}'")
                break
        
        # If no match found with specific product names, try a broader search
        if not best_match:
            logger.debug("Trying broad match with just SKU pattern")
        best_match = find_best_match(
                all_items,
                location,
                f"App Service Plan {tier} {size}",
                required_unit='Month',
                sku_name_pattern=sku_pattern
            )
        
        if best_match:
            return best_match.get('retailPrice', 0.0)
    
    logger.warning(f"Could not find a price match for App Service Plan {tier} {size} in {location}")
        return 0.0

def estimate_sql_database_cost(sku_tier: Optional[str], sku_name: Optional[str], family: Optional[str], capacity: Optional[int], location: str, console: Console = _console, logger: Optional['Logger'] = None) -> float:
    """Estimates the monthly cost of an Azure SQL Database (DTU or vCore) using the Retail Prices API."""
    if not logger: logger = logging.getLogger() # Fallback
    logger.info(f"Estimating cost for SQL Database: tier={sku_tier}, sku={sku_name}, family={family}, capacity={capacity}, location={location}")

    # Normalize location for API query
    normalized_location = _normalize_location(location, logger)
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
        items = fetch_retail_prices(filter_string, logger=logger)
        # Add logging for item count
        item_count = len(items) if items else 0
        logger.debug(f"SQL DTU filter '{filter_string}' returned {item_count} items.")

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
                price, unit = estimate_monthly_cost(best_match, logger)
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
        compute_items = fetch_retail_prices(filter_string_compute, logger=logger)
        # Add logging for item count
        item_count = len(compute_items) if compute_items else 0
        logger.debug(f"SQL vCore compute filter '{filter_string_compute}' returned {item_count} items.")
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
                price, _ = estimate_monthly_cost(best_match_compute, logger) # Converts hourly to monthly
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

def estimate_vm_cost(vm_size: str, location: str, os_type: str = 'Linux', console: Console = _console, logger: Optional['Logger'] = None) -> float:
    """
    Estimates hourly cost for an Azure VM.

    Args:
        vm_size: VM size (e.g., 'Standard_D2s_v3').
        location: Azure region.
        os_type: OS type (e.g., 'Windows', 'Linux'). Defaults to 'Linux'.
        console: Optional console for output.

    Returns:
        Estimated hourly cost.
    """
    if not logger: logger = logging.getLogger() # Fallback
    logger.debug(f"Estimating VM cost for {vm_size} ({os_type}) in {location}")
    
    # Normalize location for pricing API
    location = _normalize_location(location, logger)
    
    # Parse the VM size components for more accurate matching
    size_pattern = r'^(?:standard_)?([a-z]+)(\d+)([a-z]*)(?:_v(\d+))?$'
    match = re.match(size_pattern, vm_size.lower())
    
    if not match:
        logger.warning(f"VM size '{vm_size}' doesn't match expected pattern")
        return 0.0

    # Extract components
    series_letter, size_num, features, version = match.groups()
    # Normalize version
    version = version or ""
    
    # Generate possible series names for matching
    possible_series = []
    # Original format: "D Series"
    possible_series.append(f"{series_letter.upper()} Series")
    
    # Alternative formats based on common Azure naming
    if version:
        # "Dv3 Series" format
        possible_series.append(f"{series_letter.upper()}v{version} Series")
        # "D v3 Series" format
        possible_series.append(f"{series_letter.upper()} v{version} Series")
    
    # If B-series, also try "Burstable" in the name
    if series_letter.upper() == 'B':
        possible_series.append("Burstable")
    
    # For VMs with additional features (like 's' for premium storage)
    series_with_features = f"{series_letter.upper()}{features}" if features else series_letter.upper()
    if version:
        possible_series.append(f"{series_with_features}v{version} Series")
    
    # Multiple matching attempts with different filters
    all_items = []
    
    # Try each possible series name
    for series_name in possible_series:
        logger.debug(f"Trying series name: {series_name}")
        
        # Main filter for standard consumption VM
        filter_string = f"serviceName eq 'Virtual Machines' and location eq '{location}' and contains(productName, '{series_name}')"
        if os_type.lower() != 'linux':
            filter_string += f" and contains(productName, '{os_type}')"
        
        # This assumes Windows is the default in the API, which is usually not the case, but provides a fallback
        response = fetch_retail_prices(filter_string, logger=logger)
        items = response.get('Items', [])
        
        if items:
            logger.debug(f"Found {len(items)} price items using series '{series_name}'")
            all_items.extend(items)
            break  # Stop trying if we found items
    
    # If all series attempts failed, try a broader search just using core VM sizing terms
    if not all_items:
        # Try a more generic filter
        filter_string = f"serviceName eq 'Virtual Machines' and location eq '{location}'"
        response = fetch_retail_prices(filter_string, logger=logger)
        all_items = response.get('Items', [])
        logger.debug(f"Falling back to generic VM filter, found {len(all_items)} items")
    
    # Extract VM-specific SKU pattern from size
    sku_pattern = vm_size.replace('Standard_', '').lower()
    logger.debug(f"VM SKU pattern for matching: {sku_pattern}")
    
    # Find best match
    best_match = find_best_match(
        all_items,
        location,
        f"VM {vm_size} ({os_type})",
        required_unit='Hour',
        sku_name_pattern=f"^{re.escape(sku_pattern)}$",
        prefer_contains_meter=['Compute'],
        avoid_contains_meter=['Spot', 'Low Priority', 'Reservation'] if os_type.lower() != 'spot' else []
    )
    
    if best_match:
        return best_match.get('retailPrice', 0.0)
    
    logger.warning(f"Could not find a price match for VM {vm_size} ({os_type}) in {location}")
         return 0.0

def estimate_app_gateway_cost(sku_tier: str, sku_name: str, location: str, console: Console = _console, logger: Optional['Logger'] = None) -> float:
    """Estimates the monthly cost of an Azure Application Gateway instance using the Retail Prices API."""
    if not logger: logger = logging.getLogger() # Fallback
    logger.info(f"Estimating cost for App Gateway: tier={sku_tier}, sku={sku_name}, location={location}")

    # Normalize location for API query
    normalized_location = _normalize_location(location, logger)
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
        instance_items = fetch_retail_prices(filter_string_inst, logger=logger)
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
                price, _ = estimate_monthly_cost(best_match_inst, logger)
                if price is not None:
                    total_monthly_cost += price
                    logger.info(f"Estimated monthly instance cost for {resource_desc_inst}: {price:.2f}")
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
        cu_items = fetch_retail_prices(filter_string_cu, logger=logger)
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
                price_cu, _ = estimate_monthly_cost(best_match_cu, logger)
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

def get_cost_data(credential, subscription_id, console: Console = _console, logger: Optional['Logger'] = None) -> Tuple[Dict, float, str]:
    """Fetches actual cost data using the Cost Management API."""
    if not logger: logger = logging.getLogger() # Fallback
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

def _is_compatible_unit(item_unit: str, required_unit: str, strict_match: bool = False) -> bool:
    """
    Check if a pricing item's unit is compatible with the required unit.
    
    Args:
        item_unit: The unit string from the pricing item
        required_unit: The unit string we're requiring
        strict_match: If True, requires exact case-insensitive match
        
    Returns:
        True if units are compatible, False otherwise
    """
    if not item_unit or not required_unit:
        return False
        
    item_unit_lower = item_unit.lower()
    required_unit_lower = required_unit.lower()
    
    # Handle exact match case
    if item_unit_lower == required_unit_lower:
        return True
        
    # If strict matching is required, we're done
    if strict_match:
        return False
    
    # Non-strict matching handles common variations and abbreviations
    
    # Handle Hour/Hours variations
    if required_unit_lower in ('hour', 'hours', 'hr', 'hrs'):
        return item_unit_lower in ('hour', 'hours', '/hour', '/hours', 'hr', 'hrs', '1 hour', '1 hr')
        
    # Handle Month/Months variations
    if required_unit_lower in ('month', 'months', 'mo', 'mos'):
        return item_unit_lower in ('month', 'months', '/month', '/months', 'mo', 'mos', '1 month', '1 mo')
        
    # Handle GB/Month variations
    if required_unit_lower in ('gb/month', 'gb-month', 'gb-mo', 'gigabyte month'):
        return 'gb' in item_unit_lower and ('month' in item_unit_lower or '/mo' in item_unit_lower)
        
    # Handle 10K variations (often used for transaction pricing)
    if required_unit_lower in ('10k', '10,000', 'tenthousand'):
        return '10' in item_unit_lower and ('thousand' in item_unit_lower or 'k' in item_unit_lower)
    
    # For partial matching, check if required unit is contained in item's unit
    return required_unit_lower in item_unit_lower 