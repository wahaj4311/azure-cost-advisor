import os
import csv
import smtplib
import logging
import pandas as pd
import datetime
from email.message import EmailMessage
from rich.console import Console
from rich.table import Table
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Import necessary functions/constants from other modules
from .config import (
    SNAPSHOT_AGE_THRESHOLD_DAYS,
    LOW_CPU_THRESHOLD_PERCENT,
    APP_SERVICE_PLAN_LOW_CPU_THRESHOLD_PERCENT,
    SQL_DB_LOW_DTU_THRESHOLD_PERCENT,
    SQL_VCORE_LOW_CPU_THRESHOLD_PERCENT,
    IDLE_CONNECTION_THRESHOLD_GATEWAY,
    LOW_CPU_THRESHOLD_WEB_APP,
    METRIC_LOOKBACK_DAYS
)

# Initialize console for potential standalone use or if passed
_console = Console()
logger = logging.getLogger()

# --- Report Generation Functions ---

def generate_html_report_content(
    findings, # Combined findings dictionary/structure?
    cost_data, # Raw cost data if needed
    unattached_disks_df, # Specific DataFrames for each finding type
    stopped_vms_df,
    unused_public_ips_df,
    empty_resource_groups_df,
    empty_plans_df,
    old_snapshots_df,
    low_cpu_vms_df,
    low_usage_app_service_plans_df,
    low_dtu_dbs_df,
    low_cpu_vcore_dbs_df,
    idle_gateways_df,
    low_usage_apps_df,
    orphaned_nsgs_df,
    orphaned_route_tables_df,
    potential_savings, # Dictionary: {'Category': savings_amount}
    total_potential_savings,
    cost_breakdown, # Dictionary: {'ResourceType': cost_amount}
    ignored_resources_df, # DataFrame of ignored resources
    include_ignored,
    subscription_id, # Added for context
    currency # Added for context
):
    """Generates the report content as an HTML string with improved styling."""
    logger = logging.getLogger()

    # --- Helper function to convert DataFrame to HTML table within a Bootstrap Card ---
    def df_to_html_card(df, title, id_suffix, icon_class, description):
        """Convert a DataFrame to an HTML card with styled data table."""
        # If empty dataframe or None, return an empty card with appropriate message
        if df is None or (hasattr(df, 'empty') and df.empty):
            return f"""
            <div class="card mb-4">
                <div class="card-header">
                    <i class="bi {icon_class}"></i> {title}
                </div>
                <div class="card-body">
                    <p class="card-text">{description}</p>
                    <p class="no-data-message"><i class="bi bi-check-circle-fill text-success"></i> No resources found in this category.</p>
                </div>
            </div>
            """

        card_header = f"<h5 class=\"mb-0\"><i class=\"{icon_class} me-2\"></i>{title}</h5>"
        
        card_body_content = ""
        if description:
            card_body_content += f'<p class="card-text text-muted">{description}</p>'

        if df is None or df.empty:
            card_body_content += f"<p class=\"card-text\"><em>None found.</em></p>"
        else:
            # Prepare table HTML
            # Make specific columns like 'Potential Savings' stand out if they exist
            classes = 'table table-striped table-hover table-bordered table-sm'
            table_html = df.to_html(index=False, classes=classes, border=0, na_rep='N/A')
            card_body_content += f"<div class=\"table-responsive\">{table_html}</div>"

        card = f"""
        <div class=\"card mb-4 shadow-sm\" id=\"{id_suffix}\">
            <div class=\"card-header\">{card_header}</div>
            <div class=\"card-body\">{card_body_content}</div>
        </div>
        """
        return card

    # --- Start HTML document ---
    # Use more modern CSS, Bootstrap 5.3+, and icons
    html = f"""
<!DOCTYPE html>
<html lang="en" data-bs-theme="light"> 
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Azure Cost Optimization Report - {subscription_id}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
    <style>
        body {{ 
            padding-top: 1rem;
            padding-bottom: 1rem;
            background-color: #f8f9fa;
        }}
        .report-header {{
            padding-bottom: 1rem;
            margin-bottom: 2rem;
            border-bottom: 1px solid #dee2e6;
        }}
        .summary-card {{
             text-align: center;
        }}
        .potential-savings {{
            font-size: 2.5rem;
            font-weight: 500;
            color: var(--bs-success);
        }}
         .card-header h5 i {{ /* Slightly dim icon color */
             color: #6c757d; 
         }}
        .footer {{
             margin-top: 3rem; 
             padding-top: 1rem;
             font-size: 0.9em; 
             color: #6c757d; 
             text-align: center; 
             border-top: 1px solid #dee2e6;
        }}
        /* Style the tables generated by pandas */
        .table {{
             font-size: 0.9rem; /* Make tables slightly smaller */
        }}
        .table thead th {{
             background-color: #e9ecef;
             font-weight: 500;
        }}
         /* Optional: highlight savings column */
         /* .table td:nth-child(N) {{ font-weight: bold; color: #198754; }} */ 
         /* Replace N with the index of the savings column if needed */

         /* Dark mode styles */
         [data-bs-theme="dark"] body {{
             background-color: #212529;
             color: #dee2e6;
         }}
         [data-bs-theme="dark"] .report-header, [data-bs-theme="dark"] .footer {{
             border-color: #495057;
         }}
         [data-bs-theme="dark"] .card {{
              background-color: #343a40; 
              border-color: #495057;
         }}
         [data-bs-theme="dark"] .card-header {{
              background-color: #495057;
              border-color: #495057;
         }}
          [data-bs-theme="dark"] .potential-savings {{
             color: var(--bs-success-text-emphasis);
         }}
         [data-bs-theme="dark"] .table {{
             color: #dee2e6; /* Ensure table text is readable */
             border-color: #495057;
         }}
         [data-bs-theme="dark"] .table-striped > tbody > tr:nth-of-type(odd) > * {{
             --bs-table-accent-bg: rgba(255, 255, 255, 0.03);
         }}
         [data-bs-theme="dark"] .table-hover > tbody > tr:hover > * {{
             --bs-table-accent-bg: rgba(255, 255, 255, 0.05);
             color: #fff;
         }}
        [data-bs-theme="dark"] .table thead th {{
             background-color: #495057;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="report-header d-flex justify-content-between align-items-center">
            <h1><i class="bi bi-cloud-check-fill me-2"></i>Azure Cost Optimization Report</h1>
             <button class="btn btn-secondary btn-sm" id="theme-toggle-btn"><i class="bi bi-circle-half"></i> Toggle Theme</button>
        </div>
        <p class="text-muted mb-2">Subscription ID: {subscription_id}</p>
        <p class="text-muted mb-4">Generated on: {datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")}</p>

        <div class="card summary-card mb-4 shadow-sm">
            <div class="card-body">
                <h2 class="card-title">Total Potential Monthly Savings</h2>
                <p class="potential-savings">{currency} {total_potential_savings:.2f}</p>
                <p class="text-muted small">(Estimates based on retail prices and identified opportunities. Verify before taking action.)</p>
            </div>
        </div>

        <h2><i class="bi bi-binoculars-fill me-2"></i>Findings & Recommendations</h2>
    """

    # --- Add findings sections using cards ---
    # Structure: df_to_html_card(dataframe, title, card_id, icon, optional_description)
    html += df_to_html_card(unattached_disks_df, "Unattached Disks", "unattached-disks", "bi-hdd-stack", "Disks not connected to any Virtual Machine.")
    html += df_to_html_card(stopped_vms_df, "Stopped VMs (Not Deallocated)", "stopped-vms", "bi-stop-circle-fill", "VMs stopped from the OS but still incurring compute costs.")
    html += df_to_html_card(unused_public_ips_df, "Unused Public IPs", "unused-ips", "bi-globe2", "Static Public IP addresses not associated with any running service.")
    html += df_to_html_card(empty_resource_groups_df, "Empty Resource Groups", "empty-rgs", "bi-trash3-fill", "Resource groups containing no resources.")
    html += df_to_html_card(empty_plans_df, "Empty App Service Plans", "empty-asps", "bi-file-earmark-excel-fill", "App Service Plans with no deployed applications.")
    html += df_to_html_card(old_snapshots_df, f"Old Snapshots (> {SNAPSHOT_AGE_THRESHOLD_DAYS} days)", "old-snapshots", "bi-camera-fill", "Disk snapshots older than the configured threshold.")
    html += df_to_html_card(low_cpu_vms_df, f"Low CPU VMs (< {LOW_CPU_THRESHOLD_PERCENT}% Avg)", "low-cpu-vms", "bi-pc-display", f"Running VMs with average CPU usage below {LOW_CPU_THRESHOLD_PERCENT}% over the last {METRIC_LOOKBACK_DAYS} days.")
    html += df_to_html_card(low_usage_app_service_plans_df, f"Low CPU App Service Plans (< {APP_SERVICE_PLAN_LOW_CPU_THRESHOLD_PERCENT}% Avg)", "low-asps", "bi-server", f"App Service Plans (Basic+ tier) with average CPU below {APP_SERVICE_PLAN_LOW_CPU_THRESHOLD_PERCENT}% over the last {METRIC_LOOKBACK_DAYS} days.")
    html += df_to_html_card(low_dtu_dbs_df, f"Low DTU SQL Databases (< {SQL_DB_LOW_DTU_THRESHOLD_PERCENT}% Avg)", "low-dtu-dbs", "bi-database-fill-down", f"SQL Databases (DTU model) with average DTU usage below {SQL_DB_LOW_DTU_THRESHOLD_PERCENT}% over the last {METRIC_LOOKBACK_DAYS} days.")
    html += df_to_html_card(low_cpu_vcore_dbs_df, f"Low CPU vCore SQL Databases (< {SQL_VCORE_LOW_CPU_THRESHOLD_PERCENT}% Avg)", "low-vcore-dbs", "bi-database-fill-gear", f"SQL Databases (vCore model) with average CPU usage below {SQL_VCORE_LOW_CPU_THRESHOLD_PERCENT}% over the last {METRIC_LOOKBACK_DAYS} days.")
    html += df_to_html_card(idle_gateways_df, f"Idle Application Gateways (< {IDLE_CONNECTION_THRESHOLD_GATEWAY} Avg Connections)", "idle-gateways", "bi-router-fill", f"Application Gateways with average current connections below {IDLE_CONNECTION_THRESHOLD_GATEWAY} over the last {METRIC_LOOKBACK_DAYS} days.")
    html += df_to_html_card(low_usage_apps_df, f"Low CPU Web Apps (< {LOW_CPU_THRESHOLD_WEB_APP}% Avg)", "low-webapps", "bi-window-stack", f"Individual Web Apps (on Basic+ plans) with average CPU usage below {LOW_CPU_THRESHOLD_WEB_APP}% over the last {METRIC_LOOKBACK_DAYS} days.")
    html += df_to_html_card(orphaned_nsgs_df, "Orphaned Network Security Groups", "orphaned-nsgs", "bi-shield-slash-fill", "NSGs not associated with any NIC or Subnet.") 
    html += df_to_html_card(orphaned_route_tables_df, "Orphaned Route Tables", "orphaned-rts", "bi-map-fill", "Route Tables not associated with any Subnet.") 
    
    # Add Potential Savings Breakdown Card
    # Use the same nice names as the console summary
    finding_names = {
        'unattached_disks': "Unattached Disks",
        'stopped_vms': "Stopped VMs (Not Deallocated)",
        'unused_public_ips': "Unused Public IPs",
        'empty_rgs': "Empty Resource Groups",
        'empty_asps': "Empty App Service Plans", # Changed key name here
        'old_snapshots': f"Old Snapshots (> {SNAPSHOT_AGE_THRESHOLD_DAYS} days)",
        'low_cpu_vms': f"Low CPU VMs (< {LOW_CPU_THRESHOLD_PERCENT}%)",
        'low_cpu_asps': f"Low CPU App Service Plans (< {APP_SERVICE_PLAN_LOW_CPU_THRESHOLD_PERCENT}%)",
        'low_dtu_dbs': f"Low DTU SQL DBs (< {SQL_DB_LOW_DTU_THRESHOLD_PERCENT}%)",
        'low_cpu_vcore_dbs': f"Low CPU vCore SQL DBs (< {SQL_VCORE_LOW_CPU_THRESHOLD_PERCENT}%)",
        'idle_gateways': f"Idle Application Gateways (< {IDLE_CONNECTION_THRESHOLD_GATEWAY} conn)",
        'low_cpu_apps': f"Low CPU Web Apps (< {LOW_CPU_THRESHOLD_WEB_APP}%)",
        'orphaned_nsgs': "Orphaned NSGs",
        'orphaned_rts': "Orphaned Route Tables",
    }
    savings_breakdown_list = [(finding_names.get(cat, cat), savings) for cat, savings in potential_savings.items() if savings > 0]
    savings_breakdown_df = pd.DataFrame(savings_breakdown_list, columns=['Category', 'Potential Savings'])
    if not savings_breakdown_df.empty:
         savings_breakdown_df['Potential Savings'] = savings_breakdown_df['Potential Savings'].apply(lambda x: f"{currency} {x:.2f}")
    html += df_to_html_card(savings_breakdown_df, "Potential Savings Breakdown (Monthly Estimate)", "savings-breakdown", "bi-graph-up-arrow", "Estimated monthly cost savings by resource category.")

    # Add Cost Breakdown Card (Optional - can be large)
    # cost_breakdown_df = pd.DataFrame(list(cost_breakdown.items()), columns=['Resource Type', 'Estimated Cost']) if cost_breakdown else pd.DataFrame()
    # if not cost_breakdown_df.empty:
    #      cost_breakdown_df['Estimated Cost'] = cost_breakdown_df['Estimated Cost'].apply(lambda x: f"{currency} {x:.2f}")
    #      cost_breakdown_df = cost_breakdown_df.sort_values(by='Estimated Cost', ascending=False) # Sort by cost
    # html += df_to_html_card(cost_breakdown_df, "Cost Breakdown by Resource Type (Monthly Estimate)", "cost-breakdown", "bi-currency-dollar")

    # Add Ignored Resources section (if applicable)
    if include_ignored and ignored_resources_df is not None and not ignored_resources_df.empty:
         html += df_to_html_card(ignored_resources_df, "Ignored Resources", "ignored-resources", "bi-eye-slash-fill", "Resources excluded from cleanup suggestions based on tags or configuration.")

    # --- End HTML document ---
    html += f"""
        <div class="footer">
            <p>Report generated by Azure Cost Advisor script.</p>
        </div>
    </div> <!-- Closing container -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        const themeToggleBtn = document.getElementById('theme-toggle-btn');
        const currentTheme = localStorage.getItem('theme') ? localStorage.getItem('theme') : 'light';
        document.documentElement.setAttribute('data-bs-theme', currentTheme);

        themeToggleBtn.addEventListener('click', () => {{
            let newTheme = document.documentElement.getAttribute('data-bs-theme') === 'dark' ? 'light' : 'dark';
            document.documentElement.setAttribute('data-bs-theme', newTheme);
            localStorage.setItem('theme', newTheme);
        }});
    </script>
</body>
</html>
    """
    logger.info("HTML report content generated.")
    return html

def write_html_report(html_content, filename):
    """Writes the HTML content to a file."""
    logger = logging.getLogger()
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logger.info(f"HTML report successfully written to {filename}")
        print(f"\n📄 HTML report successfully written to: {filename}") # User feedback
        return True
    except IOError as e:
        logger.error(f"Error writing HTML report to {filename}: {e}")
        print(f"\n⚠️ Error writing HTML report to {filename}: {e}") # User feedback
        return False

# --- Summary and CSV Report Generation (Simplified - relies on DataFrames from main script) ---
def generate_summary_report(
    findings_dfs: dict, # Dict like {'unattached_disks': df1, 'stopped_vms': df2, ...}
    total_potential_savings: float,
    currency: str,
    output_csv_file: str = None,
    console: Console = _console ):
    """Generates console summary using Rich Table and optional CSV report."""
    logger = logging.getLogger()
    console.print("\n[bold blue]--- Azure Cost Optimization Summary Report ---[/]")

    # Savings Summary
    console.print(f"\n💰 Total Potential Monthly Savings: [bold green]{currency} {total_potential_savings:.2f}[/]")
    
    has_findings = any(df is not None and not df.empty for df in findings_dfs.values())

    if not has_findings:
        console.print("\n✅ No immediate cost optimization opportunities or cleanup suggestions found based on current checks.")
    else:
        console.print("\n🔎 [bold]Findings Summary:[/]")
        
        # Create Rich Table for findings
        findings_table = Table(show_header=True, header_style="bold magenta", title=None, box=None, padding=(0, 1))
        findings_table.add_column("Finding Type", style="cyan", no_wrap=True)
        findings_table.add_column("Count", style="bold yellow", justify="right")

        # Define nice names for finding types
        finding_names = {
            'unattached_disks': "Unattached Disks",
            'stopped_vms': "Stopped VMs (Not Deallocated)",
            'unused_public_ips': "Unused Public IPs",
            'empty_rgs': "Empty Resource Groups",
            'empty_asps': "Empty App Service Plans",
            'old_snapshots': f"Old Snapshots (> {SNAPSHOT_AGE_THRESHOLD_DAYS} days)",
            'low_cpu_vms': f"Low CPU VMs (< {LOW_CPU_THRESHOLD_PERCENT}%)",
            'low_cpu_asps': f"Low CPU App Service Plans (< {APP_SERVICE_PLAN_LOW_CPU_THRESHOLD_PERCENT}%)",
            'low_dtu_dbs': f"Low DTU SQL DBs (< {SQL_DB_LOW_DTU_THRESHOLD_PERCENT}%)",
            'low_cpu_vcore_dbs': f"Low CPU vCore SQL DBs (< {SQL_VCORE_LOW_CPU_THRESHOLD_PERCENT}%)",
            'idle_gateways': f"Idle Application Gateways (< {IDLE_CONNECTION_THRESHOLD_GATEWAY} conn)",
            'low_cpu_apps': f"Low CPU Web Apps (< {LOW_CPU_THRESHOLD_WEB_APP}%)",
            'orphaned_nsgs': "Orphaned NSGs",
            'orphaned_rts': "Orphaned Route Tables",
        }

        # Populate table
        for key, df in findings_dfs.items():
             if df is not None and not df.empty:
                 nice_name = finding_names.get(key, key.replace('_', ' ').title()) # Fallback name
                 findings_table.add_row(nice_name, str(len(df)))
        
        console.print(findings_table)

    # --- Handle CSV Output (remains the same) --- 
    if output_csv_file: 
        all_findings_list = []
        # Define standard columns, potentially adding 'Finding Type'
        # Example: ['Finding Type', 'Name', 'Resource Group', 'Location', 'Details', 'Potential Savings', 'Recommendation']
        common_columns = ['Finding Type', 'Name', 'Resource Group', 'Location', 'Details', 'Potential Monthly Savings', 'Recommendation']
        
        # Iterate through the findings DataFrames and format them
        for finding_type, df in findings_dfs.items():
            if df is not None and not df.empty:
                temp_df = df.copy()
                temp_df['Finding Type'] = finding_names.get(finding_type, finding_type.replace('_', ' ').title()) # Use nice name
                
                # Ensure all common columns exist, adding them with default values if missing
                for col in common_columns:
                     if col not in temp_df.columns:
                         # Handle specific defaults if needed, e.g., savings
                         if col == 'Potential Monthly Savings':
                              temp_df[col] = 0.0
                         elif col == 'Details':
                              # Add more specific details based on finding type if possible
                              temp_df[col] = 'N/A' 
                         else:
                              temp_df[col] = 'N/A' 
                
                # Reorder and select columns
                all_findings_list.append(temp_df[common_columns])

        if all_findings_list:
            # Combine all findings into a single DataFrame
            combined_df = pd.concat(all_findings_list, ignore_index=True)
            # Format savings column
            if 'Potential Monthly Savings' in combined_df.columns:
                # Ensure the column is numeric before formatting
                combined_df['Potential Monthly Savings'] = pd.to_numeric(combined_df['Potential Monthly Savings'], errors='coerce')
                combined_df['Potential Monthly Savings'] = combined_df['Potential Monthly Savings'].apply(lambda x: f"{currency} {x:.2f}" if pd.notna(x) else f"{currency} 0.00")
            
            try:
                combined_df.to_csv(output_csv_file, index=False, encoding='utf-8')
                logger.info(f"Consolidated CSV report successfully written to {output_csv_file}")
                console.print(f"\n📄 Consolidated CSV report successfully written to: {output_csv_file}")
            except Exception as e:
                logger.error(f"Error writing consolidated CSV report to {output_csv_file}: {e}")
                console.print(f"\n⚠️ Error writing consolidated CSV report to {output_csv_file}: {e}")
        elif not has_findings:
             # Write a CSV with a single row indicating no findings
             try:
                 pd.DataFrame([{'Finding Type': 'Summary', 'Details': 'No findings based on current checks'}]).to_csv(output_csv_file, index=False, encoding='utf-8')
                 logger.info(f"CSV report (no findings) written to {output_csv_file}")
                 console.print(f"\n📄 CSV report (no findings) written to: {output_csv_file}")
             except Exception as e:
                 logger.error(f"Error writing empty CSV report to {output_csv_file}: {e}")
                 console.print(f"\n⚠️ Error writing empty CSV report to {output_csv_file}: {e}")
        else:
            logger.info("No dataframes with findings to write to CSV.")

    # Note: The function used to return the text summary. 
    # It now prints directly to console. If the text summary is needed 
    # elsewhere (e.g., for email), it would need to be reconstructed or the 
    # function adjusted to return it alongside printing the table.
    # For now, returning None as the console output is handled directly.
    return None 

def send_email_report(report_content, smtp_config: dict, console: Console = _console):
    """Sends the report content via email using a configuration dictionary."""
    logger = logging.getLogger()
    console.print("\n📧 [blue]Attempting to send email report...[/]")

    # Extract config from dictionary
    smtp_host = smtp_config.get('host')
    smtp_port = smtp_config.get('port')
    smtp_user = smtp_config.get('user')
    smtp_password = smtp_config.get('password')
    sender_email = smtp_config.get('sender')
    recipient_emails = smtp_config.get('recipient') # Should be a list

    # Basic validation
    if not all([smtp_host, smtp_port, smtp_user, smtp_password, sender_email, recipient_emails]):
        console.print("  - [yellow]⚠️ Email configuration incomplete. Missing one or more required keys:[/yellow]")
        console.print("     [dim]'host', 'port', 'user', 'password', 'sender', 'recipient' (list)[/dim]")
        console.print("  - Email not sent.")
        return False
    if not isinstance(recipient_emails, list):
        console.print("  - [yellow]⚠️ Email configuration error: 'recipient' must be a list of email addresses.[/yellow]")
        console.print("  - Email not sent.")
        return False

    try:
        msg = EmailMessage()
        msg['Subject'] = f"Azure Cost Optimization Report - {datetime.datetime.now().strftime('%Y-%m-%d')}"
        msg['From'] = sender_email
        msg['To'] = ", ".join(recipient_emails)
        # Use pre-formatted text content for email body
        msg.set_content(report_content)

        # Optional: Add HTML version if available (e.g., generate HTML report first)
        # html_report_content = generate_html_report_content(...) # If you generate HTML
        # msg.add_alternative(html_report_content, subtype='html')

        # Connect and send (adapt based on port for SSL/TLS)
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                if smtp_port == 587: # Standard TLS port
                    server.starttls()
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
        
        logger.info(f"Email report successfully sent to: {', '.join(recipient_emails)}")
        console.print(f"  - ✅ [green]Email report successfully sent to:[/green] {', '.join(recipient_emails)}")
        return True

    except ValueError as e:
        logger.error(f"Invalid SMTP Port ({smtp_port}): {e}", exc_info=True)
        console.print(f"  - [bold red]❌ Invalid SMTP Port:[/bold red] {e}. Must be an integer.")
        console.print("  - Email not sent.")
        return False
    except smtplib.SMTPAuthenticationError:
         logger.error(f"SMTP Authentication Failed for user '{smtp_user}'.")
         console.print(f"  - [bold red]❌ SMTP Authentication Failed for user[/bold red] '{smtp_user}'. Check credentials.")
         console.print("  - Email not sent.")
         return False
    except smtplib.SMTPException as e:
         logger.error(f"SMTP error occurred: {e}", exc_info=True)
         console.print(f"  - [bold red]❌ SMTP Error:[/bold red] {e}")
         console.print("  - Email not sent.")
         return False
    except Exception as e:
        logger.error(f"Failed to send email: {e}", exc_info=True)
        console.print(f"  - [bold red]❌ Failed to send email:[/bold red] {e}")
        return False 

# --- Rich Table Helper (can be used by console summary) ---
def print_rich_table(df, title, icon=":mag:", console: Console = _console):
    """Prints a DataFrame as a nicely formatted Rich table."""
    if df is None or df.empty:
        return # Don't print empty tables

    table = Table(title=f"\n{icon} {title}", show_header=True, header_style="bold magenta")

    # Add columns
    for column in df.columns:
         # Simple heuristic for column styling
         style = "green" if "Savings" in column else None
         justify = "right" if "Savings" in column or "Size" in column or "Avg" in column else "left"
         table.add_column(column, style=style, justify=justify)

    # Add rows
    for _, row in df.iterrows():
        # Convert all row items to strings, handling potential NaN/None
        str_row = [str(item) if pd.notna(item) else "N/A" for item in row]
        table.add_row(*str_row)

    console.print(table)

# --- Export Functions ---

def export_findings_to_csv_local(findings_dfs: dict, export_dir: str):
    """Exports non-empty findings DataFrames to CSV files in a local directory."""
    if not findings_dfs:
        logger.info("No findings data to export.")
        return

    try:
        # Ensure the export directory exists
        os.makedirs(export_dir, exist_ok=True)
        logger.info(f"Ensured local export directory exists: {export_dir}")

        exported_files_count = 0
        for finding_type, df in findings_dfs.items():
            if df is not None and not df.empty:
                filename = f"{finding_type}.csv"
                filepath = os.path.join(export_dir, filename)
                try:
                    df.to_csv(filepath, index=False, encoding='utf-8')
                    logger.info(f"Successfully exported {finding_type} findings to {filepath}")
                    exported_files_count += 1
                except Exception as e:
                    logger.error(f"Failed to export {finding_type} to CSV {filepath}: {e}", exc_info=True)
            else:
                 logger.debug(f"Skipping export for empty finding type: {finding_type}")

        if exported_files_count > 0:
             logger.info(f"Finished exporting {exported_files_count} finding(s) to CSV files in {export_dir}")
        else:
             logger.info(f"No non-empty findings DataFrames were available to export to CSV.")

    except Exception as e:
         logger.error(f"Error during local CSV export process to directory {export_dir}: {e}", exc_info=True) 