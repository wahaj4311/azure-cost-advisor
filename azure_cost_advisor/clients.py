import os
import logging
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import SubscriptionClient
from rich.console import Console # Keep console for now, might pass later

# Initialize console here or pass it?
# For now, keep it initialized here for self-contained function, but main will pass it later.
_console = Console()

def get_azure_credentials(console: Console = _console):
    """Authenticates and determines the Azure Subscription ID."""
    logger = logging.getLogger()
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
                logger.error("No Azure subscriptions found for the current credential.")
                raise ValueError("No Azure subscriptions found for the current credential.")
            elif len(subs) == 1:
                subscription_id = subs[0].subscription_id
                console.print(f"Automatically detected and using subscription: [bold cyan]{subs[0].display_name}[/] ({subscription_id})")
            else:
                console.print("[bold yellow]Multiple Azure subscriptions found. Please select one:[/]")
                for i, sub in enumerate(subs):
                    console.print(f"  [bold]{i+1}[/]. [cyan]{sub.display_name}[/] ({sub.subscription_id})")

                while True:
                    try:
                        choice = console.input("Enter the number of the subscription to use: ")
                        selected_index = int(choice) - 1
                        if 0 <= selected_index < len(subs):
                            selected_sub = subs[selected_index]
                            subscription_id = selected_sub.subscription_id
                            console.print(f"Selected subscription: [bold cyan]{selected_sub.display_name}[/] ({subscription_id})")
                            break
                        else:
                            console.print("[red]Invalid selection. Please enter a number from the list.[/]")
                    except ValueError:
                        console.print("[red]Invalid input. Please enter a number.[/]")
                    except EOFError: # Handle Ctrl+D or empty input gracefully
                        console.print("\n[red]Selection cancelled.[/]")
                        logger.warning("Subscription selection cancelled by user.")
                        raise ValueError("Subscription selection cancelled.") # Propagate to main handler

        console.print(f"Using Subscription ID: [bold cyan]{subscription_id}[/]")
        console.print(":white_check_mark: [bold green]Authenticated successfully.[/]")
        logger.info(f"Authenticated successfully for subscription ID: {subscription_id}")
        return credential, subscription_id

    except Exception as e:
        logger.error(f"Authentication or subscription detection failed: {e}", exc_info=True)
        console.print(f"[bold red]Authentication or subscription detection failed:[/] {e}")
        return None, None

# Placeholder for future client helper functions if needed
# def get_compute_client(credential, subscription_id):
#     return ComputeManagementClient(credential, subscription_id) 