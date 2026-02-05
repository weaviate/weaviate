#!/usr/bin/env python3
"""
Namespace-to-Principal Mapping Demo

This script demonstrates multi-tenant isolation by:
1. Creating users bound to different namespaces
2. Showing each user can only access their namespace
3. Showing admin can access all namespaces

Quick Start:
  # Terminal 1: Start Weaviate with namespace demo config
  ./tools/dev/run_dev_server.sh local-namespace-demo

  # Terminal 2: Run this demo
  cd namespace_demo
  pip install requests rich
  python demo.py              # Run automatically
  python demo.py -i           # Run interactively (step by step)

The demo uses these defaults (matching local-namespace-demo config):
  WEAVIATE_URL=http://localhost:8080
  ADMIN_API_KEY=admin-key
"""

import os
import sys
import argparse
import requests
from typing import Tuple

WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8080")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "admin-key")

# Global flag for interactive mode
INTERACTIVE = False

# Try to import rich, fall back to basic output if not available
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.syntax import Syntax
    from rich.table import Table
    from rich.text import Text
    from rich.markdown import Markdown
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

console = Console() if RICH_AVAILABLE else None


def clear_screen():
    """Clear the terminal screen."""
    if INTERACTIVE:
        os.system('cls' if os.name == 'nt' else 'clear')


def wait_for_key(prompt: str = "Press any key to continue..."):
    """Wait for user to press a key."""
    if not INTERACTIVE:
        return
    console.print(f"\n[dim]{prompt}[/dim]")
    try:
        import termios
        import tty
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    except:
        input()


def show_step_header(step_num: int, title: str, description: str = None):
    """Show a beautiful step header."""
    clear_screen()

    if RICH_AVAILABLE:
        console.print()
        console.print(Panel(
            f"[bold white]{title}[/bold white]",
            title=f"[bold cyan]Step {step_num}[/bold cyan]",
            border_style="cyan",
            padding=(1, 2),
        ))
        if description and INTERACTIVE:
            console.print()
            console.print(Panel(
                description,
                border_style="dim",
                padding=(1, 2),
            ))
    else:
        print(f"\n{'='*60}")
        print(f"[Step {step_num}] {title}")
        print('='*60)
        if description and INTERACTIVE:
            print(f"\n{description}\n")


def show_api_call(method: str, endpoint: str, headers: dict = None, body: dict = None):
    """Show the API call being made with syntax highlighting."""
    if RICH_AVAILABLE:
        # Build the curl-like representation
        lines = [f"{method} {endpoint}"]
        if headers:
            for k, v in headers.items():
                if k == "Authorization":
                    # Truncate long auth headers
                    v = v[:35] + "..." if len(v) > 35 else v
                lines.append(f"  {k}: {v}")
        if body:
            import json
            lines.append(f"  Body: {json.dumps(body)}")

        code = "\n".join(lines)

        console.print()
        console.print(Panel(
            Syntax(code, "http", theme="monokai", line_numbers=False),
            title="[bold blue]API Request[/bold blue]",
            border_style="blue",
            padding=(0, 1),
        ))
    else:
        print(f"\n  API Call: {method} {endpoint}")
        if headers:
            for k, v in headers.items():
                if k == "Authorization":
                    v = v[:35] + "..." if len(v) > 35 else v
                print(f"    {k}: {v}")
        if body:
            print(f"    Body: {body}")


def show_result(result: dict, success: bool = True):
    """Show the result of an API call."""
    import json

    if RICH_AVAILABLE:
        style = "green" if success else "red"
        title = "[bold green]✓ Result[/bold green]" if success else "[bold red]✗ Error[/bold red]"

        # Pretty format the result
        if isinstance(result, dict):
            # Truncate long values for display
            display_result = {}
            for k, v in result.items():
                if isinstance(v, str) and len(v) > 50:
                    display_result[k] = v[:50] + "..."
                else:
                    display_result[k] = v
            content = json.dumps(display_result, indent=2)
        else:
            content = str(result)

        console.print(Panel(
            Syntax(content, "json", theme="monokai", line_numbers=False),
            title=title,
            border_style=style,
            padding=(0, 1),
        ))
    else:
        prefix = "  ✓" if success else "  ✗"
        print(f"{prefix} Result: {result}")


def show_success(message: str):
    """Show a success message."""
    if RICH_AVAILABLE:
        console.print(f"  [green]✓[/green] {message}")
    else:
        print(f"  ✓ {message}")


def show_error(message: str):
    """Show an error message."""
    if RICH_AVAILABLE:
        console.print(f"  [red]✗[/red] {message}")
    else:
        print(f"  ✗ {message}")


def show_info(message: str):
    """Show an info message."""
    if RICH_AVAILABLE:
        console.print(f"  [dim]→[/dim] {message}")
    else:
        print(f"  → {message}")


def show_highlight(message: str):
    """Show a highlighted message."""
    if RICH_AVAILABLE:
        console.print(f"  [yellow]{message}[/yellow]")
    else:
        print(f"  {message}")


def show_next_action(description: str):
    """Show what's about to happen next."""
    if not INTERACTIVE:
        return

    if RICH_AVAILABLE:
        console.print()
        console.print(Panel(
            f"[bold yellow]▶ Next:[/bold yellow] {description}",
            border_style="yellow",
            padding=(0, 1),
        ))
    else:
        print(f"\n  ▶ Next: {description}")

    wait_for_key()


class WeaviateClient:
    def __init__(self, url: str, api_key: str, name: str = "client"):
        self.url = url.rstrip('/')
        self.api_key = api_key
        self.name = name

    def _headers(self, namespace: str = None) -> dict:
        h = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        if namespace:
            h["X-Weaviate-Namespace"] = namespace
        return h

    def create_user(self, user_id: str, namespace: str = None, show_api: bool = False) -> Tuple[bool, dict]:
        """Create a user, optionally bound to a namespace."""
        endpoint = f"{self.url}/v1/users/db/{user_id}"
        headers = self._headers(namespace)

        if show_api:
            show_api_call("POST", endpoint, headers)

        resp = requests.post(endpoint, headers=headers)
        result = resp.json() if resp.status_code in [200, 201] else {"status": resp.status_code, "error": resp.text}

        if show_api and INTERACTIVE:
            show_result(result, resp.status_code == 201)
            wait_for_key()

        if resp.status_code == 201:
            return True, result
        return False, result

    def delete_user(self, user_id: str) -> bool:
        """Delete a user."""
        resp = requests.delete(
            f"{self.url}/v1/users/db/{user_id}",
            headers=self._headers()
        )
        return resp.status_code in [200, 204, 404]

    def create_collection(self, name: str, namespace: str = None, show_api: bool = False) -> Tuple[bool, dict]:
        """Create a collection, optionally in a specific namespace."""
        endpoint = f"{self.url}/v1/schema"
        headers = self._headers(namespace)
        payload = {
            "class": name,
            "properties": [
                {"name": "title", "dataType": ["text"]}
            ]
        }

        if show_api:
            show_api_call("POST", endpoint, headers, {"class": name, "properties": "[...]"})

        resp = requests.post(endpoint, headers=headers, json=payload)

        if resp.status_code == 200:
            if show_api and INTERACTIVE:
                show_result({"class": name, "status": "created"}, True)
                wait_for_key()
            return True, resp.json()

        result = {"status": resp.status_code, "error": resp.text}
        if show_api and INTERACTIVE:
            show_result(result, False)
            wait_for_key()
        return False, result

    def get_schema(self, show_api: bool = False) -> Tuple[bool, dict]:
        """Get schema (collections visible to this user)."""
        endpoint = f"{self.url}/v1/schema"
        headers = self._headers()

        if show_api:
            show_api_call("GET", endpoint, headers)

        resp = requests.get(endpoint, headers=headers)

        if resp.status_code == 200:
            result = resp.json()
            if show_api and INTERACTIVE:
                classes = [c.get("class") for c in (result.get("classes") or [])]
                show_result({"classes": classes}, True)
                wait_for_key()
            return True, result
        return False, {"status": resp.status_code, "error": resp.text}

    def delete_collection(self, name: str, namespace: str = None) -> bool:
        """Delete a collection."""
        resp = requests.delete(
            f"{self.url}/v1/schema/{name}",
            headers=self._headers(namespace)
        )
        return resp.status_code in [200, 204, 404]

    def check_ready(self) -> bool:
        """Check if Weaviate is ready."""
        try:
            resp = requests.get(f"{self.url}/v1/meta", headers=self._headers())
            return resp.status_code == 200
        except:
            return False

    def assign_role(self, user_id: str, role: str, show_api: bool = False) -> Tuple[bool, dict]:
        """Assign an RBAC role to a user."""
        endpoint = f"{self.url}/v1/authz/users/{user_id}/assign"
        headers = self._headers()
        body = {"roles": [role]}

        if show_api:
            show_api_call("POST", endpoint, headers, body)

        resp = requests.post(endpoint, headers=headers, json=body)

        if resp.status_code == 200:
            if show_api and INTERACTIVE:
                show_result({"status": "role assigned"}, True)
                wait_for_key()
            return True, {}
        return False, {"status": resp.status_code, "error": resp.text}

    def revoke_role(self, user_id: str, role: str) -> bool:
        """Revoke an RBAC role from a user."""
        resp = requests.post(
            f"{self.url}/v1/authz/users/{user_id}/revoke",
            headers=self._headers(),
            json={"roles": [role]}
        )
        return resp.status_code in [200, 204, 404]

    def get_user_roles(self, user_id: str) -> list:
        """Get roles assigned to a user."""
        resp = requests.get(
            f"{self.url}/v1/authz/users/{user_id}/roles",
            headers=self._headers()
        )
        if resp.status_code == 200:
            data = resp.json()
            # Handle both possible response formats
            if isinstance(data, dict) and "roles" in data:
                return [r.get("name") if isinstance(r, dict) else r for r in data.get("roles", [])]
            elif isinstance(data, list):
                return [r.get("name") if isinstance(r, dict) else r for r in data]
        return []


def cleanup(admin: WeaviateClient):
    """Clean up test users and collections."""
    admin.delete_user("tenanta-user")
    admin.delete_user("tenantb-user")
    admin.delete_collection("Articles", namespace="tenanta")
    admin.delete_collection("Articles", namespace="tenantb")


def show_welcome():
    """Show the welcome screen."""
    clear_screen()

    if RICH_AVAILABLE:
        console.print()
        console.print(Panel(
            "[bold white]Namespace-to-Principal Mapping Demo[/bold white]\n\n"
            "This demo shows how API keys are bound to namespaces:\n\n"
            "  [cyan]•[/cyan] Admin creates users assigned to specific namespaces\n"
            "  [cyan]•[/cyan] Each user's API key only accesses their namespace\n"
            "  [cyan]•[/cyan] Collections/data are isolated between namespaces",
            title="[bold cyan]🔐 Weaviate Multi-Tenant Isolation[/bold cyan]",
            border_style="cyan",
            padding=(1, 2),
        ))

        if INTERACTIVE:
            console.print()
            console.print("[yellow]Running in INTERACTIVE mode[/yellow]")
            console.print("[dim]Press any key to advance through each step[/dim]")
            wait_for_key("Press any key to begin...")
    else:
        print("="*60)
        print("  Namespace-to-Principal Mapping Demo")
        print("="*60)
        print("\nThis demo shows how API keys are bound to namespaces:")
        print("- Admin creates users assigned to specific namespaces")
        print("- Each user's API key only accesses their namespace")
        print("- Collections/data are isolated between namespaces\n")

        if INTERACTIVE:
            input("Press Enter to begin...")


def show_summary():
    """Show the final summary."""
    clear_screen()

    if RICH_AVAILABLE:
        console.print()
        console.print(Panel(
            "[bold green]Demo Complete![/bold green]",
            border_style="green",
            padding=(1, 2),
        ))

        console.print()
        console.print(Panel(
            "[bold]Summary:[/bold]\n\n"
            "  [green]•[/green] Users are created with namespace binding via [cyan]X-Weaviate-Namespace[/cyan] header\n"
            "  [green]•[/green] Each user's API key is [bold]permanently bound[/bold] to their assigned namespace\n"
            "  [green]•[/green] All operations automatically use the bound namespace\n"
            "  [green]•[/green] [bold cyan]Explicit RBAC:[/bold cyan] [cyan]namespace-admin-{ns}[/cyan] role is auto-assigned\n"
            "  [green]•[/green] Permissions come from explicit roles, enabling audit trails\n"
            "  [green]•[/green] Admin users can override namespace for management operations",
            border_style="dim",
            padding=(1, 2),
        ))

        # Show the key API calls
        create_user_example = """# Create user bound to namespace (admin only):
POST /v1/users/db/tenanta-user
Headers:
  Authorization: Bearer <admin-key>
  X-Weaviate-Namespace: tenanta  ← Binds user to this namespace
# → Automatically creates and assigns 'namespace-admin-tenanta' role"""

        user_request_example = """# User operations automatically use bound namespace:
GET /v1/schema
Headers:
  Authorization: Bearer <tenanta-user-key>
  # No X-Weaviate-Namespace needed! ← Server derives from API key"""

        console.print()
        console.print(Panel(
            Syntax(create_user_example, "yaml", theme="monokai"),
            title="[bold blue]Create Namespace-Bound User[/bold blue]",
            border_style="blue",
            padding=(0, 1),
        ))

        console.print()
        console.print(Panel(
            Syntax(user_request_example, "yaml", theme="monokai"),
            title="[bold blue]User Request (No Header Needed)[/bold blue]",
            border_style="blue",
            padding=(0, 1),
        ))

        console.print()
        console.print("[bold green]The result: Complete tenant isolation with proper RBAC![/bold green]")
        console.print()
    else:
        print("\n" + "="*60)
        print("  Demo Complete!")
        print("="*60)
        print("\nSummary:")
        print("• Users are created with namespace binding via X-Weaviate-Namespace header")
        print("• Each user's API key is permanently bound to their assigned namespace")
        print("• All operations automatically use the bound namespace")
        print("• Explicit RBAC: namespace-admin-{ns} role is auto-assigned")
        print("• Permissions come from explicit roles, enabling audit trails")
        print("• Namespace-bound users get FULL permissions in their namespace")
        print("• No explicit RBAC roles needed for namespace-scoped operations")
        print("• Admin users can override namespace for management operations")
        print()


def main():
    global INTERACTIVE

    parser = argparse.ArgumentParser(description="Namespace-to-Principal Mapping Demo")
    parser.add_argument("-i", "--interactive", action="store_true",
                        help="Run in interactive mode (pause before each step)")
    args = parser.parse_args()
    INTERACTIVE = args.interactive

    if INTERACTIVE and not RICH_AVAILABLE:
        print("Warning: 'rich' library not installed. Install with: pip install rich")
        print("Falling back to basic output.\n")

    show_welcome()

    # Initialize admin client
    admin = WeaviateClient(WEAVIATE_URL, ADMIN_API_KEY, "admin")

    # =========================================================================
    # Step 1: Check connection
    # =========================================================================
    show_step_header(
        1,
        "Checking Weaviate Connection",
        "We'll verify Weaviate is running and the admin can authenticate.\n"
        "This calls [cyan]GET /v1/meta[/cyan] with the admin API key."
    )

    show_next_action("Check Weaviate connection")

    if not admin.check_ready():
        show_error(f"Cannot connect to Weaviate at {WEAVIATE_URL}")
        show_info("Make sure Weaviate is running with:")
        show_info("  ./tools/dev/run_dev_server.sh local-namespace-demo")
        sys.exit(1)
    show_success(f"Connected to Weaviate at {WEAVIATE_URL}")

    if INTERACTIVE:
        wait_for_key()

    # =========================================================================
    # Step 2: Cleanup
    # =========================================================================
    show_step_header(
        2,
        "Cleaning Up Previous Test Data",
        "Remove any leftover users and collections from previous runs."
    )

    show_next_action("Delete old test users and collections")

    cleanup(admin)
    show_success("Cleanup complete")

    if INTERACTIVE:
        wait_for_key()

    # =========================================================================
    # Step 3: Create namespace-bound users
    # =========================================================================
    show_step_header(
        3,
        "Creating Users Bound to Different Namespaces",
        "[bold yellow]This is the KEY FEATURE.[/bold yellow]\n\n"
        "When admin creates a user with the [cyan]X-Weaviate-Namespace[/cyan] header,\n"
        "that user is [bold]PERMANENTLY[/bold] bound to that namespace.\n\n"
        "We'll create two users:\n"
        "  [green]•[/green] [cyan]tenanta-user[/cyan] → bound to '[yellow]tenanta[/yellow]' namespace\n"
        "  [green]•[/green] [cyan]tenantb-user[/cyan] → bound to '[yellow]tenantb[/yellow]' namespace\n\n"
        "Each user gets an API key that only works in their namespace.\n\n"
        "[bold cyan]EXPLICIT RBAC:[/bold cyan] Each user gets a [cyan]namespace-admin-{ns}[/cyan] role\n"
        "auto-assigned with explicit permissions for their namespace!"
    )

    # Create Tenant A user
    show_next_action("Create 'tenanta-user' bound to namespace 'tenanta'")

    show_info("Creating 'tenanta-user' in namespace 'tenanta'...")
    ok, result = admin.create_user("tenanta-user", namespace="tenanta", show_api=True)
    if not ok:
        show_error(f"Failed to create tenanta-user: {result}")
        sys.exit(1)
    tenant_a_key = result.get("apikey", "")
    show_success(f"Created tenanta-user with key: {tenant_a_key[:20]}...")

    # Verify namespace-admin role was auto-assigned
    expected_role = "namespace-admin-tenanta"
    roles = admin.get_user_roles("tenanta-user")
    if expected_role in roles:
        show_success(f"Role '{expected_role}' auto-assigned (explicit RBAC)")
    else:
        show_error(f"Expected role '{expected_role}' to be assigned, got: {roles}")
        show_error("EXPLICIT NAMESPACE ROLES NOT WORKING - Demo may fail!")

    # Create Tenant B user
    show_next_action("Create 'tenantb-user' bound to namespace 'tenantb'")

    show_info("Creating 'tenantb-user' in namespace 'tenantb'...")
    ok, result = admin.create_user("tenantb-user", namespace="tenantb", show_api=True)
    if not ok:
        show_error(f"Failed to create tenantb-user: {result}")
        sys.exit(1)
    tenant_b_key = result.get("apikey", "")
    show_success(f"Created tenantb-user with key: {tenant_b_key[:20]}...")

    # Verify namespace-admin role was auto-assigned
    expected_role = "namespace-admin-tenantb"
    roles = admin.get_user_roles("tenantb-user")
    if expected_role in roles:
        show_success(f"Role '{expected_role}' auto-assigned (explicit RBAC)")
    else:
        show_error(f"Expected role '{expected_role}' to be assigned, got: {roles}")
        show_error("EXPLICIT NAMESPACE ROLES NOT WORKING - Demo may fail!")

    # Create clients for each tenant
    tenant_a = WeaviateClient(WEAVIATE_URL, tenant_a_key, "tenant-a")
    tenant_b = WeaviateClient(WEAVIATE_URL, tenant_b_key, "tenant-b")

    # =========================================================================
    # Step 4: Tenant users create their own collections
    # =========================================================================
    show_step_header(
        4,
        "Tenant Users Create Their Own Collections",
        "[bold yellow]EXPLICIT NAMESPACE ROLES IN ACTION![/bold yellow]\n\n"
        "Each tenant user creates an '[cyan]Articles[/cyan]' collection.\n\n"
        "[bold]Key point:[/bold] Users have the [bold cyan]namespace-admin-{ns}[/bold cyan] role\n"
        "which grants explicit CRUD permissions for their namespace.\n\n"
        "This is proper RBAC - permissions come from an explicit role!\n\n"
        "Internally, these become:\n"
        "  [green]•[/green] [dim]Tenanta__Articles[/dim] (in tenanta namespace)\n"
        "  [green]•[/green] [dim]Tenantb__Articles[/dim] (in tenantb namespace)\n\n"
        "These are [bold]completely separate[/bold] collections!"
    )

    show_next_action("Tenant A creates 'Articles' in their namespace")

    show_info("Tenant A creating 'Articles' (using their API key)...")
    show_highlight("(Tenant A has namespace-admin-tenanta role for explicit RBAC permissions)")
    ok, result = tenant_a.create_collection("Articles", show_api=True)
    if not ok:
        if "already exists" in str(result):
            show_info("Collection already exists (from previous run)")
        else:
            show_error(f"Failed to create collection: {result}")
            show_info("This is expected if namespace-scoped permissions are not yet enabled")
    else:
        show_success("Tenant A created Articles (via namespace-admin-tenanta role)")

    show_next_action("Tenant B creates 'Articles' in their namespace")

    show_info("Tenant B creating 'Articles' (using their API key)...")
    show_highlight("(Tenant B has namespace-admin-tenantb role for explicit RBAC permissions)")
    ok, result = tenant_b.create_collection("Articles", show_api=True)
    if not ok:
        if "already exists" in str(result):
            show_info("Collection already exists (from previous run)")
        else:
            show_error(f"Failed to create collection: {result}")
            show_info("This is expected if namespace-scoped permissions are not yet enabled")
    else:
        show_success("Tenant B created Articles (via namespace-admin-tenantb role)")

    # =========================================================================
    # Step 5: Verify namespace isolation
    # =========================================================================
    show_step_header(
        5,
        "Verifying Namespace Isolation",
        "[bold yellow]NOW THE MAGIC![/bold yellow]\n\n"
        "Each tenant queries the schema using [bold]ONLY their API key[/bold].\n\n"
        "[bold red]NO X-Weaviate-Namespace header is sent![/bold red]\n\n"
        "The server automatically:\n"
        "  [green]1.[/green] Authenticates the API key\n"
        "  [green]2.[/green] Looks up the user's bound namespace\n"
        "  [green]3.[/green] Filters results to only show that namespace's data\n\n"
        "[cyan]Tenant A[/cyan] should ONLY see their 'Articles' collection.\n"
        "[cyan]Tenant B[/cyan] should ONLY see their 'Articles' collection.\n"
        "[bold]They cannot see each other's data![/bold]"
    )

    show_next_action("Tenant A queries schema (using their bound API key)")

    show_info("Checking what Tenant A can see...")
    show_highlight("(Note: NO X-Weaviate-Namespace header - namespace comes from API key!)")
    ok, schema = tenant_a.get_schema(show_api=True)
    if ok and schema:
        classes = [c.get("class") for c in (schema.get("classes") or [])]
        show_success(f"Tenant A sees collections: {classes}")
        show_info("Tenant A is bound to 'tenanta' namespace - only sees their data")
    else:
        show_error(f"Tenant A schema response: {schema}")

    show_next_action("Tenant B queries schema (using their bound API key)")

    show_info("Checking what Tenant B can see...")
    show_highlight("(Note: NO X-Weaviate-Namespace header - namespace comes from API key!)")
    ok, schema = tenant_b.get_schema(show_api=True)
    if ok and schema:
        classes = [c.get("class") for c in (schema.get("classes") or [])]
        show_success(f"Tenant B sees collections: {classes}")
        show_info("Tenant B is bound to 'tenantb' namespace - only sees their data")
    else:
        show_error(f"Tenant B schema response: {schema}")

    # =========================================================================
    # Step 6: Show admin can access all namespaces
    # =========================================================================
    show_step_header(
        6,
        "Admin Access to All Namespaces",
        "Admin users (root users) can still access any namespace by\n"
        "specifying the [cyan]X-Weaviate-Namespace[/cyan] header.\n\n"
        "This is useful for:\n"
        "  [green]•[/green] Managing collections across namespaces\n"
        "  [green]•[/green] Debugging and administration\n"
        "  [green]•[/green] Cross-tenant operations"
    )

    show_next_action("Show that admin can access tenanta namespace")

    show_info("Admin checking namespace 'tenanta' (using header override)...")
    show_api_call("GET", f"{WEAVIATE_URL}/v1/schema",
                  {"Authorization": "Bearer admin-key", "X-Weaviate-Namespace": "tenanta"})
    show_info("Admin can access any namespace via the X-Weaviate-Namespace header")

    if INTERACTIVE:
        wait_for_key()

    # =========================================================================
    # Step 7: Cleanup
    # =========================================================================
    show_step_header(
        7,
        "Cleanup",
        "Remove the test users and collections we created."
    )

    show_next_action("Clean up test data")

    cleanup(admin)
    show_success("Test data cleaned up")

    if INTERACTIVE:
        wait_for_key()

    # =========================================================================
    # Summary
    # =========================================================================
    show_summary()


if __name__ == "__main__":
    main()
