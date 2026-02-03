#!/usr/bin/env python3
"""
Namespace POC Demo Script

This script demonstrates the namespace functionality in Weaviate.
Collections are internally prefixed with the namespace (e.g., "myapp__Article"),
but the API transparently handles this - clients only see "Article".

Prerequisites:
- Weaviate running on localhost:8080
- pip install requests

Usage:
    python namespace_demo.py
"""

import requests
import json
import sys

BASE_URL = "http://localhost:8080/v1"

def print_header(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")

def print_response(response, description=""):
    if description:
        print(f">> {description}")
    print(f"Status: {response.status_code}")
    try:
        print(f"Response: {json.dumps(response.json(), indent=2)}")
    except:
        print(f"Response: {response.text}")
    print()

def create_class(namespace, class_name, description=""):
    """Create a collection in a specific namespace."""
    headers = {"X-Weaviate-Namespace": namespace, "Content-Type": "application/json"}
    payload = {
        "class": class_name,
        "description": description or f"A {class_name} in namespace '{namespace}'",
        "properties": [
            {"name": "title", "dataType": ["text"]},
            {"name": "content", "dataType": ["text"]}
        ]
    }
    return requests.post(f"{BASE_URL}/schema", headers=headers, json=payload)

def get_schema(namespace):
    """Get schema for a specific namespace."""
    headers = {"X-Weaviate-Namespace": namespace}
    return requests.get(f"{BASE_URL}/schema", headers=headers)

def delete_class(namespace, class_name):
    """Delete a collection from a specific namespace."""
    headers = {"X-Weaviate-Namespace": namespace}
    return requests.delete(f"{BASE_URL}/schema/{class_name}", headers=headers)

def batch_create_objects(namespace, objects):
    """Batch create objects in a specific namespace."""
    headers = {"X-Weaviate-Namespace": namespace, "Content-Type": "application/json"}
    payload = {"objects": objects}
    return requests.post(f"{BASE_URL}/batch/objects", headers=headers, json=payload)

def list_objects(namespace, class_name=None):
    """List objects from a specific namespace."""
    headers = {"X-Weaviate-Namespace": namespace}
    url = f"{BASE_URL}/objects"
    if class_name:
        url += f"?class={class_name}"
    return requests.get(url, headers=headers)

def get_object(namespace, class_name, object_id):
    """Get a specific object."""
    headers = {"X-Weaviate-Namespace": namespace}
    return requests.get(f"{BASE_URL}/objects/{class_name}/{object_id}", headers=headers)

def cleanup(namespaces_and_classes):
    """Clean up created classes."""
    print_header("Cleanup")
    for ns, classes in namespaces_and_classes.items():
        for cls in classes:
            resp = delete_class(ns, cls)
            print(f"Deleted {ns}::{cls}: {resp.status_code}")

def main():
    print_header("Weaviate Namespace POC Demo")
    print("This demo shows how namespaces isolate collections and objects.")
    print("The X-Weaviate-Namespace header determines which namespace to use.")
    print("Default namespace is 'default' when header is not provided.")

    # Track what we create for cleanup
    created = {"teamalpha": ["Article"], "teambeta": ["Article", "Document"]}

    try:
        # ============================================================
        # Step 1: Create collections in different namespaces
        # ============================================================
        print_header("Step 1: Create Collections in Different Namespaces")

        print("Creating 'Article' in namespace 'teamalpha'...")
        resp = create_class("teamalpha", "Article", "Articles for Team Alpha")
        print_response(resp)

        print("Creating 'Article' in namespace 'teambeta' (same name, different namespace)...")
        resp = create_class("teambeta", "Article", "Articles for Team Beta")
        print_response(resp)

        print("Creating 'Document' in namespace 'teambeta'...")
        resp = create_class("teambeta", "Document", "Documents for Team Beta")
        print_response(resp)

        # ============================================================
        # Step 2: List schema for each namespace
        # ============================================================
        print_header("Step 2: List Schema per Namespace (Isolation Demo)")

        print("Schema for namespace 'teamalpha' (should only show 1 Article):")
        resp = get_schema("teamalpha")
        print_response(resp)

        print("Schema for namespace 'teambeta' (should show Article + Document):")
        resp = get_schema("teambeta")
        print_response(resp)

        print("Schema for namespace 'default' (should be empty):")
        resp = get_schema("default")
        print_response(resp)

        # ============================================================
        # Step 3: Batch insert objects
        # ============================================================
        print_header("Step 3: Batch Insert Objects")

        alpha_objects = [
            {"class": "Article", "properties": {"title": "Alpha News 1", "content": "Content from team alpha"}},
            {"class": "Article", "properties": {"title": "Alpha News 2", "content": "More content from alpha"}},
        ]
        print("Inserting 2 articles into 'teamalpha' namespace...")
        resp = batch_create_objects("teamalpha", alpha_objects)
        print_response(resp)

        beta_objects = [
            {"class": "Article", "properties": {"title": "Beta Article", "content": "Content from team beta"}},
            {"class": "Document", "properties": {"title": "Beta Doc", "content": "A document from beta"}},
        ]
        print("Inserting 1 article + 1 document into 'teambeta' namespace...")
        resp = batch_create_objects("teambeta", beta_objects)
        print_response(resp)

        # ============================================================
        # Step 4: Query objects per namespace
        # ============================================================
        print_header("Step 4: Query Objects per Namespace (Isolation Demo)")

        print("Listing Article objects in 'teamalpha' (should show 2):")
        resp = list_objects("teamalpha", "Article")
        print_response(resp)

        print("Listing Article objects in 'teambeta' (should show 1):")
        resp = list_objects("teambeta", "Article")
        print_response(resp)

        print("Listing Document objects in 'teambeta' (should show 1):")
        resp = list_objects("teambeta", "Document")
        print_response(resp)

        # ============================================================
        # Step 5: Cross-namespace isolation
        # ============================================================
        print_header("Step 5: Cross-Namespace Isolation")

        print("Trying to access 'Document' from 'teamalpha' (should fail - doesn't exist):")
        resp = list_objects("teamalpha", "Document")
        print_response(resp)

        # ============================================================
        # Summary
        # ============================================================
        print_header("Summary")
        print("""
Key Points Demonstrated:
------------------------
1. Same class name ('Article') can exist in multiple namespaces
2. Each namespace only sees its own collections in schema listing
3. Objects are isolated per namespace
4. The X-Weaviate-Namespace header controls which namespace is used
5. Default namespace is 'default' when header is omitted

Internal Implementation:
------------------------
- Collections are stored as 'namespace__ClassName' internally
- e.g., 'teamalpha__Article' and 'teambeta__Article'
- The API layer handles prefix/strip transparently
- Clients never see the internal prefixed names
""")

    except requests.exceptions.ConnectionError:
        print("\nERROR: Could not connect to Weaviate at localhost:8080")
        print("Make sure Weaviate is running before executing this demo.")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: {e}")
        raise
    finally:
        # Cleanup
        cleanup(created)

if __name__ == "__main__":
    main()
