#!/usr/bin/env python3
"""
PROPER geo deletion bug reproduction

The bug manifests when:
1. Objects are inserted at location A
2. Objects are deleted (but geo index not cleaned)
3. Objects are inserted at location B (DIFFERENT from A)
4. Query at location B → Returns nothing (orphaned docIDs from A)
"""

import weaviate
from weaviate.classes.config import Property, DataType
from weaviate.classes.query import Filter, GeoCoordinate
import sys
import time
import os

COLLECTION_NAME = "GeoDeleteBugTest"
STATE_FILE = "/tmp/weaviate_geo_test_state.txt"

def get_test_run():
    """Track which run this is"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            run = int(f.read().strip()) + 1
    else:
        run = 1

    with open(STATE_FILE, 'w') as f:
        f.write(str(run))

    return run

def reset_test():
    """Reset test state"""
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

def main():
    print("=" * 70)
    print("GEO DELETION BUG - PROPER REPRODUCTION")
    print("=" * 70)
    print()

    if len(sys.argv) > 1 and sys.argv[1] == "--reset":
        reset_test()
        print("Test state reset. Run without --reset to start testing.")
        return

    run_number = get_test_run()

    # Different coordinates for each run to expose the bug
    # Run 1: Insert at (0, 0), (1, 1), (2, 2), (3, 3), (4, 4)
    # Run 2: Insert at (10, 10), (11, 11), (12, 12), (13, 13), (14, 14)
    # Run 3: Insert at (20, 20), (21, 21), ...
    base_coord = (run_number - 1) * 10
    query_center = base_coord + 2  # Query near the center of inserted points

    print(f"RUN #{run_number}")
    print(f"Will insert objects at: ({base_coord}, {base_coord}) to ({base_coord+4}, {base_coord+4})")
    print(f"Will query at: ({query_center}, {query_center}) with 1000km radius")
    print()

    client = weaviate.connect_to_local()

    try:
        # Create collection on first run
        if not client.collections.exists(COLLECTION_NAME):
            print("Creating collection (first run)...")
            client.collections.create(
                name=COLLECTION_NAME,
                properties=[
                    Property(name="name", data_type=DataType.TEXT),
                    Property(name="location", data_type=DataType.GEO_COORDINATES),
                ]
            )
            print("✓ Collection created")
            print()
        else:
            print("Collection exists (reusing from previous run)")
            print()

        collection = client.collections.get(COLLECTION_NAME)

        # Check for existing data from previous run
        existing_objects = collection.query.fetch_objects(limit=100).objects
        if len(existing_objects) > 0:
            print(f"Found {len(existing_objects)} objects from previous run:")
            for obj in existing_objects[:5]:  # Show first 5
                loc = obj.properties['location']
                # GeoCoordinate object has .latitude and .longitude attributes
                print(f"  - {obj.properties['name']} at ({loc.latitude}, {loc.longitude})")
            if len(existing_objects) > 5:
                print(f"  ... and {len(existing_objects) - 5} more")
            print()

            # THIS IS CRITICAL: Delete old objects
            print(f"Deleting all {len(existing_objects)} objects...")
            for obj in existing_objects:
                collection.data.delete_by_id(obj.uuid)
            print("✓ Deleted all objects")
            print()

            # Wait for flush to disk (critical!)
            print("Waiting 5 seconds for geo index to flush to disk...")
            time.sleep(5)
            print()

            # Verify deletion
            remaining = len(collection.query.fetch_objects(limit=100).objects)
            if remaining > 0:
                print(f"⚠ WARNING: {remaining} objects still remain after deletion!")
            else:
                print("✓ Verified: All objects deleted")
            print()

        # Insert NEW objects at DIFFERENT coordinates
        print(f"Inserting 5 NEW objects at ({base_coord}, {base_coord}) to ({base_coord+4}, {base_coord+4})...")
        inserted_uuids = []
        for i in range(5):
            lat = float(base_coord + i)
            lon = float(base_coord + i)
            uuid = collection.data.insert(properties={
                "name": f"Run{run_number}_Point_{i}",
                "location": {"latitude": lat, "longitude": lon}
            })
            inserted_uuids.append(uuid)
            print(f"  ✓ Inserted Run{run_number}_Point_{i} at ({lat}, {lon}) -> {uuid}")
        print()

        # Give a moment for indexing
        print("Waiting 2 seconds for indexing...")
        time.sleep(2)
        print()

        # Query at the NEW location
        print(f"Querying within 1000km of ({query_center}, {query_center})...")
        print("This should find all 5 objects we just inserted.")
        print()

        results = collection.query.fetch_objects(
            filters=Filter.by_property("location").within_geo_range(
                coordinate=GeoCoordinate(latitude=float(query_center), longitude=float(query_center)),
                distance=1000 * 1000
            ),
            limit=100
        )

        print(f"Found {len(results.objects)} objects (expected 5)")
        print()

        if len(results.objects) > 0:
            print("Objects found:")
            for obj in results.objects:
                loc = obj.properties['location']
                # GeoCoordinate object has .latitude and .longitude attributes
                print(f"  - {obj.properties['name']} at ({loc.latitude}, {loc.longitude})")
        else:
            print("No objects found!")
        print()

        print("=" * 70)

        if run_number == 1:
            print("RESULT: RUN #1 BASELINE")
            print("=" * 70)
            print()
            if len(results.objects) == 5:
                print("✓ First run successful (5 objects found)")
                print()
                print("Next step:")
                print("  Run this script again WITHOUT restarting Docker")
                print("  to test the bug on the second run.")
            else:
                print("✗ First run failed - unexpected!")
                print(f"  Expected 5 objects, found {len(results.objects)}")
                return False

        else:
            print(f"RESULT: RUN #{run_number} - BUG TEST")
            print("=" * 70)
            print()

            if len(results.objects) == 5:
                # Check if the objects are the RIGHT ones (not orphaned from previous run)
                found_names = {obj.properties['name'] for obj in results.objects}
                expected_names = {f"Run{run_number}_Point_{i}" for i in range(5)}

                if found_names == expected_names:
                    print("✓ SUCCESS: Query returned the correct 5 objects!")
                    print("  The geo index cleanup is working properly.")
                    print("  (Or the bug doesn't manifest in this environment)")
                else:
                    print("⚠ PARTIAL BUG: Query returned 5 objects, but wrong ones!")
                    print(f"  Expected: {expected_names}")
                    print(f"  Found: {found_names}")
                    return False

            elif len(results.objects) == 0:
                print("✗ BUG REPRODUCED!")
                print(f"  Query at ({query_center}, {query_center}) returned 0 objects")
                print(f"  But we just inserted 5 objects at ({base_coord}+, {base_coord}+)")
                print()
                print("This confirms the geo index deletion bug:")
                print("  1. Orphaned docIDs from previous run remain in geo index")
                print("  2. New objects were inserted but query returns nothing")
                print("  3. The geo index on disk has stale state")
                print()
                print("The fix adds proper cleanup of geo indices during deletion.")
                return False

            else:
                # Partial results
                print(f"⚠ PARTIAL BUG: Query returned {len(results.objects)} objects instead of 5")
                print()

                # Check if any are from the current run
                current_run_objects = [obj for obj in results.objects if f"Run{run_number}" in obj.properties['name']]
                previous_run_objects = [obj for obj in results.objects if f"Run{run_number}" not in obj.properties['name']]

                if len(previous_run_objects) > 0:
                    print(f"  Found {len(previous_run_objects)} orphaned objects from previous runs!")
                    print("  This indicates stale geo index entries.")

                if len(current_run_objects) > 0:
                    print(f"  Found {len(current_run_objects)} objects from current run")

                return False

        print()
        print("To reset and start over: python test_geo_bug_proper.py --reset")
        return True

    finally:
        client.close()

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
