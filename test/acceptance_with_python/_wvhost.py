#                           _       _
# __      _____  __ ___   ___  __ _| |_ ___
# \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
#  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
#   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
#
#  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
#
#  CONTACT: hello@weaviate.io
#

"""Resolve Weaviate REST and gRPC endpoints for acceptance tests.

Defaults are localhost:8080 / localhost:50051. WV_TEST_HOST,
WV_TEST_REST_PORT, and WV_TEST_GRPC_PORT override the non-RBAC instance.
WV_TEST_RBAC_HOST, WV_TEST_RBAC_REST_PORT, and WV_TEST_RBAC_GRPC_PORT
override the RBAC/auth instance (defaults localhost:8081 / 50052).

Mirrors test/acceptance_with_go_client/internal/wvhost/wvhost.go so the
Go and Python suites share a single configuration convention.
"""

import os


def host() -> str:
    return os.environ.get("WV_TEST_HOST") or "localhost"


def rest_port() -> int:
    return int(os.environ.get("WV_TEST_REST_PORT") or "8080")


def grpc_port() -> int:
    return int(os.environ.get("WV_TEST_GRPC_PORT") or "50051")


def rest_url() -> str:
    return f"http://{host()}:{rest_port()}"


def rbac_host() -> str:
    return os.environ.get("WV_TEST_RBAC_HOST") or "localhost"


def rbac_rest_port() -> int:
    return int(os.environ.get("WV_TEST_RBAC_REST_PORT") or "8081")


def rbac_grpc_port() -> int:
    return int(os.environ.get("WV_TEST_RBAC_GRPC_PORT") or "50052")


def rbac_rest_url() -> str:
    return f"http://{rbac_host()}:{rbac_rest_port()}"
