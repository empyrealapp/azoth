#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}Publishing Azoth crates to crates.io${NC}"
echo ""

# Function to publish a crate and wait
publish_crate() {
    local crate=$1
    echo -e "${BLUE}Publishing ${crate}...${NC}"
    cargo publish -p "$crate"
    echo -e "${GREEN}✓ ${crate} published${NC}"
    echo ""

    # Wait for crates.io to index the package
    if [ "$crate" != "azoth-cli" ]; then
        echo "Waiting 30 seconds for crates.io to index..."
        sleep 30
    fi
}

# 1. Publish core
echo -e "${BLUE}Step 1/6: Publishing core crate${NC}"
publish_crate "azoth-core"

# 2. Publish file-log, sqlite, and projector (only depend on core)
echo -e "${BLUE}Step 2/6: Publishing file-log, sqlite, and projector${NC}"
publish_crate "azoth-file-log"
publish_crate "azoth-sqlite"
publish_crate "azoth-projector"

# 3. Publish lmdb (depends on core and file-log)
echo -e "${BLUE}Step 3/6: Publishing lmdb${NC}"
publish_crate "azoth-lmdb"

# 4. Publish main crate
echo -e "${BLUE}Step 4/6: Publishing main crate${NC}"
publish_crate "azoth"

# 5. Publish vector (depends on core and sqlite)
echo -e "${BLUE}Step 5/6: Publishing vector${NC}"
publish_crate "azoth-vector"

# 6. Publish scheduler, bus, and CLI (depend on azoth)
echo -e "${BLUE}Step 6/6: Publishing scheduler, bus, and CLI${NC}"
publish_crate "azoth-scheduler"
publish_crate "azoth-bus"
publish_crate "azoth-cli"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}All crates published successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Published crates:"
echo "  - azoth-core v0.1.1"
echo "  - azoth-file-log v0.1.1"
echo "  - azoth-sqlite v0.1.1"
echo "  - azoth-projector v0.1.1"
echo "  - azoth-lmdb v0.1.1"
echo "  - azoth v0.1.1"
echo "  - azoth-vector v0.1.1"
echo "  - azoth-scheduler v0.1.1"
echo "  - azoth-bus v0.1.1"
echo "  - azoth-cli v0.1.1"
