#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CARGO_TOML="$WORKSPACE_DIR/Cargo.toml"

# Usage help
usage() {
    echo "Usage: $0 [patch|minor|major|<version>]"
    echo ""
    echo "Arguments:"
    echo "  patch       Bump patch version (0.1.1 -> 0.1.2)"
    echo "  minor       Bump minor version (0.1.1 -> 0.2.0)"
    echo "  major       Bump major version (0.1.1 -> 1.0.0)"
    echo "  <version>   Set explicit version (e.g., 0.2.0)"
    echo ""
    echo "If no argument provided, defaults to 'patch'"
    exit 1
}

# Get current version from workspace Cargo.toml
get_current_version() {
    grep -E '^version = "[0-9]+\.[0-9]+\.[0-9]+"' "$CARGO_TOML" | head -1 | sed 's/version = "\(.*\)"/\1/'
}

# Parse version into components
parse_version() {
    local version=$1
    MAJOR=$(echo "$version" | cut -d. -f1)
    MINOR=$(echo "$version" | cut -d. -f2)
    PATCH=$(echo "$version" | cut -d. -f3)
}

# Bump version based on type
bump_version() {
    local current=$1
    local bump_type=$2
    
    parse_version "$current"
    
    case $bump_type in
        patch)
            PATCH=$((PATCH + 1))
            ;;
        minor)
            MINOR=$((MINOR + 1))
            PATCH=0
            ;;
        major)
            MAJOR=$((MAJOR + 1))
            MINOR=0
            PATCH=0
            ;;
        *)
            # Assume it's an explicit version
            if [[ $bump_type =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
                echo "$bump_type"
                return
            else
                echo -e "${RED}Error: Invalid version format '$bump_type'${NC}" >&2
                usage
            fi
            ;;
    esac
    
    echo "$MAJOR.$MINOR.$PATCH"
}

# Update version in workspace Cargo.toml
update_cargo_toml() {
    local new_version=$1
    
    echo -e "${BLUE}Updating Cargo.toml versions...${NC}"
    
    # Update workspace.package.version (first version = line in [workspace.package] section)
    sed -i.bak 's/^\(version = "\)[0-9]*\.[0-9]*\.[0-9]*"/\1'"$new_version"'"/' "$CARGO_TOML"
    
    # Update all azoth-* workspace dependency versions
    # Match pattern: azoth-* = { version = "x.y.z", path = ...
    sed -i.bak 's/\(azoth[a-z-]* = { version = "\)[0-9]*\.[0-9]*\.[0-9]*"/\1'"$new_version"'"/g' "$CARGO_TOML"
    
    # Clean up backup file
    rm -f "$CARGO_TOML.bak"
    
    echo -e "${GREEN}✓ Updated all versions to $new_version${NC}"
}

# Function to publish a crate and wait
publish_crate() {
    local crate=$1
    local is_last=$2
    
    echo -e "${BLUE}Publishing ${crate}...${NC}"
    
    # Run from workspace directory
    (cd "$WORKSPACE_DIR" && cargo publish -p "$crate")
    
    echo -e "${GREEN}✓ ${crate} published${NC}"
    echo ""
    
    # Wait for crates.io to index the package (skip for last crate)
    if [ "$is_last" != "true" ]; then
        echo "Waiting 30 seconds for crates.io to index..."
        sleep 30
    fi
}

# Main script
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Azoth Crates Publisher${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Parse arguments
BUMP_TYPE="${1:-patch}"

if [ "$BUMP_TYPE" = "-h" ] || [ "$BUMP_TYPE" = "--help" ]; then
    usage
fi

# Get current version
CURRENT_VERSION=$(get_current_version)
if [ -z "$CURRENT_VERSION" ]; then
    echo -e "${RED}Error: Could not read current version from $CARGO_TOML${NC}"
    exit 1
fi

echo -e "Current version: ${YELLOW}$CURRENT_VERSION${NC}"

# Calculate new version
NEW_VERSION=$(bump_version "$CURRENT_VERSION" "$BUMP_TYPE")

echo -e "New version:     ${GREEN}$NEW_VERSION${NC}"
echo ""

# Confirm with user
read -p "Proceed with publishing v$NEW_VERSION? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

echo ""

# Update Cargo.toml
update_cargo_toml "$NEW_VERSION"

# Commit the version bump
echo -e "${BLUE}Committing version bump...${NC}"
(cd "$WORKSPACE_DIR" && git add Cargo.toml && git commit -m "chore: bump version to $NEW_VERSION")
echo -e "${GREEN}✓ Version bump committed${NC}"
echo ""

# List of crates in dependency order
CRATES=(
    "azoth-core"
    "azoth-file-log"
    "azoth-sqlite"
    "azoth-projector"
    "azoth-lmdb"
    "azoth"
    "azoth-vector"
    "azoth-scheduler"
    "azoth-bus"
    "azoth-cli"
)

echo -e "${BLUE}Publishing ${#CRATES[@]} crates to crates.io${NC}"
echo ""

# Publish each crate
LAST_INDEX=$((${#CRATES[@]} - 1))
for i in "${!CRATES[@]}"; do
    crate="${CRATES[$i]}"
    step=$((i + 1))
    
    echo -e "${BLUE}Step $step/${#CRATES[@]}: Publishing ${crate}${NC}"
    
    if [ "$i" -eq "$LAST_INDEX" ]; then
        publish_crate "$crate" "true"
    else
        publish_crate "$crate" "false"
    fi
done

# Create and push git tag
echo -e "${BLUE}Creating git tag v$NEW_VERSION...${NC}"
(cd "$WORKSPACE_DIR" && git tag -a "v$NEW_VERSION" -m "Release v$NEW_VERSION")
echo -e "${GREEN}✓ Tag created${NC}"
echo ""

echo -e "${BLUE}Pushing changes and tag to origin...${NC}"
(cd "$WORKSPACE_DIR" && git push && git push --tags)
echo -e "${GREEN}✓ Pushed to origin${NC}"
echo ""

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}All crates published successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Published crates (v$NEW_VERSION):"
for crate in "${CRATES[@]}"; do
    echo "  - $crate v$NEW_VERSION"
done
echo ""
echo -e "View on crates.io: ${BLUE}https://crates.io/crates/azoth${NC}"
