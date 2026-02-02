# Deployment Scripts

This directory contains deployment scripts for the Arcana game platform contracts.

## DeployLocal.s.sol

Deployment script for local development and testing. Deploys UserVault and BackupRegistry for the game server.

### Usage

Deploy all contracts to a local network (e.g., Anvil):

```bash
forge script script/DeployLocal.s.sol:DeployLocal \
  --rpc-url http://localhost:8545 \
  --private-key <PRIVATE_KEY> \
  --broadcast
```

### Environment Variables (Optional)

- `GAME_TOKEN_ADDRESS` - Address of an existing ERC20 game token. If not provided, the script will deploy a MockERC20 token.
- `SERVER_ADDRESS` - Address of the server that will manage balances. Defaults to the deployer address if not set.

### Deployment Order

The script deploys contracts in the following order:

1. **Game Token** - Deploys MockERC20 if `GAME_TOKEN_ADDRESS` is not provided, or uses the existing token address
2. **UserVault** - Manages user deposits and withdrawals for game balances (supports multiple whitelisted tokens)
3. **Server Address Setup** - Sets the server address on UserVault
4. **Token Whitelisting** - Whitelists the game token on UserVault (required for deposits)
5. **BackupRegistry** - Stores IPFS hashes for database backups

### Example: Local Development

```bash
# Start Anvil in another terminal
anvil

# Deploy contracts (in another terminal)
forge script script/DeployLocal.s.sol:DeployLocal \
  --rpc-url http://localhost:8545 \
  --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
  --broadcast
```

### Example: Using Existing Token

```bash
# Set environment variables
export GAME_TOKEN_ADDRESS=0x1234567890123456789012345678901234567890
export SERVER_ADDRESS=0xabcdefabcdefabcdefabcdefabcdefabcdefabcd

# Deploy to a testnet
forge script script/DeployLocal.s.sol:DeployLocal \
  --rpc-url https://rpc.sepolia.org \
  --private-key $PRIVATE_KEY \
  --broadcast \
  --verify
```

### Verification

To verify contracts on Etherscan/Arbiscan after deployment:

```bash
forge script script/DeployLocal.s.sol:DeployLocal \
  --rpc-url <RPC_URL> \
  --private-key <PRIVATE_KEY> \
  --broadcast \
  --verify \
  --etherscan-api-key <API_KEY>
```

### Local Testing (Simulation)

To test the deployment script locally without broadcasting transactions:

```bash
forge script script/DeployLocal.s.sol:DeployLocal \
  --rpc-url http://localhost:8545 \
  --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
```

This will simulate the deployment without actually sending transactions.

### Important Notes

- **Token Whitelisting**: After deployment, the game token is automatically whitelisted on UserVault. Additional tokens can be whitelisted by the server using the `whitelistToken` function.
- **Server Address**: The server address can only be set once. Make sure to set it correctly during initial deployment.
- **Multi-Token Support**: UserVault supports multiple whitelisted tokens. The server manages which tokens are allowed for deposits via the bank configuration.
