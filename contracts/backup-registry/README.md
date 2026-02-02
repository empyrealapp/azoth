# BackupRegistry

On-chain registry that stores IPFS hashes for encrypted backups, indexed by user address and backup ID. Any address can submit multiple backups; each backup is identified by a `bytes32` ID (e.g. hash of database ID, timestamp).

## Build & test

From this directory:

```bash
forge build
forge test
```

Dependencies (forge-std, OpenZeppelin) are resolved from `../../lib/`; ensure submodules are initialized at the azoth repo root.

## Deployment

**Ethereum Sepolia:** `0xE70d7910aADC9c57Df4076E9616862930d96E130`
