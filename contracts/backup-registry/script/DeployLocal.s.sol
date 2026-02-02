// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Script, console} from "forge-std/Script.sol";
import {UserVault} from "../src/UserVault.sol";
import {BackupRegistry} from "../src/BackupRegistry.sol";
import {MockERC20} from "../src/mocks/MockERC20.sol";

/**
 * @title DeployLocal
 * @notice Deployment script for local development contracts
 * @dev Deploys UserVault and BackupRegistry for the game server
 *
 * Usage:
 *   forge script script/DeployLocal.s.sol:DeployLocal --rpc-url http://localhost:8545 --private-key <PRIVATE_KEY> --broadcast
 *
 * Environment variables (optional):
 *   GAME_TOKEN_ADDRESS - Address of the ERC20 game token (if not provided, deploys MockERC20)
 *   SERVER_ADDRESS - Address of the server that will manage balances (required)
 */
contract DeployLocal is Script {
    function run()
        external
        returns (address userBalanceVaultAddress, address backupRegistryAddress, address gameTokenAddress)
    {
        // Get deployer address
        address deployer = msg.sender;
        console.log("Deploying contracts with deployer:", deployer);

        // Get server address from environment (default to deployer if not set)
        address serverAddress = vm.envOr("SERVER_ADDRESS", deployer);

        vm.startBroadcast();

        // 1. Deploy or use existing game token
        address tokenAddress;
        bool isNewToken = false;
        try vm.envAddress("GAME_TOKEN_ADDRESS") returns (address existingToken) {
            tokenAddress = existingToken;
            console.log("Using existing game token at:", tokenAddress);
        } catch {
            // Deploy MockERC20 if no token address provided
            console.log("\n1. Deploying MockERC20 game token...");
            MockERC20 gameToken = new MockERC20("Game Token", "GAME");
            tokenAddress = address(gameToken);
            console.log("   MockERC20 deployed at:", tokenAddress);
            isNewToken = true;

            // Mint tokens to test account for demo
            address testAccount = 0xa0Ee7A142d267C1f36714E4a8F75612F20a79720;
            uint256 testAmount = 1000000 ether; // 1M tokens for testing
            gameToken.mint(testAccount, testAmount);
            console.log("   Minted", testAmount / 1e18, "tokens to test account:", testAccount);
        }
        gameTokenAddress = tokenAddress;

        // 2. Deploy UserVault
        console.log("\n2. Deploying UserVault...");
        UserVault vault = new UserVault();
        userBalanceVaultAddress = address(vault);
        console.log("   UserVault deployed at:", userBalanceVaultAddress);

        // 3. Set server address on vault
        console.log("\n3. Setting server address on UserVault...");
        vault.setServerAddress(serverAddress);
        console.log("   Server address set to:", serverAddress);

        // 4. Whitelist the game token (if provided)
        if (tokenAddress != address(0)) {
            console.log("\n4. Whitelisting game token on UserVault...");
            vault.whitelistToken(tokenAddress);
            console.log("   Token whitelisted:", tokenAddress);
        }

        // 5. Deploy BackupRegistry
        console.log("\n5. Deploying BackupRegistry...");
        BackupRegistry backupRegistry = new BackupRegistry();
        backupRegistryAddress = address(backupRegistry);
        console.log("   BackupRegistry deployed at:", backupRegistryAddress);

        vm.stopBroadcast();

        // Print deployment summary
        console.log("\n=== Deployment Summary ===");
        console.log("Game Token:", gameTokenAddress);
        console.log("UserVault:", userBalanceVaultAddress);
        console.log("BackupRegistry:", backupRegistryAddress);
        console.log("Server Address:", serverAddress);
        console.log("\nAll contracts deployed and configured successfully!");
    }
}

