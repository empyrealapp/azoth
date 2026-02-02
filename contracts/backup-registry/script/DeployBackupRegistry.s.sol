// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Script, console} from "forge-std/Script.sol";
import {BackupRegistry} from "../src/BackupRegistry.sol";

contract DeployBackupRegistryScript is Script {
    function run() external {
        // Get private key from environment variable
        uint256 deployerPrivateKey = vm.envUint("PRIVATE_KEY");

        vm.startBroadcast(deployerPrivateKey);

        // Deploy BackupRegistry
        BackupRegistry registry = new BackupRegistry();
        console.log("BackupRegistry deployed at:", address(registry));

        vm.stopBroadcast();

        console.log("");
        console.log("=== DEPLOYMENT COMPLETE ===");
        console.log("BackupRegistry Address:", address(registry));
        console.log("");
        console.log("Add this to your .env file:");
        console.log("BACKUP_REGISTRY_ADDRESS=%s", address(registry));
    }
}
