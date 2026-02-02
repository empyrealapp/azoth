// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import {Script, console} from "forge-std/Script.sol";
import {MockERC20} from "../src/mocks/MockERC20.sol";
import {UserVault} from "../src/UserVault.sol";

contract DeployScript is Script {
    function run() external {
        uint256 deployerPrivateKey = 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80;
        address deployer = vm.addr(deployerPrivateKey);
        
        vm.startBroadcast(deployerPrivateKey);
        
        // Deploy MockERC20 with 6 decimals (like real USDC)
        MockERC20 token = new MockERC20("Test USDC", "USDC", 6);
        console.log("MockERC20 deployed at:", address(token));
        
        // Deploy UserVault
        UserVault vault = new UserVault();
        console.log("UserVault deployed at:", address(vault));
        
        // Configure vault
        vault.setServerAddress(deployer);
        vault.whitelistToken(address(token));
        
        // Mint 1M USDC (6 decimals) to test accounts
        address[6] memory recipients = [
            0x70997970C51812dc3A010C7d01b50e0d17dc79C8,
            0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC,
            0x90F79bf6EB2c4f870365E785982E1f101E93b906,
            0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65,
            0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc,
            0xa0Ee7A142d267C1f36714E4a8F75612F20a79720
        ];
        
        for (uint i = 0; i < recipients.length; i++) {
            token.mint(recipients[i], 1_000_000 * 10**6);
            console.log("Minted 1M USDC to:", recipients[i]);
        }
        
        vm.stopBroadcast();
        
        console.log("");
        console.log("=== DEPLOYMENT COMPLETE ===");
        console.log("Token Address:", address(token));
        console.log("Vault Address:", address(vault));
    }
}
