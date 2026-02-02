// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Script, console} from "forge-std/Script.sol";
import {MockERC20} from "../src/mocks/MockERC20.sol";

/**
 * @title DeployMockERC20
 * @notice Deployment script for MockERC20 token (for local testing)
 */
contract DeployMockERC20 is Script {
    function run() external returns (address creditTokenAddress) {
        address deployer = msg.sender;
        console.log("Deploying MockERC20 with deployer:", deployer);

        vm.startBroadcast();

        // Deploy MockERC20 token
        MockERC20 creditToken = new MockERC20("Credit Token", "CREDIT");
        creditTokenAddress = address(creditToken);

        console.log("MockERC20 deployed at:", creditTokenAddress);

        // Mint some tokens to deployer for testing
        // Mint enough for: test mnemonic (1M) + user wallet (1M) + buffer = 3M tokens
        uint256 mintAmount = 3000000 ether; // 3M tokens
        creditToken.mint(deployer, mintAmount);
        console.log("Minted", mintAmount, "tokens to deployer");

        vm.stopBroadcast();

        return creditTokenAddress;
    }
}
