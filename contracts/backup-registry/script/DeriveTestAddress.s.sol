// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Script, console} from "forge-std/Script.sol";

/**
 * @title DeriveTestAddress
 * @notice Derives the address from the test mnemonic using standard BIP39/BIP44 derivation
 * @dev Uses standard Ethereum derivation path m/44'/60'/0'/0/0 (index 0) to match ServerWallet
 */
contract DeriveTestAddress is Script {
    function run() external view returns (address testAddress) {
        // Test mnemonic: "test test test test test test test test test test test junk"
        string memory mnemonic = "test test test test test test test test test test test junk";

        // Derive private key from mnemonic using standard BIP39/BIP44 derivation
        // Path: m/44'/60'/0'/0/0 (standard Ethereum derivation, index 0)
        // This matches the server's derivation method
        uint256 privateKey = vm.deriveKey(mnemonic, 0);

        // Get address from private key
        testAddress = vm.addr(privateKey);

        console.log("Test mnemonic address:", testAddress);
    }
}
