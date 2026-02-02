// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Test} from "forge-std/Test.sol";
import {EIP712Validator} from "../src/libraries/EIP712Validator.sol";

contract EIP712ValidatorTest is Test {
    using EIP712Validator for EIP712Validator.DatabaseResponse;

    // Helper function to generate a test bytes32 databaseId
    function getTestDatabaseId() internal pure returns (bytes32) {
        return keccak256(abi.encodePacked("test-database"));
    }

    function test_GetDomainSeparator() public pure {
        string memory name = "TestContract";
        string memory version = "1";
        uint256 chainId = 11155111;
        address verifyingContract = address(0x123);

        bytes32 domainSeparator = EIP712Validator.getDomainSeparator(name, version, chainId, verifyingContract);

        // Domain separator should be non-zero
        assertNotEq(domainSeparator, bytes32(0));
    }

    function test_HashResponse() public pure {
        EIP712Validator.DatabaseResponse memory response = EIP712Validator.DatabaseResponse({
            databaseId: getTestDatabaseId(), query: "SELECT * FROM users", result: "0x1234", timestamp: 1700000000
        });

        bytes32 hash = EIP712Validator.hashResponse(response);

        // Hash should be non-zero
        assertNotEq(hash, bytes32(0));
    }

    function test_VerifySignature_Valid() public pure {
        uint256 privateKey = 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef;
        address signer = vm.addr(privateKey);

        bytes32 domainSeparator = EIP712Validator.getDomainSeparator("TestContract", "1", 11155111, address(0x123));

        EIP712Validator.DatabaseResponse memory response = EIP712Validator.DatabaseResponse({
            databaseId: getTestDatabaseId(), query: "SELECT * FROM users", result: "0x1234", timestamp: 1700000000
        });

        bytes32 structHash = EIP712Validator.hashResponse(response);
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", domainSeparator, structHash));

        (uint8 v, bytes32 r, bytes32 s) = vm.sign(privateKey, digest);
        bytes memory signature = abi.encodePacked(r, s, v);

        bool isValid = EIP712Validator.verifySignature(domainSeparator, response, signature, signer);

        assertTrue(isValid);
    }

    function test_VerifySignature_InvalidSigner() public pure {
        uint256 privateKey = 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef;
        address wrongSigner = address(0x999);

        string memory name = "TestContract";
        string memory version = "1";
        uint256 chainId = 11155111;
        address verifyingContract = address(0x123);

        bytes32 domainSeparator = EIP712Validator.getDomainSeparator(name, version, chainId, verifyingContract);

        EIP712Validator.DatabaseResponse memory response = EIP712Validator.DatabaseResponse({
            databaseId: getTestDatabaseId(), query: "SELECT * FROM users", result: "0x1234", timestamp: 1700000000
        });

        bytes32 structHash = EIP712Validator.hashResponse(response);
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", domainSeparator, structHash));

        (uint8 v, bytes32 r, bytes32 s) = vm.sign(privateKey, digest);
        bytes memory signature = abi.encodePacked(r, s, v);

        bool isValid = EIP712Validator.verifySignature(domainSeparator, response, signature, wrongSigner);

        assertFalse(isValid);
    }

    function test_VerifySignature_InvalidLength() public pure {
        bytes32 domainSeparator = EIP712Validator.getDomainSeparator("TestContract", "1", 11155111, address(0x123));

        EIP712Validator.DatabaseResponse memory response = EIP712Validator.DatabaseResponse({
            databaseId: getTestDatabaseId(), query: "SELECT * FROM users", result: "0x1234", timestamp: 1700000000
        });

        bytes memory invalidSignature = "0x1234"; // Too short

        bool isValid = EIP712Validator.verifySignature(domainSeparator, response, invalidSignature, address(0x1));

        assertFalse(isValid);
    }
}
