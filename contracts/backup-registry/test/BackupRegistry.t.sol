// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Test} from "forge-std/Test.sol";
import {BackupRegistry} from "../src/BackupRegistry.sol";

contract BackupRegistryTest is Test {
    BackupRegistry public backupRegistry;

    address public user1 = address(0x1);
    address public user2 = address(0x2);
    string public constant IPFS_HASH = "QmTestHash123456789";
    string public constant IPFS_HASH2 = "QmTestHash987654321";

    function setUp() public {
        backupRegistry = new BackupRegistry();
    }

    function test_SubmitBackup() public {
        bytes32 backupId = keccak256("test-backup-1");

        vm.expectEmit(true, true, false, true);
        emit BackupRegistry.BackupSubmitted(user1, backupId, IPFS_HASH, block.timestamp);

        vm.prank(user1);
        backupRegistry.submitBackup(backupId, IPFS_HASH);

        (string memory storedHash, uint256 timestamp) = backupRegistry.getBackup(user1, backupId);
        assertEq(storedHash, IPFS_HASH);
        assertGt(timestamp, 0);
    }

    function test_SubmitBackup_MultipleUsers() public {
        bytes32 backupId1 = keccak256("backup-1");
        bytes32 backupId2 = keccak256("backup-2");

        vm.prank(user1);
        backupRegistry.submitBackup(backupId1, IPFS_HASH);

        vm.prank(user2);
        backupRegistry.submitBackup(backupId2, IPFS_HASH2);

        (string memory hash1,) = backupRegistry.getBackup(user1, backupId1);
        (string memory hash2,) = backupRegistry.getBackup(user2, backupId2);

        assertEq(hash1, IPFS_HASH);
        assertEq(hash2, IPFS_HASH2);
    }

    function test_SubmitBackup_MultipleBackupsPerUser() public {
        bytes32 backupId1 = keccak256("backup-1");
        bytes32 backupId2 = keccak256("backup-2");

        vm.startPrank(user1);
        backupRegistry.submitBackup(backupId1, IPFS_HASH);
        backupRegistry.submitBackup(backupId2, IPFS_HASH2);
        vm.stopPrank();

        (string memory hash1,) = backupRegistry.getBackup(user1, backupId1);
        (string memory hash2,) = backupRegistry.getBackup(user1, backupId2);

        assertEq(hash1, IPFS_HASH);
        assertEq(hash2, IPFS_HASH2);

        uint256 count = backupRegistry.getUserBackupCount(user1);
        assertEq(count, 2);
    }

    function test_SubmitBackup_Overwrite() public {
        bytes32 backupId = keccak256("backup-1");

        vm.startPrank(user1);
        backupRegistry.submitBackup(backupId, IPFS_HASH);
        uint256 timestamp1 = backupRegistry.backupTimestamps(user1, backupId);

        // Overwrite with new hash
        backupRegistry.submitBackup(backupId, IPFS_HASH2);
        vm.stopPrank();

        (string memory storedHash, uint256 timestamp2) = backupRegistry.getBackup(user1, backupId);
        assertEq(storedHash, IPFS_HASH2);
        assertGe(timestamp2, timestamp1);

        // Should still only have one backup ID (not duplicated)
        uint256 count = backupRegistry.getUserBackupCount(user1);
        assertEq(count, 1);
    }

    function test_SubmitBackupFails_EmptyHash() public {
        bytes32 backupId = keccak256("backup-1");
        vm.expectRevert("IPFS hash cannot be empty");
        vm.prank(user1);
        backupRegistry.submitBackup(backupId, "");
    }

    function test_SubmitBackupFails_ZeroBackupId() public {
        vm.expectRevert("Backup ID cannot be zero");
        vm.prank(user1);
        backupRegistry.submitBackup(bytes32(0), IPFS_HASH);
    }

    function test_GetUserBackupIds() public {
        bytes32 backupId1 = keccak256("backup-1");
        bytes32 backupId2 = keccak256("backup-2");
        bytes32 backupId3 = keccak256("backup-3");

        vm.startPrank(user1);
        backupRegistry.submitBackup(backupId1, IPFS_HASH);
        backupRegistry.submitBackup(backupId2, IPFS_HASH);
        backupRegistry.submitBackup(backupId3, IPFS_HASH);
        vm.stopPrank();

        bytes32[] memory ids = backupRegistry.getUserBackupIds(user1);
        assertEq(ids.length, 3);
        assertEq(ids[0], backupId1);
        assertEq(ids[1], backupId2);
        assertEq(ids[2], backupId3);
    }

    function test_GetUserBackupCount() public {
        bytes32 backupId1 = keccak256("backup-1");
        bytes32 backupId2 = keccak256("backup-2");

        assertEq(backupRegistry.getUserBackupCount(user1), 0);

        vm.startPrank(user1);
        backupRegistry.submitBackup(backupId1, IPFS_HASH);
        assertEq(backupRegistry.getUserBackupCount(user1), 1);

        backupRegistry.submitBackup(backupId2, IPFS_HASH);
        assertEq(backupRegistry.getUserBackupCount(user1), 2);
        vm.stopPrank();
    }

    function test_GetBackup_NonExistent() public view {
        bytes32 backupId = keccak256("non-existent");
        (string memory hash, uint256 timestamp) = backupRegistry.getBackup(user1, backupId);
        assertEq(bytes(hash).length, 0);
        assertEq(timestamp, 0);
    }
}
