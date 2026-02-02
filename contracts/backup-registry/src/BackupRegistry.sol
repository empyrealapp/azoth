// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title BackupRegistry
 * @notice Stores IPFS hashes for encrypted backups, indexed by user address and backup ID
 * @dev General-purpose registry that allows any address to store multiple backups
 *      Each backup is identified by a bytes32 backup ID (e.g., hash of database ID, timestamp, etc.)
 */
contract BackupRegistry {
    // Mapping: user address => backup ID (bytes32) => IPFS hash
    mapping(address => mapping(bytes32 => string)) public backups;

    // Mapping: user address => array of backup IDs they've created
    mapping(address => bytes32[]) public userBackupIds;

    // Mapping: user address => backup ID => timestamp when submitted
    mapping(address => mapping(bytes32 => uint256)) public backupTimestamps;

    event BackupSubmitted(address indexed user, bytes32 indexed backupId, string ipfsHash, uint256 timestamp);

    constructor() {
        // No constructor parameters needed - anyone can use this registry
    }

    /**
     * @notice Submit a new backup IPFS hash
     * @param backupId Unique identifier for this backup (bytes32)
     * @param ipfsHash IPFS hash of the encrypted backup file
     */
    function submitBackup(bytes32 backupId, string calldata ipfsHash) external {
        require(bytes(ipfsHash).length > 0, "IPFS hash cannot be empty");
        require(backupId != bytes32(0), "Backup ID cannot be zero");

        address user = msg.sender;
        uint256 timestamp = block.timestamp;

        // Only add to array if this is a new backup ID for this user
        if (bytes(backups[user][backupId]).length == 0) {
            userBackupIds[user].push(backupId);
        }

        backups[user][backupId] = ipfsHash;
        backupTimestamps[user][backupId] = timestamp;

        emit BackupSubmitted(user, backupId, ipfsHash, timestamp);
    }

    /**
     * @notice Get backup for a specific user and backup ID
     * @param user Address of the user
     * @param backupId Backup ID (bytes32)
     * @return ipfsHash IPFS hash of the backup
     * @return timestamp Timestamp when the backup was submitted
     */
    function getBackup(address user, bytes32 backupId)
        external
        view
        returns (string memory ipfsHash, uint256 timestamp)
    {
        ipfsHash = backups[user][backupId];
        timestamp = backupTimestamps[user][backupId];
    }

    /**
     * @notice Get all backup IDs for a user
     * @param user Address of the user
     * @return Array of backup IDs (bytes32)
     */
    function getUserBackupIds(address user) external view returns (bytes32[] memory) {
        return userBackupIds[user];
    }

    /**
     * @notice Get the number of backups for a user
     * @param user Address of the user
     * @return Number of backups
     */
    function getUserBackupCount(address user) external view returns (uint256) {
        return userBackupIds[user].length;
    }
}
