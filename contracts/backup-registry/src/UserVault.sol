// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {IERC20} from "forge-std/interfaces/IERC20.sol";

/**
 * @title UserVault
 * @notice Manages user deposits and withdrawals for game balances
 * @dev Users deposit tokens to this vault to play games. The server monitors Deposit events
 *      and updates balances in its database. The server can withdraw tokens on behalf of users
 *      via serverWithdraw. Only whitelisted tokens can be deposited. Withdrawals work for all tokens
 *      regardless of whitelist status, ensuring funds are never trapped if a token is removed from
 *      the whitelist. All balance tracking is done off-chain by the server.
 */
contract UserVault {
    // Server address that can update balances for game outcomes and whitelist tokens
    address public serverAddress;

    // Counter for unique deposit IDs
    uint256 private depositCounter;

    uint256 public constant MAX_DEPOSIT_SIZE = 10000000000000000000000000000000000; // 1e34

    // Mapping of whitelisted token addresses
    mapping(address => bool) public whitelistedTokens;

    // Array to track all whitelisted tokens (for enumeration)
    address[] public whitelistedTokensList;

    // Maximum number of whitelisted tokens
    uint256 public constant MAX_WHITELISTED_TOKENS = 30;

    event Deposit(address indexed user, address indexed token, uint256 amount, bytes32 indexed depositId);
    event Withdrawal(address indexed user, address indexed token, uint256 amount);
    event TokenWhitelisted(address indexed token);
    event TokenRemovedFromWhitelist(address indexed token);

    constructor() {
        // No longer requires a token in constructor
        // Tokens must be whitelisted by the server after deployment
    }

    /**
     * @notice Set the server address (can only be set once)
     * @param _serverAddress The server address that can update balances
     */
    function setServerAddress(address _serverAddress) external {
        require(serverAddress == address(0), "Server address already set");
        require(_serverAddress != address(0), "Invalid server address");
        serverAddress = _serverAddress;
    }

    /**
     * @notice Whitelist a token for deposits (server-only)
     * @param token Address of the token to whitelist
     */
    function whitelistToken(address token) external {
        require(msg.sender == serverAddress, "Only server can whitelist tokens");
        require(token != address(0), "Invalid token address");
        require(!whitelistedTokens[token], "Token already whitelisted");
        require(whitelistedTokensList.length < MAX_WHITELISTED_TOKENS, "Maximum whitelisted tokens reached");

        whitelistedTokens[token] = true;
        whitelistedTokensList.push(token);
        emit TokenWhitelisted(token);
    }

    /**
     * @notice Remove a token from the whitelist (server-only)
     * @param token Address of the token to remove from whitelist
     */
    function removeWhitelistToken(address token) external {
        require(msg.sender == serverAddress, "Only server can remove whitelist tokens");
        require(token != address(0), "Invalid token address");
        require(whitelistedTokens[token], "Token not whitelisted");

        whitelistedTokens[token] = false;

        // Remove from array by finding and swapping with last element, then popping
        for (uint256 i = 0; i < whitelistedTokensList.length; i++) {
            if (whitelistedTokensList[i] == token) {
                // Swap with last element
                whitelistedTokensList[i] = whitelistedTokensList[whitelistedTokensList.length - 1];
                // Remove last element
                whitelistedTokensList.pop();
                break;
            }
        }

        emit TokenRemovedFromWhitelist(token);
    }

    /**
     * @notice Get the number of whitelisted tokens
     * @return count Number of whitelisted tokens
     */
    function getWhitelistedTokensCount() external view returns (uint256) {
        return whitelistedTokensList.length;
    }

    /**
     * @notice Get all whitelisted token addresses
     * @return tokens Array of whitelisted token addresses
     */
    function getWhitelistedTokens() external view returns (address[] memory) {
        return whitelistedTokensList;
    }

    /**
     * @notice Deposit tokens to user's game balance
     * @param token Address of the token to deposit
     * @param amount Amount of tokens to deposit
     */
    function deposit(address token, uint256 amount) external {
        require(amount > 0, "Amount must be greater than 0");
        require(whitelistedTokens[token], "Token not whitelisted");
        require(amount <= MAX_DEPOSIT_SIZE, "Amount exceeds maximum deposit size");

        IERC20 tokenContract = IERC20(token);
        require(tokenContract.transferFrom(msg.sender, address(this), amount), "Transfer failed");

        // Generate unique deposit ID
        depositCounter++;
        bytes32 depositId =
            keccak256(abi.encodePacked(msg.sender, token, amount, depositCounter, block.timestamp, block.number));

        emit Deposit(msg.sender, token, amount, depositId);
    }

    /**
     * @notice Withdraw tokens from user's game balance to their wallet (server-only)
     * @param token Address of the token to withdraw
     * @param user User address to withdraw for
     * @param amount Amount of tokens to withdraw
     * @dev Only the server can call this to withdraw funds to a user's wallet.
     *      The vault must have sufficient balance. The transfer will revert if insufficient.
     */
    function serverWithdraw(address token, address user, uint256 amount) external {
        require(msg.sender == serverAddress, "Only server can withdraw");
        require(amount > 0, "Amount must be greater than 0");
        require(user != address(0), "Invalid user address");
        require(token != address(0), "Invalid token address");

        IERC20 tokenContract = IERC20(token);

        // Check that vault has sufficient balance
        uint256 vaultBalance = tokenContract.balanceOf(address(this));
        require(vaultBalance >= amount, "Insufficient vault balance");

        require(tokenContract.transfer(user, amount), "Transfer failed");

        emit Withdrawal(user, token, amount);
    }

    /**
     * @notice Batch withdraw tokens for multiple users (server-only)
     * @param token Address of the token to withdraw
     * @param users Array of user addresses to withdraw for
     * @param amounts Array of amounts to withdraw (must match users array length)
     * @dev Only the server can call this. Processes multiple withdrawals in a single transaction.
     *      Useful for batch processing withdrawals to save gas.
     */
    function serverWithdrawBatch(address token, address[] calldata users, uint256[] calldata amounts) external {
        require(msg.sender == serverAddress, "Only server can withdraw");
        require(token != address(0), "Invalid token address");
        require(users.length == amounts.length, "Arrays length mismatch");
        require(users.length > 0, "Empty arrays");

        IERC20 tokenContract = IERC20(token);

        uint256 totalAmount = 0;
        for (uint256 i = 0; i < amounts.length; i++) {
            totalAmount += amounts[i];
        }

        // Check that vault has sufficient balance for all withdrawals
        uint256 vaultBalance = tokenContract.balanceOf(address(this));
        require(vaultBalance >= totalAmount, "Insufficient vault balance");

        // Process all withdrawals
        for (uint256 i = 0; i < users.length; i++) {
            require(amounts[i] > 0, "Amount must be greater than 0");
            require(users[i] != address(0), "Invalid user address");
            require(tokenContract.transfer(users[i], amounts[i]), "Transfer failed");
            emit Withdrawal(users[i], token, amounts[i]);
        }
    }
}
