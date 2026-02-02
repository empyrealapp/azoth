// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title EIP712Validator
 * @notice Library for validating EIP-712 signatures of server responses
 * @dev Allows users to verify server responses onchain before pushing them
 */
library EIP712Validator {
    bytes32 public constant DOMAIN_TYPEHASH =
        keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");

    bytes32 public constant RESPONSE_TYPEHASH =
        keccak256("DatabaseResponse(bytes32 databaseId,string query,bytes result,uint256 timestamp)");

    struct DatabaseResponse {
        bytes32 databaseId;
        string query;
        bytes result;
        uint256 timestamp;
    }

    /**
     * @notice Get the domain separator for EIP-712
     * @param name Contract name
     * @param version Contract version
     * @param chainId Chain ID
     * @param verifyingContract Contract address
     * @return domainSeparator The domain separator
     */
    function getDomainSeparator(string memory name, string memory version, uint256 chainId, address verifyingContract)
        internal
        pure
        returns (bytes32)
    {
        return keccak256(
            abi.encode(DOMAIN_TYPEHASH, keccak256(bytes(name)), keccak256(bytes(version)), chainId, verifyingContract)
        );
    }

    /**
     * @notice Hash a DatabaseResponse struct
     * @param response The response struct
     * @return hash The hash of the response
     */
    function hashResponse(DatabaseResponse memory response) internal pure returns (bytes32) {
        return keccak256(
            abi.encode(
                RESPONSE_TYPEHASH,
                response.databaseId,
                keccak256(bytes(response.query)),
                keccak256(response.result),
                response.timestamp
            )
        );
    }

    /**
     * @notice Verify an EIP-712 signature
     * @param domainSeparator The domain separator
     * @param response The response struct
     * @param signature The signature to verify
     * @param signer The expected signer address
     * @return isValid True if signature is valid
     */
    function verifySignature(
        bytes32 domainSeparator,
        DatabaseResponse memory response,
        bytes memory signature,
        address signer
    ) internal pure returns (bool) {
        bytes32 structHash = hashResponse(response);
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", domainSeparator, structHash));

        bytes32 r;
        bytes32 s;
        uint8 v;

        if (signature.length == 65) {
            assembly {
                r := mload(add(signature, 32))
                s := mload(add(signature, 64))
                v := byte(0, mload(add(signature, 96)))
            }
        } else {
            return false;
        }

        if (v < 27) {
            v += 27;
        }

        if (v != 27 && v != 28) {
            return false;
        }

        address recovered = ecrecover(digest, v, r, s);
        return recovered == signer && recovered != address(0);
    }
}
