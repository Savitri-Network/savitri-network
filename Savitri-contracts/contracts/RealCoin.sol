// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @title RealCoin (bytes32-account token)
/// @notice Real Solidity bytecode token used for Savitri RPC E2E checks.
/// @dev Uses bytes32 accounts so it can map directly to Savitri 32-byte addresses.
contract RealCoin {
    mapping(bytes32 => uint256) private balances;
    uint256 public totalSupply;

    function balanceOf(bytes32 account) external view returns (uint256) {
        return balances[account];
    }

    /// @notice Demo mint endpoint for testnet/devnet usage.
    function faucetMint(bytes32 to, uint256 amount) external {
        unchecked {
            balances[to] += amount;
            totalSupply += amount;
        }
    }

    function transferCoin(bytes32 to, uint256 amount) external returns (bool) {
        bytes32 sender;
        assembly {
            sender := caller()
        }

        uint256 fromBalance = balances[sender];
        require(fromBalance >= amount);

        unchecked {
            balances[sender] = fromBalance - amount;
            balances[to] += amount;
        }

        return true;
    }
}
