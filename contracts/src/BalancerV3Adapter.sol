// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

interface IERC20BalancerAdapter {
    function balanceOf(address account) external view returns (uint256);
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 amount) external returns (bool);
    function transfer(address to, uint256 amount) external returns (bool);
}

interface IBalancerV3RouterAdapter {
    function swapSingleTokenExactIn(
        address pool,
        IERC20BalancerAdapter tokenIn,
        IERC20BalancerAdapter tokenOut,
        uint256 exactAmountIn,
        uint256 minAmountOut,
        uint256 deadline,
        bool wethIsEth,
        bytes calldata userData
    ) external payable returns (uint256 amountOut);
}

/// @notice ExecutorHub adapter for Balancer V3 single-pool exact-input swaps.
/// @dev ExecutorHub transfers tokenIn into this adapter before calling swap().
contract BalancerV3Adapter {
    error UnauthorizedCaller();
    error InvalidRouter();
    error InvalidPool();
    error InvalidToken();
    error ApprovalFailed();
    error TransferFailed();
    error NoOutput();

    address public immutable hub;
    address public immutable router;

    constructor(address hub_, address router_) {
        if (hub_ == address(0) || router_ == address(0)) revert InvalidRouter();
        hub = hub_;
        router = router_;
    }

    function swap(
        address pool,
        address tokenIn,
        address tokenOut,
        uint24,
        bool,
        address,
        uint256 amountIn,
        address recipient,
        bytes calldata data
    ) external returns (uint256 amountOut) {
        if (msg.sender != hub) revert UnauthorizedCaller();
        if (pool == address(0)) revert InvalidPool();
        if (tokenIn == address(0) || tokenOut == address(0)) revert InvalidToken();

        (uint256 minAmountOut, uint256 deadline, bytes memory userData) = _decodeData(data);
        _approveIfNeeded(tokenIn, router, amountIn);

        amountOut = IBalancerV3RouterAdapter(router).swapSingleTokenExactIn(
            pool,
            IERC20BalancerAdapter(tokenIn),
            IERC20BalancerAdapter(tokenOut),
            amountIn,
            minAmountOut,
            deadline,
            false,
            userData
        );
        if (amountOut == 0) revert NoOutput();
        _safeTransfer(tokenOut, recipient, amountOut);
    }

    function _decodeData(bytes calldata data)
        internal
        view
        returns (uint256 minAmountOut, uint256 deadline, bytes memory userData)
    {
        if (data.length == 0) {
            return (0, block.timestamp + 30, bytes(""));
        }
        return abi.decode(data, (uint256, uint256, bytes));
    }

    function _approveIfNeeded(address token, address spender, uint256 amount) internal {
        if (IERC20BalancerAdapter(token).allowance(address(this), spender) >= amount) return;
        if (!IERC20BalancerAdapter(token).approve(spender, type(uint256).max)) revert ApprovalFailed();
    }

    function _safeTransfer(address token, address to, uint256 amount) internal {
        if (!IERC20BalancerAdapter(token).transfer(to, amount)) revert TransferFailed();
    }
}
