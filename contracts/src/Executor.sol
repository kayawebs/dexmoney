// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

interface IERC20 {
    function balanceOf(address account) external view returns (uint256);
    function transfer(address to, uint256 amount) external returns (bool);
}

contract Executor {
    enum DexKind {
        Aerodrome,
        UniswapV3
    }

    struct SwapStep {
        DexKind dex;
        address router;
        address pool;
        address tokenIn;
        address tokenOut;
        uint24 fee;
        bytes data;
    }

    address public owner;
    bool public paused;

    mapping(address => bool) public operators;
    mapping(address => bool) public tokenWhitelist;
    mapping(address => bool) public routerWhitelist;
    mapping(address => bool) public poolWhitelist;

    event OperatorUpdated(address indexed operator, bool allowed);
    event TokenWhitelistUpdated(address indexed token, bool allowed);
    event RouterWhitelistUpdated(address indexed router, bool allowed);
    event PoolWhitelistUpdated(address indexed pool, bool allowed);
    event Paused(bool status);
    event EmergencyWithdraw(address indexed token, address indexed to, uint256 amount);
    event Executed(address indexed caller, address indexed tokenIn, uint256 amountIn, uint256 profit);

    error OnlyOwner();
    error OnlyOperator();
    error PausedError();
    error DeadlineExpired();
    error TokenNotWhitelisted();
    error RouterNotWhitelisted();
    error PoolNotWhitelisted();
    error MinProfitNotMet();

    modifier onlyOwner() {
        if (msg.sender != owner) revert OnlyOwner();
        _;
    }

    modifier onlyOperator() {
        if (!operators[msg.sender]) revert OnlyOperator();
        _;
    }

    modifier whenNotPaused() {
        if (paused) revert PausedError();
        _;
    }

    constructor(address initialOwner) {
        owner = initialOwner;
        operators[initialOwner] = true;
    }

    function setOperator(address operator, bool allowed) external onlyOwner {
        operators[operator] = allowed;
        emit OperatorUpdated(operator, allowed);
    }

    function setTokenWhitelist(address token, bool allowed) external onlyOwner {
        tokenWhitelist[token] = allowed;
        emit TokenWhitelistUpdated(token, allowed);
    }

    function setRouterWhitelist(address router, bool allowed) external onlyOwner {
        routerWhitelist[router] = allowed;
        emit RouterWhitelistUpdated(router, allowed);
    }

    function setPoolWhitelist(address pool, bool allowed) external onlyOwner {
        poolWhitelist[pool] = allowed;
        emit PoolWhitelistUpdated(pool, allowed);
    }

    function setPaused(bool value) external onlyOwner {
        paused = value;
        emit Paused(value);
    }

    function emergencyWithdraw(address token, address to, uint256 amount) external onlyOwner {
        IERC20(token).transfer(to, amount);
        emit EmergencyWithdraw(token, to, amount);
    }

    function executeWithOwnFunds(
        address tokenIn,
        uint256 amountIn,
        SwapStep[] calldata steps,
        uint256 minProfit,
        uint256 deadline
    ) external onlyOperator whenNotPaused returns (uint256 finalBalance) {
        if (block.timestamp > deadline) revert DeadlineExpired();
        if (!tokenWhitelist[tokenIn]) revert TokenNotWhitelisted();

        uint256 balanceBefore = IERC20(tokenIn).balanceOf(address(this));
        require(balanceBefore >= amountIn, "insufficient balance");

        for (uint256 i = 0; i < steps.length; i++) {
            if (!routerWhitelist[steps[i].router]) revert RouterNotWhitelisted();
            if (!poolWhitelist[steps[i].pool]) revert PoolNotWhitelisted();

            // Router interaction is intentionally left as a skeleton.
            // Concrete Aerodrome and Uniswap V3 calls are added after calldata flow is finalized.
        }

        finalBalance = IERC20(tokenIn).balanceOf(address(this));
        uint256 profit = finalBalance > balanceBefore ? finalBalance - balanceBefore : 0;
        if (profit < minProfit) revert MinProfitNotMet();

        emit Executed(msg.sender, tokenIn, amountIn, profit);
    }
}

