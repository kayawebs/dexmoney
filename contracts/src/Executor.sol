// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

interface IERC20 {
    function balanceOf(address account) external view returns (uint256);
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 amount) external returns (bool);
    function transfer(address to, uint256 amount) external returns (bool);
}

interface IAerodromeRouter {
    struct Route {
        address from;
        address to;
        bool stable;
        address factory;
    }

    function swapExactTokensForTokens(
        uint256 amountIn,
        uint256 amountOutMin,
        Route[] calldata routes,
        address to,
        uint256 deadline
    ) external returns (uint256[] memory amounts);
}

interface IUniswapV3Router {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);
}

interface IPancakeV3Router {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);
}

contract Executor {
    enum DexKind {
        AerodromeClassic,
        AerodromeSlipstream,
        UniswapV3,
        PancakeV3
    }

    struct SwapStep {
        DexKind dex;
        address router;
        address pool;
        address tokenIn;
        address tokenOut;
        uint24 fee;
        bool stable;
        address factory;
    }

    address public owner;
    bool public paused;

    mapping(address => bool) public operators;
    mapping(address => bool) public tokenWhitelist;
    mapping(address => bool) public routerWhitelist;
    mapping(address => bool) public poolWhitelist;
    mapping(address => bool) public factoryWhitelist;

    event OperatorUpdated(address indexed operator, bool allowed);
    event TokenWhitelistUpdated(address indexed token, bool allowed);
    event RouterWhitelistUpdated(address indexed router, bool allowed);
    event PoolWhitelistUpdated(address indexed pool, bool allowed);
    event FactoryWhitelistUpdated(address indexed factory, bool allowed);
    event Paused(bool status);
    event ApprovalSet(address indexed token, address indexed spender, uint256 amount);
    event EmergencyWithdraw(address indexed token, address indexed to, uint256 amount);
    event Executed(address indexed caller, address indexed tokenIn, uint256 amountIn, uint256 profit);

    error OnlyOwner();
    error OnlyOperator();
    error PausedError();
    error DeadlineExpired();
    error TokenNotWhitelisted();
    error RouterNotWhitelisted();
    error PoolNotWhitelisted();
    error FactoryNotWhitelisted();
    error InvalidPath();
    error InvalidStepCount();
    error InsufficientBalance();
    error InsufficientAllowance();
    error UnsupportedDex();
    error MinProfitNotMet();
    error TransferFailed();
    error ApprovalFailed();

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

    function setFactoryWhitelist(address factory, bool allowed) external onlyOwner {
        factoryWhitelist[factory] = allowed;
        emit FactoryWhitelistUpdated(factory, allowed);
    }

    function setPaused(bool value) external onlyOwner {
        paused = value;
        emit Paused(value);
    }

    function approveToken(address token, address spender, uint256 amount) external onlyOwner {
        if (!IERC20(token).approve(spender, amount)) revert ApprovalFailed();
        emit ApprovalSet(token, spender, amount);
    }

    function emergencyWithdraw(address token, address to, uint256 amount) external onlyOwner {
        if (!IERC20(token).transfer(to, amount)) revert TransferFailed();
        emit EmergencyWithdraw(token, to, amount);
    }

    function executeWithOwnFunds(
        address tokenIn,
        uint256 amountIn,
        SwapStep[] calldata steps,
        uint256 minProfit,
        uint256 deadline
    ) external onlyOperator whenNotPaused returns (uint256 profit) {
        if (block.timestamp > deadline) revert DeadlineExpired();
        if (steps.length != 2) revert InvalidStepCount();
        if (!tokenWhitelist[tokenIn]) revert TokenNotWhitelisted();

        uint256 balanceBefore = IERC20(tokenIn).balanceOf(address(this));
        if (balanceBefore < amountIn) revert InsufficientBalance();

        address currentToken = tokenIn;
        uint256 currentAmount = amountIn;

        for (uint256 i = 0; i < 2; i++) {
            SwapStep calldata step = steps[i];
            _validateStep(step, currentToken);
            currentAmount = _swap(step, currentAmount, deadline);
            currentToken = step.tokenOut;
        }

        if (currentToken != tokenIn) revert InvalidPath();

        uint256 balanceAfter = IERC20(tokenIn).balanceOf(address(this));
        if (balanceAfter <= balanceBefore) revert MinProfitNotMet();

        profit = balanceAfter - balanceBefore;
        if (profit < minProfit) revert MinProfitNotMet();

        emit Executed(msg.sender, tokenIn, amountIn, profit);
    }

    function _validateStep(SwapStep calldata step, address expectedTokenIn) internal view {
        if (step.tokenIn != expectedTokenIn) revert InvalidPath();
        if (!tokenWhitelist[step.tokenIn] || !tokenWhitelist[step.tokenOut]) revert TokenNotWhitelisted();
        if (!routerWhitelist[step.router]) revert RouterNotWhitelisted();
        if (!poolWhitelist[step.pool]) revert PoolNotWhitelisted();

        if (step.dex == DexKind.AerodromeClassic && !factoryWhitelist[step.factory]) {
            revert FactoryNotWhitelisted();
        }
    }

    function _swap(SwapStep calldata step, uint256 amountIn, uint256 deadline) internal returns (uint256 amountOut) {
        if (IERC20(step.tokenIn).allowance(address(this), step.router) < amountIn) {
            revert InsufficientAllowance();
        }

        if (step.dex == DexKind.AerodromeClassic) {
            IAerodromeRouter.Route[] memory routes = new IAerodromeRouter.Route[](1);
            routes[0] = IAerodromeRouter.Route({
                from: step.tokenIn, to: step.tokenOut, stable: step.stable, factory: step.factory
            });
            uint256[] memory amounts =
                IAerodromeRouter(step.router).swapExactTokensForTokens(amountIn, 0, routes, address(this), deadline);
            amountOut = amounts[amounts.length - 1];
        } else if (step.dex == DexKind.UniswapV3) {
            amountOut = IUniswapV3Router(step.router)
                .exactInputSingle(
                    IUniswapV3Router.ExactInputSingleParams({
                        tokenIn: step.tokenIn,
                        tokenOut: step.tokenOut,
                        fee: step.fee,
                        recipient: address(this),
                        amountIn: amountIn,
                        amountOutMinimum: 0,
                        sqrtPriceLimitX96: 0
                    })
                );
        } else if (step.dex == DexKind.PancakeV3) {
            amountOut = IPancakeV3Router(step.router)
                .exactInputSingle(
                    IPancakeV3Router.ExactInputSingleParams({
                        tokenIn: step.tokenIn,
                        tokenOut: step.tokenOut,
                        fee: step.fee,
                        recipient: address(this),
                        deadline: deadline,
                        amountIn: amountIn,
                        amountOutMinimum: 0,
                        sqrtPriceLimitX96: 0
                    })
                );
        } else {
            revert UnsupportedDex();
        }
    }
}
