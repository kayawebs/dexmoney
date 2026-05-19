// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

interface IERC20V2 {
    function balanceOf(address account) external view returns (uint256);
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 amount) external returns (bool);
    function transfer(address to, uint256 amount) external returns (bool);
}

interface IAerodromeClassicRouterV2 {
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

interface IAerodromeClassicFactoryV2 {
    function getPool(address tokenA, address tokenB, bool stable) external view returns (address pool);
}

interface IV3RouterV2 {
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

interface IV3FactoryV2 {
    function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address pool);
}

interface ISlipstreamRouterV2 {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        int24 tickSpacing;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);
}

interface ISlipstreamFactoryV2 {
    function getPool(address tokenA, address tokenB, int24 tickSpacing) external view returns (address pool);
}

contract ExecutorV2 {
    enum DexKind {
        AerodromeClassic,
        AerodromeSlipstream,
        UniswapV3
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

    event OperatorUpdated(address indexed operator, bool allowed);
    event Paused(bool status);
    event ApprovalSet(address indexed token, address indexed spender, uint256 amount);
    event EmergencyWithdraw(address indexed token, address indexed to, uint256 amount);
    event Executed(address indexed caller, address indexed tokenIn, uint256 amountIn, uint256 profit);

    error OnlyOwner();
    error OnlyOperator();
    error PausedError();
    error DeadlineExpired();
    error InvalidPath();
    error InvalidStepCount();
    error InsufficientBalance();
    error InsufficientAllowance();
    error UnsupportedDex();
    error InvalidTickSpacing();
    error PoolMismatch();
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

    function setPaused(bool value) external onlyOwner {
        paused = value;
        emit Paused(value);
    }

    function approveToken(address token, address spender, uint256 amount) external onlyOwner {
        if (!IERC20V2(token).approve(spender, amount)) revert ApprovalFailed();
        emit ApprovalSet(token, spender, amount);
    }

    function emergencyWithdraw(address token, address to, uint256 amount) external onlyOwner {
        if (!IERC20V2(token).transfer(to, amount)) revert TransferFailed();
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

        uint256 balanceBefore = IERC20V2(tokenIn).balanceOf(address(this));
        if (balanceBefore < amountIn) revert InsufficientBalance();

        address currentToken = tokenIn;
        uint256 currentAmount = amountIn;

        for (uint256 i = 0; i < 2; i++) {
            SwapStep calldata step = steps[i];
            if (step.tokenIn != currentToken) revert InvalidPath();
            currentAmount = _swap(step, currentAmount, deadline);
            currentToken = step.tokenOut;
        }

        if (currentToken != tokenIn) revert InvalidPath();

        uint256 balanceAfter = IERC20V2(tokenIn).balanceOf(address(this));
        if (balanceAfter <= balanceBefore) revert MinProfitNotMet();

        profit = balanceAfter - balanceBefore;
        if (profit < minProfit) revert MinProfitNotMet();

        emit Executed(msg.sender, tokenIn, amountIn, profit);
    }

    function _swap(SwapStep calldata step, uint256 amountIn, uint256 deadline) internal returns (uint256 amountOut) {
        _validatePool(step);
        if (IERC20V2(step.tokenIn).allowance(address(this), step.router) < amountIn) {
            revert InsufficientAllowance();
        }

        if (step.dex == DexKind.AerodromeClassic) {
            IAerodromeClassicRouterV2.Route[] memory routes = new IAerodromeClassicRouterV2.Route[](1);
            routes[0] = IAerodromeClassicRouterV2.Route({
                from: step.tokenIn, to: step.tokenOut, stable: step.stable, factory: step.factory
            });
            uint256[] memory amounts = IAerodromeClassicRouterV2(step.router)
                .swapExactTokensForTokens(amountIn, 0, routes, address(this), deadline);
            amountOut = amounts[amounts.length - 1];
        } else if (step.dex == DexKind.AerodromeSlipstream) {
            int24 tickSpacing = _decodeTickSpacing(step.fee);
            amountOut = ISlipstreamRouterV2(step.router)
                .exactInputSingle(
                    ISlipstreamRouterV2.ExactInputSingleParams({
                        tokenIn: step.tokenIn,
                        tokenOut: step.tokenOut,
                        tickSpacing: tickSpacing,
                        recipient: address(this),
                        deadline: deadline,
                        amountIn: amountIn,
                        amountOutMinimum: 0,
                        sqrtPriceLimitX96: 0
                    })
                );
        } else if (step.dex == DexKind.UniswapV3) {
            amountOut = IV3RouterV2(step.router)
                .exactInputSingle(
                    IV3RouterV2.ExactInputSingleParams({
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

    function _validatePool(SwapStep calldata step) internal view {
        if (step.factory == address(0) || step.pool == address(0)) return;

        address expectedPool;
        if (step.dex == DexKind.AerodromeClassic) {
            expectedPool = IAerodromeClassicFactoryV2(step.factory).getPool(step.tokenIn, step.tokenOut, step.stable);
        } else if (step.dex == DexKind.AerodromeSlipstream) {
            expectedPool =
                ISlipstreamFactoryV2(step.factory).getPool(step.tokenIn, step.tokenOut, _decodeTickSpacing(step.fee));
        } else if (step.dex == DexKind.UniswapV3) {
            expectedPool = IV3FactoryV2(step.factory).getPool(step.tokenIn, step.tokenOut, step.fee);
        } else {
            revert UnsupportedDex();
        }

        if (expectedPool != step.pool) revert PoolMismatch();
    }

    function _decodeTickSpacing(uint24 value) internal pure returns (int24) {
        if (value == 0 || value > uint24(type(int24).max)) revert InvalidTickSpacing();
        // forge-lint: disable-next-line(unsafe-typecast)
        return int24(int256(uint256(value)));
    }
}
