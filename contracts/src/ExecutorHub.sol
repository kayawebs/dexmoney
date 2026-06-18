// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

interface IERC20Hub {
    function balanceOf(address account) external view returns (uint256);
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 amount) external returns (bool);
    function transfer(address to, uint256 amount) external returns (bool);
}

interface IAerodromeClassicRouterHub {
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

interface IAerodromeClassicFactoryHub {
    function getPool(address tokenA, address tokenB, bool stable) external view returns (address pool);
}

interface IV3RouterHub {
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

interface IPancakeV3RouterHub {
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

interface IV3FactoryHub {
    function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address pool);
}

interface ISlipstreamRouterHub {
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

interface ISlipstreamFactoryHub {
    function getPool(address tokenA, address tokenB, int24 tickSpacing) external view returns (address pool);
}

interface IV2PairHub {
    function token0() external view returns (address);
    function token1() external view returns (address);
    function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
    function swap(uint256 amount0Out, uint256 amount1Out, address to, bytes calldata data) external;
}

interface IV3PoolHub {
    function token0() external view returns (address);
    function token1() external view returns (address);
    function fee() external view returns (uint24);
    function swap(address recipient, bool zeroForOne, int256 amountSpecified, uint160 sqrtPriceLimitX96, bytes calldata data)
        external
        returns (int256 amount0, int256 amount1);
}

interface IExecutorAdapterHub {
    function swap(
        address pool,
        address tokenIn,
        address tokenOut,
        uint24 fee,
        bool stable,
        address factory,
        uint256 amountIn,
        address recipient,
        bytes calldata data
    ) external returns (uint256 amountOut);
}

contract ExecutorHub {
    enum StepKind {
        AerodromeClassic,
        AerodromeSlipstream,
        UniswapV3,
        PancakeV3,
        DirectV2,
        DirectV3,
        Adapter
    }

    struct SwapStep {
        StepKind dex;
        address router;
        address pool;
        address tokenIn;
        address tokenOut;
        uint24 fee;
        bool stable;
        address factory;
        bytes data;
    }

    struct V3CallbackData {
        address pool;
        address tokenIn;
    }

    uint160 internal constant MIN_SQRT_RATIO_PLUS_ONE = 4295128740;
    uint160 internal constant MAX_SQRT_RATIO_MINUS_ONE = 1461446703485210103287273052203988822378723970341;

    address public owner;
    bool public paused;

    mapping(address => bool) public operators;
    mapping(address => bool) public adapters;

    event OperatorUpdated(address indexed operator, bool allowed);
    event AdapterUpdated(address indexed adapter, bool allowed);
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
    error InvalidFee();
    error PoolMismatch();
    error MinProfitNotMet();
    error TransferFailed();
    error ApprovalFailed();
    error UnauthorizedCallback();
    error InvalidCallback();
    error BalanceDidNotIncrease();
    error AdapterNotWhitelisted();

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

    function setAdapter(address adapter, bool allowed) external onlyOwner {
        adapters[adapter] = allowed;
        emit AdapterUpdated(adapter, allowed);
    }

    function setPaused(bool value) external onlyOwner {
        paused = value;
        emit Paused(value);
    }

    function approveToken(address token, address spender, uint256 amount) external onlyOwner {
        if (!IERC20Hub(token).approve(spender, amount)) revert ApprovalFailed();
        emit ApprovalSet(token, spender, amount);
    }

    function emergencyWithdraw(address token, address to, uint256 amount) external onlyOwner {
        _safeTransfer(token, to, amount);
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
        if (steps.length < 2 || steps.length > 4) revert InvalidStepCount();

        uint256 balanceBefore = IERC20Hub(tokenIn).balanceOf(address(this));
        if (balanceBefore < amountIn) revert InsufficientBalance();

        address currentToken = tokenIn;
        uint256 currentAmount = amountIn;

        for (uint256 i = 0; i < steps.length; i++) {
            SwapStep calldata step = steps[i];
            if (step.tokenIn != currentToken) revert InvalidPath();

            uint256 tokenOutBefore = IERC20Hub(step.tokenOut).balanceOf(address(this));
            _swap(step, currentAmount, deadline);
            uint256 tokenOutAfter = IERC20Hub(step.tokenOut).balanceOf(address(this));
            if (tokenOutAfter <= tokenOutBefore) revert BalanceDidNotIncrease();

            currentAmount = tokenOutAfter - tokenOutBefore;
            currentToken = step.tokenOut;
        }

        if (currentToken != tokenIn) revert InvalidPath();

        uint256 balanceAfter = IERC20Hub(tokenIn).balanceOf(address(this));
        if (balanceAfter <= balanceBefore) revert MinProfitNotMet();

        profit = balanceAfter - balanceBefore;
        if (profit < minProfit) revert MinProfitNotMet();

        emit Executed(msg.sender, tokenIn, amountIn, profit);
    }

    function _swap(SwapStep calldata step, uint256 amountIn, uint256 deadline) internal {
        if (step.dex == StepKind.DirectV3) {
            _swapDirectV3(step, amountIn);
        } else if (step.dex == StepKind.DirectV2) {
            _swapDirectV2(step, amountIn);
        } else if (step.dex == StepKind.Adapter) {
            _swapAdapter(step, amountIn);
        } else {
            _swapRouter(step, amountIn, deadline);
        }
    }

    function _swapRouter(SwapStep calldata step, uint256 amountIn, uint256 deadline) internal {
        _validatePool(step);
        if (IERC20Hub(step.tokenIn).allowance(address(this), step.router) < amountIn) {
            revert InsufficientAllowance();
        }

        if (step.dex == StepKind.AerodromeClassic) {
            IAerodromeClassicRouterHub.Route[] memory routes = new IAerodromeClassicRouterHub.Route[](1);
            routes[0] = IAerodromeClassicRouterHub.Route({
                from: step.tokenIn,
                to: step.tokenOut,
                stable: step.stable,
                factory: step.factory
            });
            IAerodromeClassicRouterHub(step.router).swapExactTokensForTokens(amountIn, 0, routes, address(this), deadline);
        } else if (step.dex == StepKind.AerodromeSlipstream) {
            ISlipstreamRouterHub(step.router).exactInputSingle(
                ISlipstreamRouterHub.ExactInputSingleParams({
                    tokenIn: step.tokenIn,
                    tokenOut: step.tokenOut,
                    tickSpacing: _decodeTickSpacing(step.fee),
                    recipient: address(this),
                    deadline: deadline,
                    amountIn: amountIn,
                    amountOutMinimum: 0,
                    sqrtPriceLimitX96: 0
                })
            );
        } else if (step.dex == StepKind.UniswapV3) {
            IV3RouterHub(step.router).exactInputSingle(
                IV3RouterHub.ExactInputSingleParams({
                    tokenIn: step.tokenIn,
                    tokenOut: step.tokenOut,
                    fee: step.fee,
                    recipient: address(this),
                    amountIn: amountIn,
                    amountOutMinimum: 0,
                    sqrtPriceLimitX96: 0
                })
            );
        } else if (step.dex == StepKind.PancakeV3) {
            IPancakeV3RouterHub(step.router).exactInputSingle(
                IPancakeV3RouterHub.ExactInputSingleParams({
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

    function _swapDirectV3(SwapStep calldata step, uint256 amountIn) internal {
        IV3PoolHub pool = IV3PoolHub(step.pool);
        address token0 = pool.token0();
        address token1 = pool.token1();
        bool zeroForOne;
        if (step.tokenIn == token0 && step.tokenOut == token1) {
            zeroForOne = true;
        } else if (step.tokenIn == token1 && step.tokenOut == token0) {
            zeroForOne = false;
        } else {
            revert PoolMismatch();
        }
        if (step.fee != 0 && pool.fee() != step.fee) revert PoolMismatch();

        uint160 sqrtPriceLimitX96 = zeroForOne ? MIN_SQRT_RATIO_PLUS_ONE : MAX_SQRT_RATIO_MINUS_ONE;
        pool.swap(
            address(this),
            zeroForOne,
            int256(amountIn),
            sqrtPriceLimitX96,
            abi.encode(V3CallbackData({pool: step.pool, tokenIn: step.tokenIn}))
        );
    }

    function _swapDirectV2(SwapStep calldata step, uint256 amountIn) internal {
        if (step.stable) revert UnsupportedDex();
        if (step.fee == 0 || step.fee >= 10_000) revert InvalidFee();

        IV2PairHub pair = IV2PairHub(step.pool);
        address token0 = pair.token0();
        address token1 = pair.token1();
        (uint112 reserve0, uint112 reserve1,) = pair.getReserves();

        uint256 reserveIn;
        uint256 reserveOut;
        bool tokenOutIsToken0;
        if (step.tokenIn == token0 && step.tokenOut == token1) {
            reserveIn = uint256(reserve0);
            reserveOut = uint256(reserve1);
            tokenOutIsToken0 = false;
        } else if (step.tokenIn == token1 && step.tokenOut == token0) {
            reserveIn = uint256(reserve1);
            reserveOut = uint256(reserve0);
            tokenOutIsToken0 = true;
        } else {
            revert PoolMismatch();
        }

        _safeTransfer(step.tokenIn, step.pool, amountIn);
        uint256 amountOut = _v2AmountOut(amountIn, reserveIn, reserveOut, step.fee);
        pair.swap(tokenOutIsToken0 ? amountOut : 0, tokenOutIsToken0 ? 0 : amountOut, address(this), "");
    }

    function _swapAdapter(SwapStep calldata step, uint256 amountIn) internal {
        if (!adapters[step.router]) revert AdapterNotWhitelisted();
        _safeTransfer(step.tokenIn, step.router, amountIn);
        IExecutorAdapterHub(step.router).swap(
            step.pool,
            step.tokenIn,
            step.tokenOut,
            step.fee,
            step.stable,
            step.factory,
            amountIn,
            address(this),
            step.data
        );
    }

    function uniswapV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata data) external {
        _v3SwapCallback(amount0Delta, amount1Delta, data);
    }

    function pancakeV3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata data) external {
        _v3SwapCallback(amount0Delta, amount1Delta, data);
    }

    function _v3SwapCallback(int256 amount0Delta, int256 amount1Delta, bytes calldata data) internal {
        V3CallbackData memory decoded = abi.decode(data, (V3CallbackData));
        if (msg.sender != decoded.pool) revert UnauthorizedCallback();

        address tokenToPay;
        uint256 amountToPay;
        if (amount0Delta > 0) {
            tokenToPay = IV3PoolHub(decoded.pool).token0();
            amountToPay = uint256(amount0Delta);
        } else if (amount1Delta > 0) {
            tokenToPay = IV3PoolHub(decoded.pool).token1();
            amountToPay = uint256(amount1Delta);
        } else {
            revert InvalidCallback();
        }
        if (tokenToPay != decoded.tokenIn) revert InvalidCallback();
        _safeTransfer(tokenToPay, msg.sender, amountToPay);
    }

    function _validatePool(SwapStep calldata step) internal view {
        if (step.factory == address(0) || step.pool == address(0)) return;

        address expectedPool;
        if (step.dex == StepKind.AerodromeClassic) {
            expectedPool = IAerodromeClassicFactoryHub(step.factory).getPool(step.tokenIn, step.tokenOut, step.stable);
        } else if (step.dex == StepKind.AerodromeSlipstream) {
            expectedPool =
                ISlipstreamFactoryHub(step.factory).getPool(step.tokenIn, step.tokenOut, _decodeTickSpacing(step.fee));
        } else if (step.dex == StepKind.UniswapV3 || step.dex == StepKind.PancakeV3) {
            expectedPool = IV3FactoryHub(step.factory).getPool(step.tokenIn, step.tokenOut, step.fee);
        } else {
            return;
        }

        if (expectedPool != step.pool) revert PoolMismatch();
    }

    function _v2AmountOut(uint256 amountIn, uint256 reserveIn, uint256 reserveOut, uint24 feeBps)
        internal
        pure
        returns (uint256)
    {
        uint256 amountInWithFee = amountIn * (10_000 - uint256(feeBps));
        return (amountInWithFee * reserveOut) / (reserveIn * 10_000 + amountInWithFee);
    }

    function _decodeTickSpacing(uint24 value) internal pure returns (int24) {
        if (value == 0 || value > uint24(type(int24).max)) revert InvalidTickSpacing();
        return int24(int256(uint256(value)));
    }

    function _safeTransfer(address token, address to, uint256 amount) internal {
        (bool ok, bytes memory data) = token.call(abi.encodeWithSelector(IERC20Hub.transfer.selector, to, amount));
        if (!ok || (data.length != 0 && !abi.decode(data, (bool)))) revert TransferFailed();
    }
}
