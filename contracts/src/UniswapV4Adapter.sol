// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

interface IERC20V4Adapter {
    function balanceOf(address account) external view returns (uint256);
    function transfer(address to, uint256 amount) external returns (bool);
}

interface IUnlockCallbackV4Adapter {
    function unlockCallback(bytes calldata data) external returns (bytes memory);
}

interface IPoolManagerV4Adapter {
    struct PoolKey {
        address currency0;
        address currency1;
        uint24 fee;
        int24 tickSpacing;
        address hooks;
    }

    struct SwapParams {
        bool zeroForOne;
        int256 amountSpecified;
        uint160 sqrtPriceLimitX96;
    }

    function unlock(bytes calldata data) external returns (bytes memory);
    function swap(PoolKey calldata key, SwapParams calldata params, bytes calldata hookData)
        external
        returns (int256 swapDelta);
    function sync(address currency) external;
    function settle() external payable returns (uint256 paid);
    function take(address currency, address to, uint256 amount) external;
}

/// @notice ExecutorHub adapter for Uniswap V4 exact-input swaps.
/// @dev Adapter data is abi.encode(currency0, currency1, fee, tickSpacing, hooks, sqrtPriceLimitX96, hookData).
contract UniswapV4Adapter is IUnlockCallbackV4Adapter {
    uint160 internal constant MIN_SQRT_RATIO_PLUS_ONE = 4295128740;
    uint160 internal constant MAX_SQRT_RATIO_MINUS_ONE =
        1461446703485210103287273052203988822378723970341;

    error UnauthorizedCaller();
    error UnauthorizedCallback();
    error InvalidManager();
    error InvalidPoolKey();
    error InvalidToken();
    error InvalidAmount();
    error TransferFailed();
    error NoOutput();

    address public immutable hub;

    struct UnlockData {
        address manager;
        IPoolManagerV4Adapter.PoolKey key;
        address tokenIn;
        address tokenOut;
        uint256 amountIn;
        address recipient;
        uint160 sqrtPriceLimitX96;
        bytes hookData;
    }

    constructor(address hub_) {
        if (hub_ == address(0)) revert UnauthorizedCaller();
        hub = hub_;
    }

    function swap(
        address pool,
        address tokenIn,
        address tokenOut,
        uint24 fee,
        bool,
        address factory,
        uint256 amountIn,
        address recipient,
        bytes calldata data
    ) external returns (uint256 amountOut) {
        if (msg.sender != hub) revert UnauthorizedCaller();
        if (tokenIn == address(0) || tokenOut == address(0)) revert InvalidToken();
        if (amountIn == 0) revert InvalidAmount();

        address manager = factory == address(0) ? pool : factory;
        if (manager == address(0)) revert InvalidManager();

        UnlockData memory unlockData =
            _decodeUnlockData(manager, tokenIn, tokenOut, fee, amountIn, recipient, data);
        bytes memory result = IPoolManagerV4Adapter(manager).unlock(abi.encode(unlockData));
        amountOut = abi.decode(result, (uint256));
        if (amountOut == 0) revert NoOutput();
    }

    function unlockCallback(bytes calldata data) external returns (bytes memory) {
        UnlockData memory decoded = abi.decode(data, (UnlockData));
        if (msg.sender != decoded.manager) revert UnauthorizedCallback();

        bool zeroForOne = _validateAndDirection(decoded.key, decoded.tokenIn, decoded.tokenOut);
        int256 delta = IPoolManagerV4Adapter(decoded.manager).swap(
            decoded.key,
            IPoolManagerV4Adapter.SwapParams({
                zeroForOne: zeroForOne,
                amountSpecified: -int256(decoded.amountIn),
                sqrtPriceLimitX96: decoded.sqrtPriceLimitX96
            }),
            decoded.hookData
        );

        int128 delta0 = _amount0(delta);
        int128 delta1 = _amount1(delta);
        int128 inputDelta = zeroForOne ? delta0 : delta1;
        int128 outputDelta = zeroForOne ? delta1 : delta0;
        if (inputDelta >= 0 || outputDelta <= 0) revert NoOutput();

        uint256 amountToSettle = uint256(uint128(-inputDelta));
        uint256 amountToTake = uint256(uint128(outputDelta));
        _settle(decoded.manager, decoded.tokenIn, amountToSettle);
        IPoolManagerV4Adapter(decoded.manager).take(decoded.tokenOut, decoded.recipient, amountToTake);

        return abi.encode(amountToTake);
    }

    function _decodeUnlockData(
        address manager,
        address tokenIn,
        address tokenOut,
        uint24 fallbackFee,
        uint256 amountIn,
        address recipient,
        bytes calldata data
    ) internal pure returns (UnlockData memory unlockData) {
        (
            address currency0,
            address currency1,
            uint24 fee,
            int24 tickSpacing,
            address hooks,
            uint160 sqrtPriceLimitX96,
            bytes memory hookData
        ) = _decodeData(data);
        if (fee == 0) fee = fallbackFee;
        unlockData = UnlockData({
            manager: manager,
            key: IPoolManagerV4Adapter.PoolKey({
                currency0: currency0,
                currency1: currency1,
                fee: fee,
                tickSpacing: tickSpacing,
                hooks: hooks
            }),
            tokenIn: tokenIn,
            tokenOut: tokenOut,
            amountIn: amountIn,
            recipient: recipient,
            sqrtPriceLimitX96: sqrtPriceLimitX96,
            hookData: hookData
        });
        bool zeroForOne = _validateAndDirection(unlockData.key, tokenIn, tokenOut);
        if (unlockData.sqrtPriceLimitX96 == 0) {
            unlockData.sqrtPriceLimitX96 =
                zeroForOne ? MIN_SQRT_RATIO_PLUS_ONE : MAX_SQRT_RATIO_MINUS_ONE;
        }
    }

    function _decodeData(bytes calldata data)
        internal
        pure
        returns (
            address currency0,
            address currency1,
            uint24 fee,
            int24 tickSpacing,
            address hooks,
            uint160 sqrtPriceLimitX96,
            bytes memory hookData
        )
    {
        return abi.decode(data, (address, address, uint24, int24, address, uint160, bytes));
    }

    function _validateAndDirection(IPoolManagerV4Adapter.PoolKey memory key, address tokenIn, address tokenOut)
        internal
        pure
        returns (bool zeroForOne)
    {
        if (key.currency0 >= key.currency1) revert InvalidPoolKey();
        if (tokenIn == key.currency0 && tokenOut == key.currency1) return true;
        if (tokenIn == key.currency1 && tokenOut == key.currency0) return false;
        revert InvalidToken();
    }

    function _settle(address manager, address token, uint256 amount) internal {
        IPoolManagerV4Adapter(manager).sync(token);
        if (!IERC20V4Adapter(token).transfer(manager, amount)) revert TransferFailed();
        IPoolManagerV4Adapter(manager).settle();
    }

    function _amount0(int256 delta) internal pure returns (int128) {
        return int128(delta >> 128);
    }

    function _amount1(int256 delta) internal pure returns (int128) {
        return int128(delta);
    }
}
