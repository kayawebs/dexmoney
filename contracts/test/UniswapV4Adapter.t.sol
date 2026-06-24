// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import "../src/UniswapV4Adapter.sol";

contract MockERC20V4AdapterTest {
    mapping(address => uint256) public balanceOf;

    function mint(address to, uint256 amount) external {
        balanceOf[to] += amount;
    }

    function transfer(address to, uint256 amount) external returns (bool) {
        balanceOf[msg.sender] -= amount;
        balanceOf[to] += amount;
        return true;
    }
}

contract MockPoolManagerV4AdapterTest {
    int128 public amount0Delta;
    int128 public amount1Delta;

    function setDelta(int128 amount0Delta_, int128 amount1Delta_) external {
        amount0Delta = amount0Delta_;
        amount1Delta = amount1Delta_;
    }

    function unlock(bytes calldata data) external returns (bytes memory) {
        return IUnlockCallbackV4Adapter(msg.sender).unlockCallback(data);
    }

    function swap(
        IPoolManagerV4Adapter.PoolKey calldata,
        IPoolManagerV4Adapter.SwapParams calldata,
        bytes calldata
    ) external view returns (int256) {
        return (int256(amount0Delta) << 128) | int256(uint256(uint128(amount1Delta)));
    }

    function sync(address) external {}

    function settle() external payable returns (uint256 paid) {
        return 0;
    }

    function take(address currency, address to, uint256 amount) external {
        MockERC20V4AdapterTest(currency).transfer(to, amount);
    }
}

contract UniswapV4AdapterTest is Test {
    MockPoolManagerV4AdapterTest internal manager;
    UniswapV4Adapter internal adapter;
    MockERC20V4AdapterTest internal tokenA;
    MockERC20V4AdapterTest internal tokenB;

    function setUp() public {
        manager = new MockPoolManagerV4AdapterTest();
        adapter = new UniswapV4Adapter(address(this), address(manager));
        tokenA = new MockERC20V4AdapterTest();
        tokenB = new MockERC20V4AdapterTest();
    }

    function testSwapUsesV4CallerBalanceDeltaSigns() public {
        (address currency0, address currency1) = address(tokenA) < address(tokenB)
            ? (address(tokenA), address(tokenB))
            : (address(tokenB), address(tokenA));
        MockERC20V4AdapterTest tokenIn = MockERC20V4AdapterTest(currency0);
        MockERC20V4AdapterTest tokenOut = MockERC20V4AdapterTest(currency1);

        uint256 amountIn = 100;
        uint256 amountOut = 90;
        tokenIn.mint(address(adapter), amountIn);
        tokenOut.mint(address(manager), amountOut);

        manager.setDelta(-int128(int256(amountIn)), int128(int256(amountOut)));

        bytes memory data = abi.encode(currency0, currency1, uint24(500), int24(10), address(0), uint160(0), bytes(""));

        uint256 returned = adapter.swap(
            address(0),
            currency0,
            currency1,
            500,
            false,
            address(manager),
            amountIn,
            address(this),
            data
        );

        assertEq(returned, amountOut);
        assertEq(tokenIn.balanceOf(address(manager)), amountIn);
        assertEq(tokenOut.balanceOf(address(this)), amountOut);
    }
}
