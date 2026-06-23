// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import "../src/ExecutorHub.sol";

contract MockERC20HubTest {
    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(address => uint256)) public allowance;

    function mint(address to, uint256 amount) external {
        balanceOf[to] += amount;
    }

    function approve(address spender, uint256 amount) external returns (bool) {
        allowance[msg.sender][spender] = amount;
        return true;
    }

    function transfer(address to, uint256 amount) external returns (bool) {
        balanceOf[msg.sender] -= amount;
        balanceOf[to] += amount;
        return true;
    }

    function transferFrom(address from, address to, uint256 amount) external returns (bool) {
        uint256 currentAllowance = allowance[from][msg.sender];
        if (currentAllowance != type(uint256).max) {
            allowance[from][msg.sender] = currentAllowance - amount;
        }
        balanceOf[from] -= amount;
        balanceOf[to] += amount;
        return true;
    }
}

contract MaliciousV3CallbackCaller {
    address public immutable token0;
    address public immutable token1;
    uint24 public immutable fee;

    constructor(address token0_, address token1_, uint24 fee_) {
        token0 = token0_;
        token1 = token1_;
        fee = fee_;
    }

    function attack(ExecutorHub hub, address tokenIn, uint256 amount) external {
        hub.uniswapV3SwapCallback(
            int256(amount),
            0,
            abi.encode(ExecutorHub.V3CallbackData({pool: address(this), tokenIn: tokenIn}))
        );
    }
}

contract ExecutorHubTest is Test {
    ExecutorHub internal hub;
    MockERC20HubTest internal usdc;
    MockERC20HubTest internal weth;
    MaliciousV3CallbackCaller internal maliciousPool;

    address internal owner = address(0xA11CE);

    function setUp() public {
        vm.prank(owner);
        hub = new ExecutorHub(owner);
        usdc = new MockERC20HubTest();
        weth = new MockERC20HubTest();
        maliciousPool = new MaliciousV3CallbackCaller(address(usdc), address(weth), 500);
        usdc.mint(address(hub), 1_000_000);
    }

    function testRejectsExternalV3CallbackTheft() public {
        vm.expectRevert(ExecutorHub.UnauthorizedCallback.selector);
        maliciousPool.attack(hub, address(usdc), 100_000);

        assertEq(usdc.balanceOf(address(hub)), 1_000_000);
        assertEq(usdc.balanceOf(address(maliciousPool)), 0);
    }
}
