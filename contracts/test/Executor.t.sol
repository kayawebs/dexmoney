// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import "../src/Executor.sol";

contract MockERC20 {
    string public name = "Mock";
    string public symbol = "MOCK";
    uint8 public decimals = 6;
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

contract ExecutorTest is Test {
    Executor internal executor;
    MockERC20 internal token;

    address internal owner = address(0xA11CE);
    address internal operator = address(0xB0B);
    address internal stranger = address(0xCAFE);
    address internal router = address(0x1111);
    address internal pool = address(0x2222);

    function setUp() public {
        vm.prank(owner);
        executor = new Executor(owner);

        token = new MockERC20();
        token.mint(address(executor), 1_000_000);

        vm.startPrank(owner);
        executor.setOperator(operator, true);
        executor.setTokenWhitelist(address(token), true);
        executor.setRouterWhitelist(router, true);
        executor.setPoolWhitelist(pool, true);
        vm.stopPrank();
    }

    function _emptySteps() internal view returns (Executor.SwapStep[] memory steps) {
        steps = new Executor.SwapStep[](0);
    }

    function testOnlyOperator() public {
        vm.expectRevert(Executor.OnlyOperator.selector);
        vm.prank(stranger);
        executor.executeWithOwnFunds(address(token), 100, _emptySteps(), 0, block.timestamp + 1);
    }

    function testWhitelist() public {
        vm.prank(owner);
        executor.setTokenWhitelist(address(token), false);

        vm.expectRevert(Executor.TokenNotWhitelisted.selector);
        vm.prank(operator);
        executor.executeWithOwnFunds(address(token), 100, _emptySteps(), 0, block.timestamp + 1);
    }

    function testMinProfitRevert() public {
        vm.expectRevert(Executor.MinProfitNotMet.selector);
        vm.prank(operator);
        executor.executeWithOwnFunds(address(token), 100, _emptySteps(), 1, block.timestamp + 1);
    }

    function testEmergencyWithdraw() public {
        uint256 beforeBal = token.balanceOf(owner);

        vm.prank(owner);
        executor.emergencyWithdraw(address(token), owner, 200);

        assertEq(token.balanceOf(owner), beforeBal + 200);
    }

    function testPause() public {
        vm.prank(owner);
        executor.setPaused(true);

        vm.expectRevert(Executor.PausedError.selector);
        vm.prank(operator);
        executor.executeWithOwnFunds(address(token), 100, _emptySteps(), 0, block.timestamp + 1);
    }
}

