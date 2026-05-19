// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import "../src/ExecutorV2.sol";

contract MockERC20V2 {
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

contract MockSlipstreamRouterV2 {
    uint256 public amountOut;
    int24 public lastTickSpacing;

    function setAmountOut(uint256 value) external {
        amountOut = value;
    }

    function exactInputSingle(ISlipstreamRouterV2.ExactInputSingleParams calldata params)
        external
        payable
        returns (uint256)
    {
        lastTickSpacing = params.tickSpacing;
        MockERC20V2(params.tokenIn).transferFrom(msg.sender, address(this), params.amountIn);
        MockERC20V2(params.tokenOut).transfer(params.recipient, amountOut);
        return amountOut;
    }
}

contract MockV3RouterV2 {
    uint256 public amountOut;

    function setAmountOut(uint256 value) external {
        amountOut = value;
    }

    function exactInputSingle(IV3RouterV2.ExactInputSingleParams calldata params) external payable returns (uint256) {
        MockERC20V2(params.tokenIn).transferFrom(msg.sender, address(this), params.amountIn);
        MockERC20V2(params.tokenOut).transfer(params.recipient, amountOut);
        return amountOut;
    }
}

contract MockV3FactoryV2 {
    address public pool;

    function setPool(address value) external {
        pool = value;
    }

    function getPool(address, address, uint24) external view returns (address) {
        return pool;
    }
}

contract ExecutorV2Test is Test {
    ExecutorV2 internal executor;
    MockERC20V2 internal usdc;
    MockERC20V2 internal weth;
    MockSlipstreamRouterV2 internal slipstreamRouter;
    MockV3RouterV2 internal v3Router;
    MockV3FactoryV2 internal v3Factory;

    address internal owner = address(0xA11CE);
    address internal operator = address(0xB0B);
    address internal slipstreamPool = address(0x2222);
    address internal v3Pool = address(0x3333);

    function setUp() public {
        vm.prank(owner);
        executor = new ExecutorV2(owner);

        usdc = new MockERC20V2();
        weth = new MockERC20V2();
        slipstreamRouter = new MockSlipstreamRouterV2();
        v3Router = new MockV3RouterV2();
        v3Factory = new MockV3FactoryV2();

        usdc.mint(address(executor), 1_000_000);
        weth.mint(address(slipstreamRouter), 1_000_000);
        usdc.mint(address(v3Router), 1_000_000);

        slipstreamRouter.setAmountOut(100);
        v3Router.setAmountOut(100_001);
        v3Factory.setPool(v3Pool);

        vm.startPrank(owner);
        executor.setOperator(operator, true);
        executor.approveToken(address(usdc), address(slipstreamRouter), type(uint256).max);
        executor.approveToken(address(weth), address(v3Router), type(uint256).max);
        vm.stopPrank();
    }

    function _steps() internal view returns (ExecutorV2.SwapStep[] memory steps) {
        steps = new ExecutorV2.SwapStep[](2);
        steps[0] = ExecutorV2.SwapStep({
            dex: ExecutorV2.DexKind.AerodromeSlipstream,
            router: address(slipstreamRouter),
            pool: slipstreamPool,
            tokenIn: address(usdc),
            tokenOut: address(weth),
            fee: 100,
            stable: false,
            factory: address(0)
        });
        steps[1] = ExecutorV2.SwapStep({
            dex: ExecutorV2.DexKind.UniswapV3,
            router: address(v3Router),
            pool: v3Pool,
            tokenIn: address(weth),
            tokenOut: address(usdc),
            fee: 500,
            stable: false,
            factory: address(v3Factory)
        });
    }

    function testExecutesSlipstreamWithoutWhitelists() public {
        vm.prank(operator);
        uint256 profit = executor.executeWithOwnFunds(address(usdc), 100_000, _steps(), 1, block.timestamp + 1);

        assertEq(profit, 1);
        if (slipstreamRouter.lastTickSpacing() != int24(100)) revert("wrong tick spacing");
        assertEq(usdc.balanceOf(address(executor)), 1_000_001);
        assertEq(usdc.allowance(address(executor), address(slipstreamRouter)), type(uint256).max);
        assertEq(weth.allowance(address(executor), address(v3Router)), type(uint256).max);
    }

    function testRejectsZeroSlipstreamTickSpacing() public {
        ExecutorV2.SwapStep[] memory steps = _steps();
        steps[0].fee = 0;

        vm.expectRevert(ExecutorV2.InvalidTickSpacing.selector);
        vm.prank(operator);
        executor.executeWithOwnFunds(address(usdc), 100_000, steps, 0, block.timestamp + 1);
    }

    function testRejectsPoolMismatchWhenFactoryProvided() public {
        v3Factory.setPool(address(0xDEAD));

        vm.expectRevert(ExecutorV2.PoolMismatch.selector);
        vm.prank(operator);
        executor.executeWithOwnFunds(address(usdc), 100_000, _steps(), 0, block.timestamp + 1);
    }

    function testRequiresApproval() public {
        MockSlipstreamRouterV2 unapprovedRouter = new MockSlipstreamRouterV2();
        unapprovedRouter.setAmountOut(100);
        weth.mint(address(unapprovedRouter), 1_000_000);

        ExecutorV2.SwapStep[] memory steps = _steps();
        steps[0].router = address(unapprovedRouter);

        vm.expectRevert(ExecutorV2.InsufficientAllowance.selector);
        vm.prank(operator);
        executor.executeWithOwnFunds(address(usdc), 100_000, steps, 0, block.timestamp + 1);
    }
}
