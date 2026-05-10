// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Test.sol";
import "../src/Executor.sol";

contract MockERC20 {
    string public name = "Mock";
    string public symbol = "MOCK";
    uint8 public decimals = 6;
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

contract MockAerodromeRouter {
    uint256 public amountOut;

    function setAmountOut(uint256 value) external {
        amountOut = value;
    }

    function swapExactTokensForTokens(
        uint256 amountIn,
        uint256,
        IAerodromeRouter.Route[] calldata routes,
        address to,
        uint256
    ) external returns (uint256[] memory amounts) {
        MockERC20(routes[0].from).transferFrom(msg.sender, address(this), amountIn);
        MockERC20(routes[0].to).transfer(to, amountOut);

        amounts = new uint256[](2);
        amounts[0] = amountIn;
        amounts[1] = amountOut;
    }
}

contract MockUniswapV3Router {
    uint256 public amountOut;

    function setAmountOut(uint256 value) external {
        amountOut = value;
    }

    function exactInputSingle(IUniswapV3Router.ExactInputSingleParams calldata params)
        external
        payable
        returns (uint256)
    {
        MockERC20(params.tokenIn).transferFrom(msg.sender, address(this), params.amountIn);
        MockERC20(params.tokenOut).transfer(params.recipient, amountOut);
        return amountOut;
    }
}

contract ExecutorTest is Test {
    Executor internal executor;
    MockERC20 internal usdc;
    MockERC20 internal weth;
    MockAerodromeRouter internal aeroRouter;
    MockUniswapV3Router internal uniRouter;

    address internal owner = address(0xA11CE);
    address internal operator = address(0xB0B);
    address internal stranger = address(0xCAFE);
    address internal aeroPool = address(0x2222);
    address internal uniPool = address(0x3333);
    address internal aeroFactory = address(0x4444);

    function setUp() public {
        vm.prank(owner);
        executor = new Executor(owner);

        usdc = new MockERC20();
        weth = new MockERC20();
        aeroRouter = new MockAerodromeRouter();
        uniRouter = new MockUniswapV3Router();

        usdc.mint(address(executor), 1_000_000);
        weth.mint(address(aeroRouter), 1_000_000);
        usdc.mint(address(uniRouter), 1_000_000);

        aeroRouter.setAmountOut(100);
        uniRouter.setAmountOut(100_001);

        vm.startPrank(owner);
        executor.setOperator(operator, true);
        executor.setTokenWhitelist(address(usdc), true);
        executor.setTokenWhitelist(address(weth), true);
        executor.setRouterWhitelist(address(aeroRouter), true);
        executor.setRouterWhitelist(address(uniRouter), true);
        executor.setPoolWhitelist(aeroPool, true);
        executor.setPoolWhitelist(uniPool, true);
        executor.setFactoryWhitelist(aeroFactory, true);
        executor.approveToken(address(usdc), address(aeroRouter), type(uint256).max);
        executor.approveToken(address(weth), address(uniRouter), type(uint256).max);
        vm.stopPrank();
    }

    function _steps() internal view returns (Executor.SwapStep[] memory steps) {
        steps = new Executor.SwapStep[](2);
        steps[0] = Executor.SwapStep({
            dex: Executor.DexKind.AerodromeClassic,
            router: address(aeroRouter),
            pool: aeroPool,
            tokenIn: address(usdc),
            tokenOut: address(weth),
            fee: 30,
            stable: false,
            factory: aeroFactory
        });
        steps[1] = Executor.SwapStep({
            dex: Executor.DexKind.UniswapV3,
            router: address(uniRouter),
            pool: uniPool,
            tokenIn: address(weth),
            tokenOut: address(usdc),
            fee: 500,
            stable: false,
            factory: address(0)
        });
    }

    function testOnlyOperator() public {
        vm.expectRevert(Executor.OnlyOperator.selector);
        vm.prank(stranger);
        executor.executeWithOwnFunds(address(usdc), 100_000, _steps(), 0, block.timestamp + 1);
    }

    function testWhitelist() public {
        vm.prank(owner);
        executor.setTokenWhitelist(address(usdc), false);

        vm.expectRevert(Executor.TokenNotWhitelisted.selector);
        vm.prank(operator);
        executor.executeWithOwnFunds(address(usdc), 100_000, _steps(), 0, block.timestamp + 1);
    }

    function testMinProfitRevertRollsBackSwaps() public {
        uint256 beforeBal = usdc.balanceOf(address(executor));

        vm.expectRevert(Executor.MinProfitNotMet.selector);
        vm.prank(operator);
        executor.executeWithOwnFunds(address(usdc), 100_000, _steps(), 2, block.timestamp + 1);

        assertEq(usdc.balanceOf(address(executor)), beforeBal);
    }

    function testExecuteWithProfit() public {
        vm.prank(operator);
        uint256 profit = executor.executeWithOwnFunds(address(usdc), 100_000, _steps(), 1, block.timestamp + 1);

        assertEq(profit, 1);
        assertEq(usdc.balanceOf(address(executor)), 1_000_001);
    }

    function testEmergencyWithdraw() public {
        uint256 beforeBal = usdc.balanceOf(owner);

        vm.prank(owner);
        executor.emergencyWithdraw(address(usdc), owner, 200);

        assertEq(usdc.balanceOf(owner), beforeBal + 200);
    }

    function testPause() public {
        vm.prank(owner);
        executor.setPaused(true);

        vm.expectRevert(Executor.PausedError.selector);
        vm.prank(operator);
        executor.executeWithOwnFunds(address(usdc), 100_000, _steps(), 0, block.timestamp + 1);
    }

    function testUnsupportedSlipstream() public {
        Executor.SwapStep[] memory steps = _steps();
        steps[0].dex = Executor.DexKind.AerodromeSlipstream;

        vm.expectRevert(Executor.UnsupportedDex.selector);
        vm.prank(operator);
        executor.executeWithOwnFunds(address(usdc), 100_000, steps, 0, block.timestamp + 1);
    }
}
