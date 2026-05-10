// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Script.sol";
import "../src/Executor.sol";

contract DeployExecutor is Script {
    uint256 internal constant MAX_UINT = type(uint256).max;

    function run() external returns (Executor executor) {
        uint256 deployerKey = vm.envUint("DEPLOYER_PRIVATE_KEY");
        address owner = vm.envAddress("EXECUTOR_OWNER");
        address operator = vm.envOr("EXECUTOR_OPERATOR", owner);
        require(owner != address(0), "EXECUTOR_OWNER required");
        if (operator == address(0)) operator = owner;

        vm.startBroadcast(deployerKey);

        executor = new Executor(owner);

        executor.setOperator(operator, true);

        address aerodromeRouter = vm.envOr("AERODROME_ROUTER", address(0));
        address uniswapV3Router = vm.envOr("UNISWAP_V3_ROUTER", address(0));
        _setRouter(executor, aerodromeRouter);
        _setRouter(executor, uniswapV3Router);

        _setFactory(executor, vm.envOr("AERODROME_POOL_FACTORY", address(0)));

        _setPool(executor, vm.envOr("AERODROME_USDC_WETH_POOL", address(0)));
        _setPool(executor, vm.envOr("UNISWAP_V3_USDC_WETH_500_POOL", address(0)));
        _setPool(executor, vm.envOr("UNISWAP_V3_USDC_WETH_3000_POOL", address(0)));
        _setOptionalPools(executor);

        address usdc = vm.envAddress("USDC_ADDRESS");
        address weth = vm.envAddress("WETH_ADDRESS");
        _setTokenAndApprovals(executor, usdc, aerodromeRouter, uniswapV3Router);
        _setTokenAndApprovals(executor, weth, aerodromeRouter, uniswapV3Router);
        _setOptionalTokens(executor, aerodromeRouter, uniswapV3Router);

        vm.stopBroadcast();
    }

    function _setOptionalTokens(Executor executor, address aerodromeRouter, address uniswapV3Router) internal {
        _setTokenAndApprovals(executor, vm.envOr("TOKEN_WHITELIST_3", address(0)), aerodromeRouter, uniswapV3Router);
        _setTokenAndApprovals(executor, vm.envOr("TOKEN_WHITELIST_4", address(0)), aerodromeRouter, uniswapV3Router);
        _setTokenAndApprovals(executor, vm.envOr("TOKEN_WHITELIST_5", address(0)), aerodromeRouter, uniswapV3Router);
        _setTokenAndApprovals(executor, vm.envOr("TOKEN_WHITELIST_6", address(0)), aerodromeRouter, uniswapV3Router);
        _setTokenAndApprovals(executor, vm.envOr("TOKEN_WHITELIST_7", address(0)), aerodromeRouter, uniswapV3Router);
        _setTokenAndApprovals(executor, vm.envOr("TOKEN_WHITELIST_8", address(0)), aerodromeRouter, uniswapV3Router);
        _setTokenAndApprovals(executor, vm.envOr("TOKEN_WHITELIST_9", address(0)), aerodromeRouter, uniswapV3Router);
        _setTokenAndApprovals(executor, vm.envOr("TOKEN_WHITELIST_10", address(0)), aerodromeRouter, uniswapV3Router);
    }

    function _setOptionalPools(Executor executor) internal {
        _setPool(executor, vm.envOr("POOL_WHITELIST_1", address(0)));
        _setPool(executor, vm.envOr("POOL_WHITELIST_2", address(0)));
        _setPool(executor, vm.envOr("POOL_WHITELIST_3", address(0)));
        _setPool(executor, vm.envOr("POOL_WHITELIST_4", address(0)));
        _setPool(executor, vm.envOr("POOL_WHITELIST_5", address(0)));
        _setPool(executor, vm.envOr("POOL_WHITELIST_6", address(0)));
        _setPool(executor, vm.envOr("POOL_WHITELIST_7", address(0)));
        _setPool(executor, vm.envOr("POOL_WHITELIST_8", address(0)));
        _setPool(executor, vm.envOr("POOL_WHITELIST_9", address(0)));
        _setPool(executor, vm.envOr("POOL_WHITELIST_10", address(0)));
    }

    function _setTokenAndApprovals(Executor executor, address token, address aerodromeRouter, address uniswapV3Router)
        internal
    {
        if (token == address(0)) return;
        executor.setTokenWhitelist(token, true);
        if (aerodromeRouter != address(0)) executor.approveToken(token, aerodromeRouter, MAX_UINT);
        if (uniswapV3Router != address(0)) executor.approveToken(token, uniswapV3Router, MAX_UINT);
    }

    function _setRouter(Executor executor, address router) internal {
        if (router != address(0)) executor.setRouterWhitelist(router, true);
    }

    function _setFactory(Executor executor, address factory) internal {
        if (factory != address(0)) executor.setFactoryWhitelist(factory, true);
    }

    function _setPool(Executor executor, address pool) internal {
        if (pool != address(0)) executor.setPoolWhitelist(pool, true);
    }
}
