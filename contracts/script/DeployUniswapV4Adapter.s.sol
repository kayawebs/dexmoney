// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Script.sol";
import "../src/UniswapV4Adapter.sol";

contract DeployUniswapV4Adapter is Script {
    function run() external returns (UniswapV4Adapter adapter) {
        uint256 deployerKey = vm.envUint("DEPLOYER_PRIVATE_KEY");
        address hub = vm.envOr("EXECUTOR_HUB", vm.envAddress("EXECUTOR_CONTRACT"));
        address manager = vm.envAddress("UNISWAP_V4_POOL_MANAGER");
        require(hub != address(0), "EXECUTOR_HUB or EXECUTOR_CONTRACT required");
        require(manager != address(0), "UNISWAP_V4_POOL_MANAGER required");

        vm.startBroadcast(deployerKey);
        adapter = new UniswapV4Adapter(hub, manager);
        vm.stopBroadcast();
    }
}
