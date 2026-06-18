// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Script.sol";
import "../src/BalancerV3Adapter.sol";

contract DeployBalancerV3Adapter is Script {
    function run() external returns (BalancerV3Adapter adapter) {
        uint256 deployerKey = vm.envUint("DEPLOYER_PRIVATE_KEY");
        address hub = vm.envOr("EXECUTOR_HUB", vm.envAddress("EXECUTOR_CONTRACT"));
        address router = vm.envAddress("BALANCER_V3_ROUTER");
        require(hub != address(0), "EXECUTOR_HUB or EXECUTOR_CONTRACT required");
        require(router != address(0), "BALANCER_V3_ROUTER required");

        vm.startBroadcast(deployerKey);
        adapter = new BalancerV3Adapter(hub, router);
        vm.stopBroadcast();
    }
}
