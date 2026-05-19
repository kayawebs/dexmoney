// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Script.sol";
import "../src/ExecutorV2.sol";

contract DeployExecutorV2 is Script {
    function run() external returns (ExecutorV2 executor) {
        uint256 deployerKey = vm.envUint("DEPLOYER_PRIVATE_KEY");
        address owner = vm.envAddress("EXECUTOR_OWNER");
        address operator = vm.envOr("EXECUTOR_OPERATOR", owner);
        require(owner != address(0), "EXECUTOR_OWNER required");
        if (operator == address(0)) operator = owner;

        vm.startBroadcast(deployerKey);

        executor = new ExecutorV2(owner);
        executor.setOperator(operator, true);

        vm.stopBroadcast();
    }
}
