// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Script.sol";
import "../src/ExecutorHub.sol";

contract DeployExecutorHub is Script {
    function run() external returns (ExecutorHub executor) {
        uint256 deployerKey = vm.envUint("DEPLOYER_PRIVATE_KEY");
        address owner = vm.envAddress("EXECUTOR_OWNER");
        address operator = vm.envOr("EXECUTOR_OPERATOR", owner);
        require(owner != address(0), "EXECUTOR_OWNER required");
        if (operator == address(0)) operator = owner;

        vm.startBroadcast(deployerKey);

        executor = new ExecutorHub(owner);
        executor.setOperator(operator, true);

        vm.stopBroadcast();
    }
}
