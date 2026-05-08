// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Script.sol";
import "../src/Executor.sol";

contract DeployExecutor is Script {
    function run() external returns (Executor executor) {
        uint256 deployerKey = vm.envUint("DEPLOYER_PRIVATE_KEY");
        address owner = vm.envAddress("EXECUTOR_OWNER");

        vm.startBroadcast(deployerKey);
        executor = new Executor(owner);
        vm.stopBroadcast();
    }
}
