// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import "forge-std/Script.sol";
import "../src/Executor.sol";

contract ConfigureExecutorTokenWhitelist is Script {
    uint256 internal constant MAX_UINT = type(uint256).max;

    address internal constant EXECUTOR_CONTRACT = 0x34867310A771F285967Ae8aa6aA622582fd8418A;
    address internal constant AERODROME_ROUTER = 0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43;
    address internal constant UNISWAP_V3_ROUTER = 0x2626664c2603336E57B271c5C0b26F421741e481;

    address internal constant TOKEN_3 = 0x989cFDC3508500d0C91f22896a0D2ee1Ef675870;
    address internal constant TOKEN_4 = 0xacfE6019Ed1A7Dc6f7B508C02d1b04ec88cC21bf;
    address internal constant TOKEN_5 = 0x526728DBc96689597F85ae4cd716d4f7fCcBAE9d;
    address internal constant TOKEN_6 = 0x7Ba6F01772924a82D9626c126347A28299E98c98;
    address internal constant TOKEN_7 = 0x940181a94A35A4569E4529A3CDfB74e38FD98631;
    address internal constant TOKEN_8 = 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf;

    function run() external {
        uint256 deployerKey = vm.envUint("DEPLOYER_PRIVATE_KEY");
        Executor executor = Executor(EXECUTOR_CONTRACT);

        vm.startBroadcast(deployerKey);

        _configureToken(executor, TOKEN_3);
        _configureToken(executor, TOKEN_4);
        _configureToken(executor, TOKEN_5);
        _configureToken(executor, TOKEN_6);
        _configureToken(executor, TOKEN_7);
        _configureToken(executor, TOKEN_8);

        vm.stopBroadcast();
    }

    function _configureToken(Executor executor, address token) internal {
        if (token == address(0)) return;

        executor.setTokenWhitelist(token, true);
        executor.approveToken(token, AERODROME_ROUTER, MAX_UINT);
        executor.approveToken(token, UNISWAP_V3_ROUTER, MAX_UINT);
    }
}
