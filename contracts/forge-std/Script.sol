// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

interface VmScript {
    function envUint(string calldata name) external view returns (uint256);
    function envAddress(string calldata name) external view returns (address);
    function envOr(string calldata name, address defaultValue) external view returns (address);
    function startBroadcast(uint256 privateKey) external;
    function stopBroadcast() external;
}

contract Script {
    VmScript internal constant vm = VmScript(address(uint160(uint256(keccak256("hevm cheat code")))));
}
