// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

contract Eventful {
    event Poke(address indexed from, uint256 n);

    function poke(uint256 n) public {
        emit Poke(msg.sender, n);
    }
}

