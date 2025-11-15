// TinyToken.sol
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract TinyToken {
    mapping(address => uint256) public balanceOf;
    event Transfer(address indexed from, address indexed to, uint256 value);

    constructor(uint256 initial) {
        balanceOf[msg.sender] = initial;
    }

    function transfer(address to, uint256 value) public returns (bool) {
        uint256 fromBal = balanceOf[msg.sender];
        require(fromBal >= value);
        balanceOf[msg.sender] = fromBal - value;
        balanceOf[to] += value;
        emit Transfer(msg.sender, to, value);
        return true;
    }
}

