"""This module describes how to decrypt secrets stored in our database (such as passwords for scraper runs).

Uses pyaes; see https://github.com/ricmoo/pyaes
"""

import pyaes

from datafeeds import config


def _aes_key():
    # key must be exactly 32 bytes
    return (config.AES_SECRET_KEY or "snapmeter" * 4)[:32].encode("utf-8")


def aes_encrypt(text):
    """Encrypt the given string and return bytes"""
    aes = pyaes.AESModeOfOperationCTR(_aes_key())
    return aes.encrypt(text)  # bytes


def aes_decrypt(encrypted_bytes):
    """Decrypt the given bytes and return a string"""
    aes = pyaes.AESModeOfOperationCTR(_aes_key())
    return aes.decrypt(encrypted_bytes).decode("utf-8")
