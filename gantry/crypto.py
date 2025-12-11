import os
from cryptography.fernet import Fernet
from typing import Optional

class KeyManager:
    """
    Manages the lifecycle of a symmetric encryption key.
    """
    def __init__(self, key_path: str = "gantry.key"):
        self.key_path = os.path.abspath(key_path)
        self.key: Optional[bytes] = None

    def load_or_generate_key(self) -> bytes:
        """
        Loads the key from disk if it exists, otherwise generates a new one and saves it.
        """
        if os.path.exists(self.key_path):
            with open(self.key_path, "rb") as f:
                self.key = f.read()
        else:
            self.key = Fernet.generate_key()
            with open(self.key_path, "wb") as f:
                f.write(self.key)
        return self.key

    def get_key(self) -> bytes:
        if not self.key:
            raise RuntimeError("Key not loaded. Call load_or_generate_key() first.")
        return self.key


class CryptoEngine:
    """
    Handles encryption and decryption of bytes using Fernet (AES-128-CBC w/ HMAC-SHA256).
    """
    def __init__(self, key: bytes):
        self.fernet = Fernet(key)

    def encrypt(self, data: bytes) -> bytes:
        return self.fernet.encrypt(data)

    def decrypt(self, token: bytes) -> bytes:
        return self.fernet.decrypt(token)
