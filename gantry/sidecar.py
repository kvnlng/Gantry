import os
import zlib
import numpy as np
from typing import Tuple, Optional
from threading import Lock

class SidecarManager:
    """
    Manages appending and reading from a binary sidecar file.
    Thread-safe for writes (append-only).
    Format: Just raw (compressed) bytes concatenated. Offsets stored in DB.
    """
    def __init__(self, filepath: str):
        self.filepath = filepath
        self._lock = Lock()
        self._ensure_file()

    def _ensure_file(self):
        if not os.path.exists(self.filepath):
            # Create empty file
            with open(self.filepath, 'wb') as f:
                pass

    def write_frame(self, data: bytes, compression: str = 'zlib') -> Tuple[int, int]:
        """
        Appends data to the sidecar.
        Returns: (offset, length)
        """
        if compression == 'zlib':
            blob = zlib.compress(data)
        elif compression == 'raw':
            blob = data
        else:
            raise ValueError(f"Unsupported compression: {compression}")
        
        length = len(blob)
        
        with self._lock:
            with open(self.filepath, 'ab') as f:
                offset = f.tell()
                f.write(blob)
                f.flush()
                # os.fsync(f.fileno()) # Slow, but safe. Optional.
                
        return offset, length

    def read_frame(self, offset: int, length: int, compression: str = 'zlib') -> bytes:
        """
        Reads a frame from the sidecar.
        """
        with open(self.filepath, 'rb') as f:
            f.seek(offset)
            blob = f.read(length)
            
        if len(blob) != length:
            raise IOError(f"Incomplete read from sidecar. Expected {length}, got {len(blob)}.")
            
        if compression == 'zlib':
            return zlib.decompress(blob)
        elif compression == 'raw':
            return blob
        else:
            raise ValueError(f"Unsupported compression: {compression}")

    size = property(lambda self: os.path.getsize(self.filepath))
