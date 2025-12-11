import json
import base64
from typing import Dict, Any, Optional
from .entities import Instance
from .crypto import CryptoEngine, KeyManager
from .logger import get_logger

class ReversibilityService:
    """
    Handles the embedding and recovery of encrypted original data in DICOM files.
    Uses Private Creator 'GANTRY_SECURE' at Group 0x0099.
    """
    
    # "0099,0010" -> Private Creator "GANTRY_SECURE"
    TAG_CREATOR = "0099,0010"
    CREATOR_VALUE = "GANTRY_SECURE"
    
    # "0099,1001" -> The payload (assuming 0010 reserves 1000-10FF, and we take 01)
    # Note: In pure DICOM, the slot relies on where the creator is. 
    # For Gantry, we enforce this specific slot for simplicity.
    TAG_PAYLOAD = "0099,1001"
    
    def __init__(self, key_manager: KeyManager):
        self.key_manager = key_manager
        self.engine = CryptoEngine(key_manager.get_key())
        self.logger = get_logger()

    def embed_original_data(self, instance: Instance, original_attributes: Dict[str, Any]):
        """
        Serializes, encrypts, and embeds the provided attributes into the instance.
        """
        if not original_attributes:
            return

        try:
            # 1. Serialize
            json_str = json.dumps(original_attributes)
            data_bytes = json_str.encode('utf-8')
            
            # 2. Encrypt
            encrypted_bytes = self.engine.encrypt(data_bytes)
            
            # 3. Embed into attributes dict
            instance.set_attr(self.TAG_CREATOR, self.CREATOR_VALUE)
            instance.set_attr(self.TAG_PAYLOAD, encrypted_bytes)
            
            self.logger.debug(f"Embedded {len(encrypted_bytes)} bytes of encrypted data into {instance.sop_instance_uid}.")
            
        except Exception as e:
            self.logger.error(f"Failed to embed original data: {e}")
            raise

    def recover_original_data(self, instance: Instance) -> Optional[Dict[str, Any]]:
        """
        Extracts and decrypts the original attributes from the instance.
        """
        try:
            # 1. Check for block
            creator = instance.attributes.get(self.TAG_CREATOR)
            if creator != self.CREATOR_VALUE:
                # self.logger.debug("No GANTRY_SECURE private block found.")
                return None
            
            # 2. Read Element
            encrypted_bytes = instance.attributes.get(self.TAG_PAYLOAD)
            if not encrypted_bytes:
                self.logger.warning("Encrypted data element not found despite creator presence.")
                return None
            
            # 3. Decrypt
            decrypted_bytes = self.engine.decrypt(encrypted_bytes)
            
            # 4. Deserialize
            json_str = decrypted_bytes.decode('utf-8')
            return json.loads(json_str)

        except Exception as e:
            self.logger.error(f"Failed to recover data from {instance.sop_instance_uid}: {e}")
            return None
