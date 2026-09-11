import json
from typing import Dict, Any, Optional
from .entities import Instance
from .crypto import CryptoEngine, KeyManager
from .logger import get_logger, describe_exception


class ReversibilityService:
    """
    Handles the embedding and recovery of encrypted original data in DICOM files.

    Compliant with DICOM Part 15, E.1.2 "Re-identifier" logic via the
    Encrypted Attributes Sequence (0400,0500). Uses `CryptoEngine` for encryption.
    """

    # DICOM Standard Tags for Encrypted Attributes
    TAG_ENCRYPTED_ATTRS_SEQ = "0400,0500"
    TAG_ENCRYPTED_CONTENT = "0400,0510"
    TAG_TRANSFER_SYNTAX_UID = "0400,0520"

    # Transfer Syntax for the Encrypted Payload (Dataset)
    # We use Implicit VR Little Endian (Default) as a signal that the decrypted bytes
    # form a dataset-like structure (even though we wrap JSON, this is metadata).
    PAYLOAD_TRANSFER_SYNTAX = "1.2.840.10008.1.2"

    def __init__(self, key_manager: KeyManager):
        self.key_manager = key_manager
        self.engine = CryptoEngine(key_manager.get_key())
        self.logger = get_logger()

    def generate_identity_token(self, original_attributes: Dict[str, Any]) -> bytes:
        """
        Serializes and encrypts the attributes into a reusable token.

        Args:
            original_attributes (Dict[str, Any]): Dictionary of tag-value pairs to preserve.

        Returns:
            bytes: The encrypted JSON payload.
        """
        if not original_attributes:
            return b""

        json_str = json.dumps(original_attributes)
        data_bytes = json_str.encode('utf-8')
        return self.engine.encrypt(data_bytes)

    def embed_identity_token(self, instance: Instance, token: bytes):
        """
        Embeds a pre-calculated encrypted token into the instance.

        Wraps the token in an Encrypted Attributes Sequence item with the
        appropriate Transfer Syntax UID, and **replaces** whatever
        `(0400,0500)` held: after any call the sequence carries exactly
        one item, however many times the instance has been locked (#399).

        Args:
            instance (Instance): The target instance.
            token (bytes): The encrypted payload.
        """
        if not token:
            return

        try:
            # Create Sequence Item
            from .entities import DicomItem

            item = DicomItem()
            item.set_attr(self.TAG_ENCRYPTED_CONTENT, token)
            item.set_attr(self.TAG_TRANSFER_SYNTAX_UID, self.PAYLOAD_TRANSFER_SYNTAX)

            # `add_sequence()` plus a slice assignment rather than
            # `add_sequence_item()`, which appends: this sequence holds
            # exactly one item, and that item is the token this call was
            # handed. `recover_original_data` below reads items[0], and
            # until #399 the two disagreed -- so a second lock was
            # accepted, reported as success, persisted and exported while
            # recovery kept answering with the *first* capture, and every
            # stale token shipped in the file. Whatever the sequence held
            # is replaced, including an Encrypted Attributes Sequence the
            # source file carried: such an instance was not recoverable at
            # all before this, because the foreign blob sat at index 0.
            #
            # `mark_modified()` is NOT redundant and must not be tidied
            # away. `add_sequence()` marks the instance modified **only
            # when it creates** -- `self.mark_modified()` at
            # `entities.py` line 375 sits under `if sequence is None`,
            # #186's rule -- and this path reaches into `items` in place
            # rather than through `add_sequence_item()`, which marks on
            # every call. Without the line below the second and later
            # locks advance no revision, `has_unsaved_changes` stays
            # False, the next `save()` skips the instance, and the new
            # token never reaches the store: memory answers with capture
            # #2 and a reopened session answers with capture #1, with
            # nothing saying so. That is #173's shape one module over.
            sequence = instance.add_sequence(self.TAG_ENCRYPTED_ATTRS_SEQ)
            sequence.items[:] = [item]
            instance.mark_modified()

            # self.logger.debug(f"Embedded token into {instance.sop_instance_uid}.")

        except Exception as e:
            # `describe_exception`, not `{e}`: a bare raise has an
            # empty `str()`, and this line then said a step failed
            # without saying how (#487, #435's class).
            self.logger.error(
                f"Failed to embed token: {describe_exception(e)}")
            raise

    def embed_original_data(self, instance: Instance, original_attributes: Dict[str, Any]):
        """
        Serializes, encrypts, and embeds the provided attributes into the instance.

        This is a higher-level wrapper for `generate_identity_token` + `embed_identity_token`.

        Args:
            instance (Instance): The instance to modify.
            original_attributes (Dict[str, Any]): attributes to encrypt and store.
        """
        if not original_attributes:
            return

        try:
            token = self.generate_identity_token(original_attributes)
            self.embed_identity_token(instance, token)
            self.logger.debug(
                f"Embedded {
                    len(token)} bytes of encrypted data into {
                    instance.sop_instance_uid}.")

        except Exception as e:
            self.logger.error(
                f"Failed to embed original data: {describe_exception(e)}")
            raise

    def recover_original_data(self, instance: Instance) -> Optional[Dict[str, Any]]:
        """
        Extracts and decrypts the original attributes from the instance.

        Locates the Encrypted Attributes Sequence, decrypts the first item's
        Encrypted Content, and deserializes the JSON.

        **Item 0, and not the last item.** Since #399 every sequence this
        library writes holds exactly one item, so `items[0]` and
        `items[-1]` are the same expression on every file it will write
        again -- but they are not the same on a file written by 0.9.4 or
        earlier, which carries one item per lock and whose *first* one is
        what that release's recovery answered with.
        `docs/api/stability.md` promises those files stay recoverable, so
        this index is a compatibility commitment rather than a detail;
        `tests/test_relock_identity_token.py` holds it.

        Args:
            instance (Instance): The anonymized instance.

        Returns:
            Optional[Dict[str, Any]]: The recovered dictionary of original attributes, or None if failed/missing.
        """
        try:
            # 1. Check for Sequence
            if self.TAG_ENCRYPTED_ATTRS_SEQ not in instance.sequences:
                return None

            seq = instance.sequences[self.TAG_ENCRYPTED_ATTRS_SEQ]
            if not seq.items:
                return None

            # 2. Read First Item
            item = seq.items[0]
            encrypted_bytes = item.attributes.get(self.TAG_ENCRYPTED_CONTENT)

            if not encrypted_bytes:
                self.logger.warning("EncryptedContent (0400,0510) not found in sequence item.")
                return None

            # 3. Decrypt
            decrypted_bytes = self.engine.decrypt(encrypted_bytes)

            # 4. Deserialize
            json_str = decrypted_bytes.decode('utf-8')
            return json.loads(json_str)

        except Exception as e:
            # `describe_exception`, not `{e}`. The one failure that
            # means "a token is here and this key cannot open it" is
            # Fernet's `InvalidToken`, whose `str()` is empty, so this
            # line read `Failed to recover data from <uid>: ` and a
            # wrong key could not be told from any other failure
            # (#487). Now `...: InvalidToken`.
            self.logger.error(
                f"Failed to recover data from {instance.sop_instance_uid}: "
                f"{describe_exception(e)}")
            return None
