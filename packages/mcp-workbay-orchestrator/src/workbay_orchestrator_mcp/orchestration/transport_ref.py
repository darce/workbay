"""Namespaces used by current and legacy remote-agent transport refs."""

TRANSPORT_REF_PREFIXES = ("refs/workbay/remote-agent/", "refs/heads/")


def is_transport_ref(ref: str, nonce: str) -> bool:
    """Match an accepted namespace, nonempty lane key, and exact nonce suffix."""
    suffix = f"-{nonce}"
    return ref.endswith(suffix) and any(
        ref.startswith(prefix) and len(ref) > len(prefix) + len(suffix) for prefix in TRANSPORT_REF_PREFIXES
    )
