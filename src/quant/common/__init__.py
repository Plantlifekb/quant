from .version import PLATFORM_VERSION, SCHEMA_VERSION

def assert_platform_version(expected: str):
    if PLATFORM_VERSION != expected:
        raise RuntimeError(
            f"Platform version mismatch: expected {expected}, got {PLATFORM_VERSION}"
        )
