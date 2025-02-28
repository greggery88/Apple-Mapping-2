def cleanup_str(string: str) -> str:
    return (
        string.lower()
        .strip()
        .replace("?", "")
        .replace("()", "")
        .replace("  ", " ")
        .replace("(pears)", "(pear)")
    )


def remove_ssl_error() -> None:
    import warnings
    from urllib3.exceptions import NotOpenSSLWarning
    warnings.filterwarnings("ignore", category=NotOpenSSLWarning)

